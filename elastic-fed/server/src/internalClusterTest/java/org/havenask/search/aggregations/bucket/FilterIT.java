/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.search.aggregations.bucket;

import org.havenask.HavenaskException;
import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.query.BoolQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.bucket.filter.Filter;
import org.havenask.search.aggregations.bucket.histogram.Histogram;
import org.havenask.search.aggregations.metrics.Avg;
import org.havenask.test.HavenaskIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;

import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.index.query.QueryBuilders.matchAllQuery;
import static org.havenask.index.query.QueryBuilders.termQuery;
import static org.havenask.search.aggregations.AggregationBuilders.avg;
import static org.havenask.search.aggregations.AggregationBuilders.filter;
import static org.havenask.search.aggregations.AggregationBuilders.histogram;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@HavenaskIntegTestCase.SuiteScopeTestCase
public class FilterIT extends HavenaskIntegTestCase {

    static int numDocs, numTag1Docs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        numDocs = randomIntBetween(5, 20);
        numTag1Docs = randomIntBetween(1, numDocs - 1);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numTag1Docs; i++) {
            builders.add(client().prepareIndex("idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .field("tag", "tag1")
                    .endObject()));
        }
        for (int i = numTag1Docs; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder()
                    .startObject()
                    .field("value", i)
                    .field("tag", "tag2")
                    .field("name", "name" + i)
                    .endObject();
            builders.add(client().prepareIndex("idx", "type", ""+i).setSource(source));
            if (randomBoolean()) {
                // randomly index the document twice so that we have deleted docs that match the filter
                builders.add(client().prepareIndex("idx", "type", ""+i).setSource(source));
            }
        }
        prepareCreate("empty_bucket_idx").addMapping("type", "value", "type=integer").get();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testSimple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(filter("tag1", termQuery("tag", "tag1")))
                .get();

        assertSearchResponse(response);


        Filter filter = response.getAggregations().get("tag1");
        assertThat(filter, notNullValue());
        assertThat(filter.getName(), equalTo("tag1"));
        assertThat(filter.getDocCount(), equalTo((long) numTag1Docs));
    }

    // See NullPointer issue when filters are empty:
    // https://github.com/elastic/elasticsearch/issues/8438
    public void testEmptyFilterDeclarations() throws Exception {
        QueryBuilder emptyFilter = new BoolQueryBuilder();
        SearchResponse response = client().prepareSearch("idx").addAggregation(filter("tag1", emptyFilter)).get();

        assertSearchResponse(response);

        Filter filter = response.getAggregations().get("tag1");
        assertThat(filter, notNullValue());
        assertThat(filter.getDocCount(), equalTo((long) numDocs));
    }

    public void testWithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(filter("tag1", termQuery("tag", "tag1"))
                        .subAggregation(avg("avg_value").field("value")))
                .get();

        assertSearchResponse(response);


        Filter filter = response.getAggregations().get("tag1");
        assertThat(filter, notNullValue());
        assertThat(filter.getName(), equalTo("tag1"));
        assertThat(filter.getDocCount(), equalTo((long) numTag1Docs));
        assertThat((long) ((InternalAggregation)filter).getProperty("_count"), equalTo((long) numTag1Docs));

        long sum = 0;
        for (int i = 0; i < numTag1Docs; ++i) {
            sum += i + 1;
        }
        assertThat(filter.getAggregations().asList().isEmpty(), is(false));
        Avg avgValue = filter.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) sum / numTag1Docs));
        assertThat((double) ((InternalAggregation)filter).getProperty("avg_value.value"), equalTo((double) sum / numTag1Docs));
    }

    public void testAsSubAggregation() {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field("value").interval(2L).subAggregation(
                                filter("filter", matchAllQuery()))).get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getBuckets().size(), greaterThanOrEqualTo(1));

        for (Histogram.Bucket bucket : histo.getBuckets()) {
            Filter filter = bucket.getAggregations().get("filter");
            assertThat(filter, notNullValue());
            assertEquals(bucket.getDocCount(), filter.getDocCount());
        }
    }

    public void testWithContextBasedSubAggregation() throws Exception {
        try {
            client().prepareSearch("idx")
                    .addAggregation(filter("tag1", termQuery("tag", "tag1"))
                            .subAggregation(avg("avg_value")))
                    .get();

            fail("expected execution to fail - an attempt to have a context based numeric sub-aggregation, but there is not value source" +
                    "context which the sub-aggregation can inherit");

        } catch (HavenaskException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0)
                        .subAggregation(filter("filter", matchAllQuery())))
                .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Filter filter = bucket.getAggregations().get("filter");
        assertThat(filter, Matchers.notNullValue());
        assertThat(filter.getName(), equalTo("filter"));
        assertThat(filter.getDocCount(), is(0L));
    }
}
