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

package org.havenask.search.aggregations;

import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.unit.TimeValue;
import org.havenask.search.aggregations.bucket.terms.Terms;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.havenask.search.aggregations.AggregationBuilders.terms;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertSearchResponse;


@HavenaskIntegTestCase.SuiteScopeTestCase
public class AggregationsIntegrationIT extends HavenaskIntegTestCase {

    static int numDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", "f", "type=keyword").get());
        numDocs = randomIntBetween(1, 20);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(client().prepareIndex("index", "type").setSource("f", Integer.toString(i / 3)));
        }
        indexRandom(true, docs);
    }

    public void testScroll() {
        final int size = randomIntBetween(1, 4);
        SearchResponse response = client().prepareSearch("index")
                .setSize(size).setScroll(TimeValue.timeValueMinutes(1))
                .addAggregation(terms("f").field("f")).get();
        assertSearchResponse(response);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Terms terms = aggregations.get("f");
        assertEquals(Math.min(numDocs, 3L), terms.getBucketByKey("0").getDocCount());

        int total = response.getHits().getHits().length;
        while (response.getHits().getHits().length > 0) {
            response = client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1))
                    .get();
            assertSearchResponse(response);
            assertNull(response.getAggregations());
            total += response.getHits().getHits().length;
        }
        clearScroll(response.getScrollId());
        assertEquals(numDocs, total);
    }

}
