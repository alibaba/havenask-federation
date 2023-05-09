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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.test.search.aggregations.bucket;

import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.xcontent.XContentType;
import org.havenask.search.aggregations.Aggregation;
import org.havenask.search.aggregations.bucket.terms.SignificantTerms;
import org.havenask.search.aggregations.bucket.terms.StringTerms;
import org.havenask.search.aggregations.bucket.terms.Terms;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskTestCase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.havenask.test.HavenaskIntegTestCase.client;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.havenask.search.aggregations.AggregationBuilders.significantTerms;
import static org.havenask.search.aggregations.AggregationBuilders.terms;

public class SharedSignificantTermsTestMethods {
    public static final String INDEX_NAME = "testidx";
    public static final String DOC_TYPE = "_doc";
    public static final String TEXT_FIELD = "text";
    public static final String CLASS_FIELD = "class";

    public static void aggregateAndCheckFromSeveralShards(HavenaskIntegTestCase testCase)
        throws ExecutionException, InterruptedException {
        String type = HavenaskTestCase.randomBoolean() ? "text" : "keyword";
        String settings = "{\"index.number_of_shards\": 7, \"index.number_of_routing_shards\": 7, \"index.number_of_replicas\": 0}";
        index01Docs(type, settings, testCase);
        testCase.ensureGreen();
        testCase.logClusterState();
        checkSignificantTermsAggregationCorrect(testCase);
    }

    private static void checkSignificantTermsAggregationCorrect(HavenaskIntegTestCase testCase) {
        SearchResponse response = client().prepareSearch(INDEX_NAME).setTypes(DOC_TYPE).addAggregation(
                terms("class").field(CLASS_FIELD).subAggregation(significantTerms("sig_terms").field(TEXT_FIELD)))
                .execute().actionGet();
        assertSearchResponse(response);
        StringTerms classes = response.getAggregations().get("class");
        Assert.assertThat(classes.getBuckets().size(), equalTo(2));
        for (Terms.Bucket classBucket : classes.getBuckets()) {
            Map<String, Aggregation> aggs = classBucket.getAggregations().asMap();
            Assert.assertTrue(aggs.containsKey("sig_terms"));
            SignificantTerms agg = (SignificantTerms) aggs.get("sig_terms");
            Assert.assertThat(agg.getBuckets().size(), equalTo(1));
            SignificantTerms.Bucket sigBucket = agg.iterator().next();
            String term = sigBucket.getKeyAsString();
            String classTerm = classBucket.getKeyAsString();
            Assert.assertTrue(term.equals(classTerm));
        }
    }

    public static void index01Docs(String type, String settings, HavenaskIntegTestCase testCase)
        throws ExecutionException, InterruptedException {
        String textMappings = "type=" + type;
        if (type.equals("text")) {
            textMappings += ",fielddata=true";
        }
        assertAcked(testCase.prepareCreate(INDEX_NAME).setSettings(settings, XContentType.JSON)
                .addMapping("_doc", "text", textMappings, CLASS_FIELD, "type=keyword"));
        String[] gb = {"0", "1"};
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "1")
                .setSource(TEXT_FIELD, "1", CLASS_FIELD, "1"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "2")
                .setSource(TEXT_FIELD, "1", CLASS_FIELD, "1"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "3")
                .setSource(TEXT_FIELD, "0", CLASS_FIELD, "0"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "4")
                .setSource(TEXT_FIELD, "0", CLASS_FIELD, "0"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "5")
                .setSource(TEXT_FIELD, gb, CLASS_FIELD, "1"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "6")
                .setSource(TEXT_FIELD, gb, CLASS_FIELD, "0"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE, "7")
                .setSource(TEXT_FIELD, "0", CLASS_FIELD, "0"));
        testCase.indexRandom(true, false, indexRequestBuilderList);
    }
}
