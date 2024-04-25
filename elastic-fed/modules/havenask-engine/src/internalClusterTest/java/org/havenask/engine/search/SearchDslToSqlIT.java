/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.search;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.havenask.ArpcThreadLeakFilterIT;
import org.havenask.HttpThreadLeakFilterIT;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskInternalClusterTestCase;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.test.HavenaskIntegTestCase;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

@ThreadLeakFilters(filters = { HttpThreadLeakFilterIT.class, ArpcThreadLeakFilterIT.class })
@HavenaskIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0)
@HavenaskIntegTestCase.SuiteScopeTestCase
public class SearchDslToSqlIT extends HavenaskInternalClusterTestCase {

    private static final String INDEX_NAME = "test_dsl2sql";

    @Before
    public void setupSuiteScopeCluster() {
        logger.info("Creating index [{}]", INDEX_NAME);
        prepareCreate(INDEX_NAME).setSettings(
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                .build()
        )
            .addMapping(
                "_doc",
                "field1",
                "type=text",
                "field2",
                "type=integer",
                "field3",
                "type=keyword",
                "field4",
                "type=long",
                "field5",
                "type=double",
                "field6",
                "type=date",
                "field7",
                "type=boolean"
            )
            .execute()
            .actionGet();
        ensureGreen();
        for (int i = 0; i < 100; i++) {
            // index data with i
            client().prepareIndex(INDEX_NAME, "_doc", String.valueOf(i))
                .setSource(
                    "field1",
                    "hello world" + i,
                    "field2",
                    i,
                    "field3",
                    "value" + i,
                    "field4",
                    i,
                    "field5",
                    i,
                    "field6",
                    i,
                    "field7",
                    i % 2 == 0
                )
                .execute()
                .actionGet();
        }
    }

    // test term dsl query
    public void testTermQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("field3", "value1"));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // check value
            assertEquals("value1", searchResponse.getHits().getHits()[0].getSourceAsMap().get("field3"));
            assertEquals("1", searchResponse.getHits().getHits()[0].getId());
        }, 5, TimeUnit.SECONDS);
    }

    // test range dsl query
    public void testRangeQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.rangeQuery("field2").gte(10).lt(20));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(10, searchResponse.getHits().getTotalHits().value);
            // check value
            for (int i = 10; i < 20; i++) {
                assertEquals(i, searchResponse.getHits().getHits()[i - 10].getSourceAsMap().get("field2"));
                assertEquals(String.valueOf(i), searchResponse.getHits().getHits()[i - 10].getId());
            }
        }, 5, TimeUnit.SECONDS);
    }

    // test bool dsl query
    public void testBoolQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field2", 1)).filter(QueryBuilders.termQuery("field3", "value1"))
        );

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // check value
            assertEquals(1, searchResponse.getHits().getHits()[0].getSourceAsMap().get("field2"));
            assertEquals("value1", searchResponse.getHits().getHits()[0].getSourceAsMap().get("field3"));
            assertEquals("1", searchResponse.getHits().getHits()[0].getId());
        }, 5, TimeUnit.SECONDS);
    }

    // test bool dsl query with should
    public void testBoolQueryWithShould() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field2", 1)).should(QueryBuilders.termQuery("field3", "value1"))
        );

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // check value
            assertEquals(1, searchResponse.getHits().getHits()[0].getSourceAsMap().get("field2"));
            assertEquals("value1", searchResponse.getHits().getHits()[0].getSourceAsMap().get("field3"));
            assertEquals("1", searchResponse.getHits().getHits()[0].getId());
        }, 5, TimeUnit.SECONDS);
    }

    // test bool dsl query with must
    public void testBoolQueryWithMust() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field2", 1)).must(QueryBuilders.termQuery("field3", "value1"))
        );

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // check value
            assertEquals(1, searchResponse.getHits().getHits()[0].getSourceAsMap().get("field2"));
            assertEquals("value1", searchResponse.getHits().getHits()[0].getSourceAsMap().get("field3"));
            assertEquals("1", searchResponse.getHits().getHits()[0].getId());
        }, 5, TimeUnit.SECONDS);
    }

    // test bool dsl query with must_not
    public void testBoolQueryWithMustNot() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field2", 1)).mustNot(QueryBuilders.termQuery("field3", "value1"))
        );

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(0, searchResponse.getHits().getTotalHits().value);
        }, 5, TimeUnit.SECONDS);
    }

    // test bool dsl query with filter
    public void testBoolQueryWithFilter() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field2", 1)).filter(QueryBuilders.termQuery("field3", "value1"))
        );

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // check value
            assertEquals(1, searchResponse.getHits().getHits()[0].getSourceAsMap().get("field2"));
            assertEquals("value1", searchResponse.getHits().getHits()[0].getSourceAsMap().get("field3"));
            assertEquals("1", searchResponse.getHits().getHits()[0].getId());
        }, 5, TimeUnit.SECONDS);
    }

    // test terms
    public void testTermsQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termsQuery("field3", "value1", "value2"));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(2, searchResponse.getHits().getTotalHits().value);
            // check value
            for (int i = 1; i < 3; i++) {
                assertEquals("value" + i, searchResponse.getHits().getHits()[i - 1].getSourceAsMap().get("field3"));
                assertEquals(String.valueOf(i), searchResponse.getHits().getHits()[i - 1].getId());
            }
        }, 5, TimeUnit.SECONDS);
    }

    // test match phrase query
    public void testMatchPhraseQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchPhraseQuery("field1", "hello world1"));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // check value
            assertEquals("hello world1", searchResponse.getHits().getHits()[0].getSourceAsMap().get("field1"));
            assertEquals("1", searchResponse.getHits().getHits()[0].getId());
        }, 5, TimeUnit.SECONDS);
    }

    // test query string query
    public void testQueryStringQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.queryStringQuery("field1:world1"));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // check value
            assertEquals("hello world1", searchResponse.getHits().getHits()[0].getSourceAsMap().get("field1"));
            assertEquals("1", searchResponse.getHits().getHits()[0].getId());
        }, 5, TimeUnit.SECONDS);
    }

    // test match
    public void testMatchQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("field1", "world1"));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // check value
            assertEquals("hello world1", searchResponse.getHits().getHits()[0].getSourceAsMap().get("field1"));
            assertEquals("1", searchResponse.getHits().getHits()[0].getId());
        }, 5, TimeUnit.SECONDS);
    }

    // test match all
    public void testMatchAllQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSize(100).setSource(builder).get();
            assertEquals(100, searchResponse.getHits().getTotalHits().value);
        }, 5, TimeUnit.SECONDS);
    }

    // test exists
    public void testExistsQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.existsQuery("field1"));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(100, searchResponse.getHits().getTotalHits().value);
        }, 5, TimeUnit.SECONDS);
    }

    // test not exists
    public void testNotExistsQuery() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("field1")));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSource(builder).get();
            assertEquals(0, searchResponse.getHits().getTotalHits().value);
        }, 5, TimeUnit.SECONDS);
    }
}
