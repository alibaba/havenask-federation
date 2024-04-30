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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.ArpcThreadLeakFilterIT;
import org.havenask.HavenaskException;
import org.havenask.HttpThreadLeakFilterIT;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.search.ClearScrollRequest;
import org.havenask.action.search.ClearScrollResponse;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.Strings;
import org.havenask.common.collect.List;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.HavenaskInternalClusterTestCase;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.index.IndexNotFoundException;
import org.havenask.index.reindex.BulkByScrollResponse;
import org.havenask.index.reindex.ReindexAction;
import org.havenask.index.reindex.ReindexRequestBuilder;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.slice.SliceBuilder;
import org.havenask.test.HavenaskIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.havenask.test.HavenaskIntegTestCase.Scope.SUITE;

@ThreadLeakFilters(filters = { HttpThreadLeakFilterIT.class, ArpcThreadLeakFilterIT.class })
@HavenaskIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0, scope = SUITE)
public class SearchIT extends HavenaskInternalClusterTestCase {
    private static final Logger logger = LogManager.getLogger(SearchIT.class);

    public void testSearch() throws Exception {
        String index = "keyword_test";
        prepareKeywordIndex(index);

        // PUT docs
        int docNum = randomIntBetween(100, 200);
        for (int i = 0; i < docNum; i++) {
            client().index(new IndexRequest(index).id(String.valueOf(i)).source(Map.of("content", "keyword " + i), XContentType.JSON));
        }

        // get data with _search API
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(docNum);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(index).setSource(searchSourceBuilder).get();
            assertEquals(docNum, searchResponse.getHits().getHits().length);
        }, 5, TimeUnit.SECONDS);

        logger.info("testSearch success");

        try {
            AcknowledgedResponse deleteIndexResponse = client().admin().indices().prepareDelete(index).get();
            assertTrue(deleteIndexResponse.isAcknowledged());
        } catch (IndexNotFoundException e) {
            fail("Index was not found to delete: " + index);
        }
        boolean exists = client().admin().indices().prepareExists(index).get().isExists();
        assertFalse("Index should have been deleted but still exists", exists);
    }

    public void testKnnSearch() throws Exception {
        String index = "knn_search";
        prepareIndex(index);

        // put doc
        int docNum = randomIntBetween(100, 200);
        for (int i = 0; i < docNum; i++) {
            client().index(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(Map.of("vector", new float[] { 0.1f + i, 0.1f + i }), XContentType.JSON)
            );
        }

        // search doc
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 1.5f, 2.5f }, docNum, docNum, null)));

        searchSourceBuilder.size(docNum);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(index).setSource(searchSourceBuilder).get();
            assertEquals(docNum, searchResponse.getHits().getHits().length);
        }, 5, TimeUnit.SECONDS);

        logger.info("testKnnSearch success");

        try {
            AcknowledgedResponse deleteIndexResponse = client().admin().indices().prepareDelete(index).get();
            assertTrue(deleteIndexResponse.isAcknowledged());
        } catch (IndexNotFoundException e) {
            fail("Index was not found to delete: " + index);
        }
        boolean exists = client().admin().indices().prepareExists(index).get().isExists();
        assertFalse("Index should have been deleted but still exists", exists);
    }

    private void prepareKeywordIndex(String index) throws Exception {
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"content\": {\n"
            + "      \"type\": \"keyword\""
            + "    }\n"
            + "  }\n"
            + "}";

        ClusterHealthResponse chResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shardsNum)
                        .put("index.number_of_replicas", replicasNum)
                        .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                        .build()
                )
                .addMapping("_doc", mapping, XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 30, TimeUnit.SECONDS);
    }

    private void prepareIndex(String index) throws Exception {
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"vector\": {\n"
            + "      \"type\": \"vector\",\n"
            + "      \"dims\": 2\n"
            + "    }\n"
            + "  }\n"
            + "}";

        ClusterHealthResponse chResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shardsNum)
                        .put("index.number_of_replicas", replicasNum)
                        .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                        .build()
                )
                .addMapping("_doc", mapping, XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        ensureGreen(index);
    }

    public void testSourceFilter() throws Exception {
        String index = "test2";
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject("name");
                {
                    mappingBuilder.field("type", "keyword");
                }
                mappingBuilder.endObject();
                mappingBuilder.startObject("seq");
                {
                    mappingBuilder.field("type", "integer");
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                        .build()
                )
                .addMapping("_doc", Strings.toString(mappingBuilder), XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        ensureGreen(index);

        // put doc
        client().index(new IndexRequest(index).id(String.valueOf("1")).source(Map.of("name", "alice", "seq", 1), XContentType.JSON));
        client().index(new IndexRequest(index).id(String.valueOf("2")).source(Map.of("name", "bob", "seq", 2), XContentType.JSON));
        client().index(new IndexRequest(index).id(String.valueOf("3")).source(Map.of("name", "eve", "seq", 3), XContentType.JSON));

        String[] include1 = new String[] { "name" };
        String[] exclude1 = new String[] { "seq" };

        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        searchSourceBuilder1.fetchSource(include1, exclude1);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(index).setSource(searchSourceBuilder1).get();
            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                assertTrue(searchResponse.getHits().getHits()[i].getSourceAsString().contains("name"));
                assertFalse(searchResponse.getHits().getHits()[i].getSourceAsString().contains("seq"));
            }
        }, 5, TimeUnit.SECONDS);

        String[] include2 = null;
        String[] exclude2 = new String[] { "name" };

        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        searchSourceBuilder2.fetchSource(include2, exclude2);
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(index).setSource(searchSourceBuilder2).get();
            for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
                assertFalse(searchResponse.getHits().getHits()[i].getSourceAsString().contains("name"));
                assertTrue(searchResponse.getHits().getHits()[i].getSourceAsString().contains("seq"));
            }
        }, 5, TimeUnit.SECONDS);

        logger.info("testSourceFilter success");

        try {
            AcknowledgedResponse deleteIndexResponse = client().admin().indices().prepareDelete(index).get();
            assertTrue(deleteIndexResponse.isAcknowledged());
        } catch (IndexNotFoundException e) {
            fail("Index was not found to delete: " + index);
        }
        boolean exists = client().admin().indices().prepareExists(index).get().isExists();
        assertFalse("Index should have been deleted but still exists", exists);
    }

    public void testReindex() throws Exception {
        String sourceIndex = "reindex_test_source";
        String destIndex = "reindex_test_dest";

        prepareReindexAndScrollIndex(sourceIndex);
        prepareReindexAndScrollIndex(destIndex);

        // put docs into source index
        int docNum = randomIntBetween(100, 200);
        for (int i = 0; i < docNum; i++) {
            client().index(
                new IndexRequest(sourceIndex).id(String.valueOf(i))
                    .source(Map.of("name", randomAlphaOfLength(6), "seq", i), XContentType.JSON)
            );
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(docNum);
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(sourceIndex).setSource(searchSourceBuilder).get();
            assertEquals(docNum, searchResponse.getHits().getHits().length);
        }, 5, TimeUnit.SECONDS);

        // execute reindex
        ReindexRequestBuilder reindexRequestBuilder = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source(sourceIndex)
            .destination(destIndex);
        BulkByScrollResponse response = reindexRequestBuilder.get();
        assertFalse(response.isTimedOut());
        assertEquals(docNum, response.getCreated());

        // check dest index
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(destIndex).setSource(searchSourceBuilder).get();
            assertEquals(docNum, searchResponse.getHits().getHits().length);
        }, 5, TimeUnit.SECONDS);

        try {
            AcknowledgedResponse deleteIndexResponse = client().admin().indices().prepareDelete(sourceIndex, destIndex).get();
            assertTrue(deleteIndexResponse.isAcknowledged());
        } catch (IndexNotFoundException e) {
            fail("Index was not found to delete: " + sourceIndex + ", " + destIndex);
        }
        boolean exists = client().admin().indices().prepareExists(sourceIndex, destIndex).get().isExists();
        assertFalse("Index should have been deleted but still exists", exists);
    }

    public void testScrollHavenaskIndexAndESIndex() throws Exception {
        String havenaskIndex = "scroll_havenask_index_test";
        String esIndex = "scroll_es_index_test";
        // prepare index
        prepareReindexAndScrollIndex(havenaskIndex);
        prepareReindexAndScrollIndex(esIndex, EngineSettings.ENGINE_LUCENE);

        // put docs
        int docNum = randomIntBetween(100, 200);
        for (int i = 0; i < docNum; i++) {
            client().index(
                new IndexRequest(havenaskIndex).id(String.valueOf(i))
                    .source(Map.of("name", "havenask" + randomAlphaOfLength(6), "seq", i), XContentType.JSON)
            );
            client().index(
                new IndexRequest(esIndex).id(String.valueOf(i))
                    .source(Map.of("name", "es" + randomAlphaOfLength(6), "seq", i), XContentType.JSON)
            );
        }

        // check doc
        SearchSourceBuilder checkSearchSourceBuilder = new SearchSourceBuilder();
        checkSearchSourceBuilder.size(docNum);

        assertBusy(() -> {
            SearchResponse havenaskSearchResponse = client().prepareSearch(havenaskIndex).setSource(checkSearchSourceBuilder).get();
            assertEquals(docNum, havenaskSearchResponse.getHits().getHits().length);
            SearchResponse esSearchResponse = client().prepareSearch(havenaskIndex).setSource(checkSearchSourceBuilder).get();
            assertEquals(docNum, esSearchResponse.getHits().getHits().length);
        }, 5, TimeUnit.SECONDS);

        // check scroll search
        java.util.List<String> indices = List.of(havenaskIndex, esIndex);
        java.util.List<SearchResponse> searchResponses = new ArrayList<>(2);
        for (int i = 0; i < indices.size(); i++) {
            int resCount = 0;

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(10);
            SearchResponse searchResponse = client().prepareSearch(indices.get(i))
                .setSource(searchSourceBuilder)
                .setScroll(TimeValue.timeValueMinutes(1))
                .setSize(10)
                .get();
            assertEquals(10, searchResponse.getHits().getHits().length);
            resCount += searchResponse.getHits().getHits().length;

            while (true) {
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
                resCount += searchResponse.getHits().getHits().length;
                if (searchResponse.getHits().getHits().length == 0) {
                    break;
                }
            }
            assertEquals(docNum, resCount);
            searchResponses.add(searchResponse);
        }

        // clean scoll id
        for (int i = 0; i < indices.size(); i++) {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(searchResponses.get(i).getScrollId());
            ClearScrollResponse clearScrollResponse = client().clearScroll(clearScrollRequest).actionGet();
            assertTrue(clearScrollResponse.isSucceeded());
        }

        // assert already clean scroll id
        java.util.List<String> exceptionMessages = List.of("no havenask scroll context found, sessionId:", "all shards failed");
        for (int i = 0; i < indices.size(); i++) {
            String clearedScrollId = searchResponses.get(i).getScrollId();
            Exception Exception = expectThrows(
                HavenaskException.class,
                () -> { client().prepareSearchScroll(clearedScrollId).setScroll(TimeValue.timeValueMinutes(1)).get(); }
            );
            assertTrue(Exception.getMessage().contains(exceptionMessages.get(i)));
        }

        // delete index
        for (int i = 0; i < indices.size(); i++) {
            try {
                AcknowledgedResponse deleteIndexResponse = client().admin().indices().prepareDelete(indices.get(i)).get();
                assertTrue(deleteIndexResponse.isAcknowledged());
            } catch (IndexNotFoundException e) {
                fail("Index was not found to delete: " + havenaskIndex);
            }
            boolean exists = client().admin().indices().prepareExists(havenaskIndex).get().isExists();
            assertFalse("Index should have been deleted but still exists", exists);
        }
    }

    public void testScrollHavenaskIndexWithSlice() throws Exception {
        String index = "scroll_havenask_index_test";
        // prepare index
        prepareReindexAndScrollIndex(index);

        // put docs
        int docNum = randomIntBetween(100, 200);
        for (int i = 0; i < docNum; i++) {
            client().index(
                new IndexRequest(index).id(String.valueOf(i)).source(Map.of("name", randomAlphaOfLength(6), "seq", i), XContentType.JSON)
            );
        }

        // check doc
        SearchSourceBuilder checkSearchSourceBuilder = new SearchSourceBuilder();
        checkSearchSourceBuilder.size(docNum);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(index).setSource(checkSearchSourceBuilder).get();
            assertEquals(docNum, searchResponse.getHits().getHits().length);
        }, 5, TimeUnit.SECONDS);

        // scroll search
        int resCount = 0;

        SearchSourceBuilder firstSliceSearchSourceBuilder = new SearchSourceBuilder();
        firstSliceSearchSourceBuilder.size(10);
        firstSliceSearchSourceBuilder.slice(new SliceBuilder(0, 2));
        SearchResponse firstSliceSearchResponse = client().prepareSearch(index)
            .setSource(firstSliceSearchSourceBuilder)
            .setScroll(TimeValue.timeValueMinutes(1))
            .setSize(10)
            .get();
        assertEquals(10, firstSliceSearchResponse.getHits().getHits().length);
        resCount += firstSliceSearchResponse.getHits().getHits().length;

        while (true) {
            firstSliceSearchResponse = client().prepareSearchScroll(firstSliceSearchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(1))
                .get();
            resCount += firstSliceSearchResponse.getHits().getHits().length;
            if (firstSliceSearchResponse.getHits().getHits().length == 0) {
                break;
            }
        }

        SearchSourceBuilder secondSliceSearchSourceBuilder = new SearchSourceBuilder();
        secondSliceSearchSourceBuilder.size(10);
        secondSliceSearchSourceBuilder.slice(new SliceBuilder(1, 2));
        SearchResponse secondSliceSearchResponse = client().prepareSearch(index)
            .setSource(secondSliceSearchSourceBuilder)
            .setScroll(TimeValue.timeValueMinutes(1))
            .setSize(10)
            .get();
        assertEquals(10, secondSliceSearchResponse.getHits().getHits().length);
        resCount += secondSliceSearchResponse.getHits().getHits().length;

        while (true) {
            secondSliceSearchResponse = client().prepareSearchScroll(secondSliceSearchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(1))
                .get();
            resCount += secondSliceSearchResponse.getHits().getHits().length;
            if (secondSliceSearchResponse.getHits().getHits().length == 0) {
                break;
            }
        }

        assertEquals(docNum, resCount);

        // clean scoll id
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(firstSliceSearchResponse.getScrollId());
        clearScrollRequest.addScrollId(secondSliceSearchResponse.getScrollId());
        ClearScrollResponse clearScrollResponse = client().clearScroll(clearScrollRequest).actionGet();
        assertTrue(clearScrollResponse.isSucceeded());

        // assert already clean scroll id
        java.util.List<String> clearedScrollIds = List.of(firstSliceSearchResponse.getScrollId(), secondSliceSearchResponse.getScrollId());
        for (int i = 0; i < 2; i++) {
            // assert already clean scroll id
            String clearedScrollId = clearedScrollIds.get(i);
            HavenaskException havenaskException = expectThrows(
                HavenaskException.class,
                () -> { client().prepareSearchScroll(clearedScrollId).setScroll(TimeValue.timeValueMinutes(1)).get(); }
            );
            assertTrue(havenaskException.getMessage().contains("no havenask scroll context found, sessionId:"));
        }

        try {
            AcknowledgedResponse deleteIndexResponse = client().admin().indices().prepareDelete(index).get();
            assertTrue(deleteIndexResponse.isAcknowledged());
        } catch (IndexNotFoundException e) {
            fail("Index was not found to delete: " + index);
        }
        boolean exists = client().admin().indices().prepareExists(index).get().isExists();
        assertFalse("Index should have been deleted but still exists", exists);
    }

    public void prepareReindexAndScrollIndex(String index, String engine) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        Settings settings = Settings.builder()
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 0)
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), engine)
            .build();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject("name");
                {
                    mappingBuilder.field("type", "keyword");
                }
                mappingBuilder.endObject();
                mappingBuilder.startObject("seq");
                {
                    mappingBuilder.field("type", "integer");
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                .addMapping("_doc", Strings.toString(mappingBuilder), XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        ensureGreen(index);
    }

    public void prepareReindexAndScrollIndex(String index) throws IOException {
        prepareReindexAndScrollIndex(index, EngineSettings.ENGINE_HAVENASK);
    }
}
