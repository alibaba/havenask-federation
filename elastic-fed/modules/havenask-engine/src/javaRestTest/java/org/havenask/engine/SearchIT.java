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

package org.havenask.engine;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.get.GetResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.client.RequestOptions;
import org.havenask.client.ha.SqlResponse;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.collect.List;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilders;
import org.havenask.index.reindex.ReindexRequest;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.junit.AfterClass;

public class SearchIT extends AbstractHavenaskRestTestCase {
    // static logger
    private static final Logger logger = LogManager.getLogger(SearchIT.class);
    private static Set<String> searchITIndices = new HashSet<>();

    @AfterClass
    public static void cleanIndices() {
        try {
            for (String index : searchITIndices) {
                if (highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                    highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                    logger.info("clean index {}", index);
                }
            }
        } catch (IOException e) {
            logger.error("clean index failed", e);
        }
    }

    public void testSearch() throws Exception {
        String index = "search_test";
        searchITIndices.add(index);

        double delta = 0.00001;
        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        // create index
        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put(NUMBER_OF_SHARDS, shardsNum)
            .put(NUMBER_OF_REPLICAS, replicasNum)
            .build();

        java.util.Map<String, ?> map = Map.of("properties", Map.of("seq", Map.of("type", "integer"), "content", Map.of("type", "text")));
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        // PUT docs
        int dataNum = randomIntBetween(100, 200);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(
                new IndexRequest(index).id(String.valueOf(i)).source(Map.of("seq", i, "content", "欢迎使用 " + i), XContentType.JSON)
            );
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get data with _search API
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(dataNum);
        searchRequest.source(searchSourceBuilder);

        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getHits().length);

        for (int i = 0; i < dataNum; i++) {
            assertEquals(index, searchResponse.getHits().getHits()[i].getIndex());
            assertEquals(1.0, searchResponse.getHits().getHits()[i].getScore(), delta);
            int seqNum = (int) searchResponse.getHits().getHits()[i].getSourceAsMap().get("seq");
            assertEquals("欢迎使用 " + seqNum, searchResponse.getHits().getHits()[i].getSourceAsMap().get("content"));
        }

        // test match search
        SearchRequest matchSearchRequest = new SearchRequest(index);
        SearchSourceBuilder matchSearchSourceBuilder = new SearchSourceBuilder();
        matchSearchSourceBuilder.query(new MatchQueryBuilder("content", "欢迎使用"));
        matchSearchSourceBuilder.size(dataNum);
        matchSearchRequest.source(matchSearchSourceBuilder);

        assertBusy(() -> {
            SearchResponse matchSearchResponse = highLevelClient().search(matchSearchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, matchSearchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);

        // test term search
        SearchRequest termSearchRequest = new SearchRequest(index);
        SearchSourceBuilder termSearchSourceBuilder = new SearchSourceBuilder();
        termSearchSourceBuilder.query(QueryBuilders.termQuery("seq", 1));
        termSearchRequest.source(termSearchSourceBuilder);
        assertBusy(() -> {
            SearchResponse termSearchResponse = highLevelClient().search(termSearchRequest, RequestOptions.DEFAULT);
            assertEquals(1, termSearchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);

        // delete index and HEAD index
        deleteAndHeadIndex(index);
    }

    public void testSingleShardKnn() throws Exception {
        String index = "single_shard_test";
        searchITIndices.add(index);

        String fieldName = "image";
        String similarity = "l2_norm";
        int vectorDims = 2;
        int dataNum = 4;

        String[] ids = { "1", "2", "3", "4" };
        float[][] images = new float[][] { { 1.1f, 1.1f }, { 2.2f, 2.2f }, { 3.3f, 3.3f }, { 4.4f, 4.4f } };
        float[] expectedScores = new float[] { 0.6329114f, 0.32051283f, 0.20491804f, 0.07680491f };
        String[] expectedId = { "2", "1", "3", "4" };

        String[] resSourceAsString = { "{\"image\":[2.2,2.2]}", "{\"image\":[1.1,1.1]}", "{\"image\":[3.3,3.3]}", "{\"image\":[4.4,4.4]}" };
        double delta = 0.0000001;

        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .build()
                    ).mapping(createMapping(fieldName, vectorDims, similarity)),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(new IndexRequest(index).id(ids[i]).source(Map.of(fieldName, images[i]), XContentType.JSON));
        }

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get data with _search
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder(fieldName, new float[] { 1.5f, 2.5f }, dataNum, dataNum, null)));
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getHits().length);

        for (int i = 0; i < dataNum; i++) {
            assertEquals(expectedId[i], searchResponse.getHits().getHits()[i].getId());
            assertEquals(resSourceAsString[i], searchResponse.getHits().getHits()[i].getSourceAsString());
            assertEquals(expectedScores[i], searchResponse.getHits().getHits()[i].getScore(), delta);
        }

        // delete index and HEAD index
        deleteAndHeadIndex(index);
    }

    public void testMultiShardKnn() throws Exception {
        String index = "multi_shard_test";
        searchITIndices.add(index);

        String fieldName = "image";
        String similarity = "l2_norm";
        int vectorDims = 2;

        ClusterHealthResponse chResponse = highLevelClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put("index.number_of_shards", shardsNum)
                            .put("index.number_of_replicas", replicasNum)
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .build()
                    ).mapping(createMapping(fieldName, vectorDims, similarity)),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        int dataNum = randomIntBetween(100, 200);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(Map.of(fieldName, new float[] { (float) (i + 0.1), (float) (i + 0.1) }), XContentType.JSON)
            );
        }

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get data with _search
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder(fieldName, new float[] { 0.1f, 0.1f }, dataNum, dataNum, null)));
        searchSourceBuilder.size(dataNum);
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getHits().length);

        for (int i = 0; i < dataNum; i++) {
            assertEquals(String.valueOf(i), searchResponse.getHits().getHits()[i].getId());
            assertEquals(
                String.format(Locale.ROOT, "{\"image\":[%.1f,%.1f]}", (float) (i + 0.1), (float) (i + 0.1)),
                searchResponse.getHits().getHits()[i].getSourceAsString()
            );
        }

        // delete index and HEAD index
        deleteAndHeadIndex(index);
    }

    public void testMultiKnnQuery() throws Exception {
        String index = "multi_vector_test";
        searchITIndices.add(index);

        String[] fieldNames = { "field1", "field2" };
        int[] multiVectorDims = { 2, 2 };
        String[] similarities = { "l2_norm", "dot_product" };

        ClusterHealthResponse chResponse = highLevelClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put("index.number_of_shards", shardsNum)
                            .put("index.number_of_replicas", replicasNum)
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .build()
                    ).mapping(createMultiVectorMapping(fieldNames, multiVectorDims, similarities)),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        int dataNum = randomIntBetween(100, 200);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            float randomFloat = ((float) randomIntBetween(0, 100)) / 100;
            bulkRequest.add(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(
                        Map.of(
                            fieldNames[0],
                            new float[] { i, i },
                            fieldNames[1],
                            new float[] { randomFloat, (float) Math.sqrt((1 - randomFloat * randomFloat)) }
                        ),
                        XContentType.JSON
                    )
            );
        }

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get data with _search
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.knnSearch(
            List.of(
                new KnnSearchBuilder(fieldNames[0], new float[] { 0f, 0f }, dataNum, dataNum, null),
                new KnnSearchBuilder(fieldNames[1], new float[] { 0.6f, 0.8f }, dataNum, dataNum, null)
            )
        );
        searchSourceBuilder.size(dataNum);
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getHits().length);

        // delete index and HEAD index
        deleteAndHeadIndex(index);
    }

    public void testObjectSearch() throws Exception {
        String index = "object_search_test";
        searchITIndices.add(index);

        // create index
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject("user");
                {
                    mappingBuilder.startObject("properties");
                    {
                        mappingBuilder.startObject("name");
                        {
                            mappingBuilder.field("type", "keyword");
                        }
                        mappingBuilder.endObject();
                        mappingBuilder.startObject("image_vector");
                        {
                            mappingBuilder.field("type", "vector").field("dims", 2).field("similarity", "l2_norm");
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();

        ClusterHealthResponse chResponse = highLevelClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put("index.number_of_shards", shardsNum)
                            .put("index.number_of_replicas", replicasNum)
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .build()
                    ).mapping(mappingBuilder),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );

        waitIndexGreen(index);

        // put doc
        int dataNum = randomIntBetween(100, 200);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(
                        Map.of("user", Map.of("name", "person" + i, "image_vector", new float[] { (float) (i + 0.1), (float) (i + 0.1) })),
                        XContentType.JSON
                    )
            );
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // search object field
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("user.name", "person0"));
        searchRequest.source(searchSourceBuilder);
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(1, searchResponse.getHits().getHits().length);
            assertTrue(searchResponse.getHits().getHits()[0].getSourceAsString().contains("\"image_vector\":[0.1,0.1]"));
            assertTrue(searchResponse.getHits().getHits()[0].getSourceAsString().contains("\"name\":\"person0\""));
        }, 2, TimeUnit.SECONDS);

        // search object knn
        SearchRequest knnSearchRequest = new SearchRequest(index);
        SearchSourceBuilder knnSearchSourceBuilder = new SearchSourceBuilder();
        knnSearchSourceBuilder.query(QueryBuilders.matchAllQuery());
        knnSearchSourceBuilder.knnSearch(
            List.of(new KnnSearchBuilder("user.image_vector", new float[] { 1.5f, 1.2f }, dataNum, dataNum, null))
        );
        knnSearchSourceBuilder.size(dataNum);
        knnSearchRequest.source(knnSearchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse knnSearchResponse = highLevelClient().search(knnSearchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, knnSearchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);

        deleteAndHeadIndex(index);
    }

    public void testSourceFilteringSearch() throws Exception {
        String index = "source_filtering_test";
        searchITIndices.add(index);

        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        assertTrue(
            createTestIndex(
                index,
                Settings.builder()
                    .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                    .put("number_of_shards", shardsNum)
                    .put("number_of_replicas", replicasNum)
                    .build(),
                Map.of(
                    "properties",
                    Map.of(
                        "user",
                        Map.of("properties", Map.of("name", Map.of("type", "keyword"), "password", Map.of("type", "keyword"))),
                        "title",
                        Map.of("type", "keyword"),
                        "content",
                        Map.of("type", "text")
                    )
                )
            )
        );

        // put doc
        int dataNum = randomIntBetween(100, 200);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(
                        Map.of(
                            "user",
                            Map.of("name", "person" + i, "password", "password" + i),
                            "title",
                            "title" + i,
                            "content",
                            "content" + i
                        ),
                        XContentType.JSON
                    )
            );
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // test source filtering

        // test include
        sourceFilteringSearch(
            index,
            dataNum,
            new String[] { "user.*" },
            null,
            new String[] { "name", "password" },
            new String[] { "title", "content" }
        );

        // test exclude
        sourceFilteringSearch(
            index,
            dataNum,
            null,
            new String[] { "user.*", "content" },
            new String[] { "title" },
            new String[] { "name", "password", "content" }
        );

        // test include & exclude
        sourceFilteringSearch(
            index,
            dataNum,
            new String[] { "user.*", "title" },
            new String[] { "content", "user.name" },
            new String[] { "password", "title" },
            new String[] { "name", "content" }
        );

        // delete index
        deleteAndHeadIndex(index);
    }

    public void testLuceneIndexSearch() throws Exception {
        String index = "lucene_index_test";
        searchITIndices.add(index);

        // create index
        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        assertTrue(
            createTestIndex(
                index,
                Settings.builder()
                    .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_LUCENE)
                    .put("number_of_shards", shardsNum)
                    .put("number_of_replicas", replicasNum)
                    .build(),
                Map.of("properties", Map.of("user", Map.of("type", "keyword"), "content", Map.of("type", "text")))
            )
        );

        // put doc
        int dataNum = randomIntBetween(100, 200);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(Map.of("user", "user" + i, "content", "content" + i), XContentType.JSON)
            );
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // search doc
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(dataNum);
        searchRequest.source(searchSourceBuilder);

        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
            for (int i = 0; i < dataNum; i++) {
                assertEquals(index, searchResponse.getHits().getHits()[i].getIndex());
                assertTrue(searchResponse.getHits().getHits()[i].getSourceAsString().contains("user"));
                assertTrue(searchResponse.getHits().getHits()[i].getSourceAsString().contains("content"));
            }
        }, 2, TimeUnit.SECONDS);

        // update doc
        int updateNum = randomIntBetween(10, 20);
        for (int i = 0; i < updateNum; i++) {
            updateDoc(index, String.valueOf(i), Map.of("user", "update_user" + i, "content", "update_content" + i));
        }

        // get doc
        for (int i = 0; i < updateNum; i++) {
            GetResponse getResponse = getDocById(index, String.valueOf(i));
            assertEquals(index, getResponse.getIndex());
            assertTrue(getResponse.getSourceAsString().contains("update_user" + i));
            assertTrue(getResponse.getSourceAsString().contains("update_content" + i));
        }

        // delete index
        deleteAndHeadIndex(index);
    }

    public void testLuceneAndHavenaskIndexSearch() throws Exception {
        String luceneIndex = "lucene_index_test";
        String havenaskIndex = "havenask_index_test";
        searchITIndices.add(luceneIndex);
        searchITIndices.add(havenaskIndex);

        // create index
        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();
        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        assertTrue(
            createTestIndex(
                luceneIndex,
                Settings.builder()
                    .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_LUCENE)
                    .put("number_of_shards", shardsNum)
                    .put("number_of_replicas", replicasNum)
                    .build(),
                Map.of("properties", Map.of("user", Map.of("type", "keyword"), "content", Map.of("type", "text")))
            )
        );

        assertTrue(
            createTestIndex(
                havenaskIndex,
                Settings.builder()
                    .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                    .put("number_of_shards", shardsNum)
                    .put("number_of_replicas", replicasNum)
                    .build(),
                Map.of("properties", Map.of("user", Map.of("type", "keyword"), "content", Map.of("type", "text")))
            )
        );

        // put doc
        int luceneDataNum = randomIntBetween(100, 200);
        BulkRequest luceneBulkRequest = new BulkRequest();
        for (int i = 0; i < luceneDataNum; i++) {
            luceneBulkRequest.add(
                new IndexRequest(luceneIndex).id(String.valueOf(i))
                    .source(Map.of("user", "lucene_user" + i, "content", "lucene_content" + i), XContentType.JSON)
            );
        }
        highLevelClient().bulk(luceneBulkRequest, RequestOptions.DEFAULT);

        int havenaskDataNum = randomIntBetween(100, 200);
        BulkRequest havenaskBulkRequest = new BulkRequest();
        for (int i = 0; i < havenaskDataNum; i++) {
            havenaskBulkRequest.add(
                new IndexRequest(havenaskIndex).id(String.valueOf(i))
                    .source(Map.of("user", "havenask_user" + i, "content", "havenask_content" + i), XContentType.JSON)
            );
        }
        highLevelClient().bulk(havenaskBulkRequest, RequestOptions.DEFAULT);

        // search doc
        int totalDataNum = luceneDataNum + havenaskDataNum;

        SearchRequest searchRequest = new SearchRequest(luceneIndex, havenaskIndex);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(totalDataNum);
        searchRequest.source(searchSourceBuilder);
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(totalDataNum, searchResponse.getHits().getHits().length);
        }, 2, TimeUnit.SECONDS);

        // delete index
        deleteAndHeadIndex(luceneIndex);
        deleteAndHeadIndex(havenaskIndex);
    }

    public void testVectorWithCategory() throws Exception {
        String index = "vector_with_category_test";
        searchITIndices.add(index);
        // create index
        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();
        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);

        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject("image_vector");
                {
                    mappingBuilder.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    mappingBuilder.field("dims", 3);
                    mappingBuilder.field("similarity", "l2_norm");
                    mappingBuilder.field("category", "file_type");
                }
                mappingBuilder.endObject();
                mappingBuilder.startObject("file_type");
                {
                    mappingBuilder.field("type", "keyword");
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();

        assertTrue(
            createTestIndex(
                index,
                Settings.builder()
                    .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                    .put("number_of_shards", shardsNum)
                    .put("number_of_replicas", replicasNum)
                    .build(),
                mappingBuilder
            )
        );

        waitIndexGreen(index);

        // put doc
        BulkRequest bulkRequest = new BulkRequest();
        int docNum = randomIntBetween(20, 40);
        int jpgNum = docNum / 2;
        int pngNum = docNum - jpgNum;
        for (int i = 0; i < docNum; i++) {
            String fileType = i < jpgNum ? "jpg" : "png";
            bulkRequest.add(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(
                        Map.of("image_vector", new float[] { randomFloat(), randomFloat(), randomFloat() }, "file_type", fileType),
                        XContentType.JSON
                    )
            );
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get doc with sql
        assertBusy(() -> {
            SqlResponse sqlResponse = getSqlResponse(
                String.format(
                    Locale.ROOT,
                    "select _id, file_type, ((1/(1+vector_score('image_vector')))) as _score "
                        + "from `%s` where MATCHINDEX('image_vector', 'jpg#-5.0,9.0,-12.0&n=10') "
                        + "order by _score desc limit 40 offset 0",
                    index
                )
            );
            assertEquals(jpgNum, sqlResponse.getSqlResult().getData().length);
        }, 20, TimeUnit.SECONDS);

        assertBusy(() -> {
            SqlResponse sqlResponse = getSqlResponse(
                String.format(
                    Locale.ROOT,
                    "select _id, file_type, ((1/(1+vector_score('image_vector')))) as _score "
                        + "from `%s` where MATCHINDEX('image_vector', 'png#-5.0,9.0,-12.0&n=10') "
                        + "order by _score desc limit 40 offset 0",
                    index
                )
            );
            assertEquals(pngNum, sqlResponse.getSqlResult().getData().length);
        }, 2, TimeUnit.SECONDS);

        // delete index and HEAD index
        deleteAndHeadIndex(index);
    }

    public void testReindex() throws Exception {
        String sourceIndex = "reindex_source_test";
        String destIndex = "reindex_dest_test";

        // create index
        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();
        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);

        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject("seq");
                {
                    mappingBuilder.field("type", "integer");
                }
                mappingBuilder.endObject();
                mappingBuilder.startObject("key");
                {
                    mappingBuilder.field("type", "keyword");
                }
                mappingBuilder.endObject();
                mappingBuilder.startObject("content");
                {
                    mappingBuilder.field("type", "text");
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();

        assertTrue(
            createTestIndex(
                sourceIndex,
                Settings.builder()
                    .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                    .put("number_of_shards", shardsNum)
                    .put("number_of_replicas", replicasNum)
                    .build(),
                mappingBuilder
            )
        );
        assertTrue(
            createTestIndex(
                destIndex,
                Settings.builder()
                    .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                    .put("number_of_shards", shardsNum)
                    .put("number_of_replicas", replicasNum)
                    .build(),
                mappingBuilder
            )
        );

        waitIndexGreen(sourceIndex);
        waitIndexGreen(destIndex);

        // bulk doc
        BulkRequest bulkRequest = new BulkRequest();
        int docNum = randomIntBetween(10000, 20000);
        for (int i = 0; i < docNum; i++) {
            bulkRequest.add(
                new IndexRequest(sourceIndex).id(String.valueOf(i))
                    .source(Map.of("seq", i, "key", "key" + i, "content", "欢迎使用 " + i), XContentType.JSON)
            );
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get sourceIndex data wtih search
        SearchRequest sourceSearchRequest = new SearchRequest(sourceIndex);
        SearchSourceBuilder sourceSearchSourceBuilder = new SearchSourceBuilder();
        sourceSearchSourceBuilder.size(docNum);
        sourceSearchRequest.source(sourceSearchSourceBuilder);

        assertBusy(() -> {
            SearchResponse sourceSearchResponse = highLevelClient().search(sourceSearchRequest, RequestOptions.DEFAULT);
            assertEquals(docNum, sourceSearchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(sourceSearchRequest, RequestOptions.DEFAULT);
        assertEquals(docNum, searchResponse.getHits().getHits().length);

        // reindex
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndex);
        reindexRequest.setDestIndex(destIndex);
        highLevelClient().reindex(reindexRequest, RequestOptions.DEFAULT);

        // get destIndex data with search
        SearchRequest destSearchRequest = new SearchRequest(destIndex);
        SearchSourceBuilder destSearchSourceBuilder = new SearchSourceBuilder();
        destSearchSourceBuilder.size(docNum);
        destSearchRequest.source(destSearchSourceBuilder);
        assertBusy(() -> {
            SearchResponse destSearchResponse = highLevelClient().search(destSearchRequest, RequestOptions.DEFAULT);
            assertEquals(docNum, destSearchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);

        // delete index
        deleteAndHeadIndex(sourceIndex);
        deleteAndHeadIndex(destIndex);
    }

    private static XContentBuilder createMapping(String fieldName, int vectorDims, String similarity) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", DenseVectorFieldMapper.CONTENT_TYPE)
            .field("dims", vectorDims)
            .field("similarity", similarity)
            .endObject()
            .endObject()
            .endObject();
        return mappingBuilder;
    }

    private static XContentBuilder createMultiVectorMapping(String[] fieldNames, int[] multiVectorDims, String[] similaritys)
        throws IOException {
        if (fieldNames.length != multiVectorDims.length || fieldNames.length != similaritys.length) {
            throw new IllegalArgumentException("createMultiVectorMapping error：invalid params length");
        }
        int length = multiVectorDims.length;
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                for (int i = 0; i < length; i++) {
                    mappingBuilder.startObject(fieldNames[i]);
                    {
                        mappingBuilder.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                        mappingBuilder.field("dims", multiVectorDims[i]);
                        mappingBuilder.field("similarity", similaritys[i]);
                    }
                    mappingBuilder.endObject();
                }
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        return mappingBuilder;
    }

    private void sourceFilteringSearch(
        String index,
        int dataNum,
        String[] includes,
        String[] excludes,
        String[] expectedIncludes,
        String[] expectedExcludes
    ) throws Exception {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(includes, excludes);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(dataNum);
        searchRequest.source(searchSourceBuilder);
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
            for (int i = 0; i < dataNum; i++) {
                for (String include : expectedIncludes) {
                    assertTrue(searchResponse.getHits().getHits()[i].getSourceAsString().contains(include));
                }
                for (String exclude : expectedExcludes) {
                    assertFalse(searchResponse.getHits().getHits()[i].getSourceAsString().contains(exclude));
                }
            }
        }, 2, TimeUnit.SECONDS);
    }
}
