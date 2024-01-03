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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.client.RequestOptions;
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
import org.havenask.engine.index.query.KnnQueryBuilder;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.junit.AfterClass;

public class SearchIT extends AbstractHavenaskRestTestCase {
    // static logger
    private static final Logger logger = LogManager.getLogger(SearchIT.class);
    private static Set<String> SearchITIndices = new HashSet<>();

    @AfterClass
    public static void cleanIndices() {
        try {
            for (String index : SearchITIndices) {
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
        SearchITIndices.add(index);

        int dataNum = 3;
        double delta = 0.00001;
        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(2, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        // create index
        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put(NUMBER_OF_SHARDS, shardsNum)
            .put(NUMBER_OF_REPLICAS, replicasNum)
            .build();

        java.util.Map<String, ?> map = Map.of(
            "properties",
            Map.of("seq", Map.of("type", "integer"), "content", Map.of("type", "text"), "time", Map.of("type", "date"))
        );
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        // PUT docs
        String[] idList = { "1", "2", "3" };
        java.util.List<java.util.Map<String, ?>> sourceList = new ArrayList<>();
        sourceList.add(Map.of("seq", 1, "content", "欢迎使用 1", "time", "20230718"));
        sourceList.add(Map.of("seq", 2, "content", "欢迎使用 2", "time", "20230717"));
        sourceList.add(Map.of("seq", 3, "content", "欢迎使用 3", "time", "20230716"));
        for (int i = 0; i < idList.length; i++) {
            putDoc(index, idList[i], sourceList.get(i));
        }

        // get data with _search API
        SearchRequest searchRequest = new SearchRequest(index);

        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);

        Set<Integer> expectedSeq = Set.of(1, 2, 3);
        Set<String> expectedContent = Set.of("欢迎使用 1", "欢迎使用 2", "欢迎使用 3");
        Set<String> expectedTime = Set.of("20230718", "20230717", "20230716");
        for (int i = 0; i < dataNum; i++) {
            assertEquals(index, searchResponse.getHits().getHits()[i].getIndex());
            assertEquals(1.0, searchResponse.getHits().getHits()[i].getScore(), delta);
            assertTrue(expectedSeq.contains(searchResponse.getHits().getHits()[i].getSourceAsMap().get("seq")));
            assertTrue(expectedContent.contains(searchResponse.getHits().getHits()[i].getSourceAsMap().get("content")));
            assertTrue(expectedTime.contains(searchResponse.getHits().getHits()[i].getSourceAsMap().get("time")));
        }

        // test match search
        SearchRequest matchSearchRequest = new SearchRequest(index);
        SearchSourceBuilder matchSearchSourceBuilder = new SearchSourceBuilder();
        matchSearchSourceBuilder.query(new MatchQueryBuilder("content", "欢迎使用"));
        matchSearchRequest.source(matchSearchSourceBuilder);

        assertBusy(() -> {
            SearchResponse matchSearchResponse = highLevelClient().search(matchSearchRequest, RequestOptions.DEFAULT);
            assertEquals(3, matchSearchResponse.getHits().getTotalHits().value);
        }, 10, TimeUnit.SECONDS);

        // test term search
        SearchRequest termSearchRequest = new SearchRequest(index);
        SearchSourceBuilder termSearchSourceBuilder = new SearchSourceBuilder();
        termSearchSourceBuilder.query(QueryBuilders.termQuery("seq", 1));
        termSearchRequest.source(termSearchSourceBuilder);
        assertBusy(() -> {
            SearchResponse termSearchResponse = highLevelClient().search(termSearchRequest, RequestOptions.DEFAULT);
            assertEquals(1, termSearchResponse.getHits().getTotalHits().value);
        }, 10, TimeUnit.SECONDS);

        // delete index and HEAD index
        deleteAndHeadIndex(index);
    }

    public void testSingleShardKnn() throws Exception {
        String index = "single_shard_test";
        SearchITIndices.add(index);

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
        KnnQueryBuilder knnQueryBuilder = new KnnQueryBuilder(fieldName, new float[] { 1.5f, 2.5f }, 10);
        searchSourceBuilder.query(knnQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);

        for (int i = 0; i < dataNum; i++) {
            assertEquals(expectedId[i], searchResponse.getHits().getHits()[i].getId());
            assertEquals(resSourceAsString[i], searchResponse.getHits().getHits()[i].getSourceAsString());
            assertEquals(expectedScores[i], searchResponse.getHits().getHits()[i].getScore(), delta);
        }

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    public void testMultiShardKnn() throws Exception {
        String index = "multi_shard_test";
        SearchITIndices.add(index);

        String fieldName = "image";
        String similarity = "l2_norm";
        int vectorDims = 2;
        int dataNum = 8;

        double delta = 0.0000001;

        String[] ids = { "1", "2", "3", "4", "5", "6", "7", "8" };
        float[][] images = new float[][] {
            { 1.1f, 1.1f },
            { 2.2f, 2.2f },
            { 3.3f, 3.3f },
            { 4.4f, 4.4f },
            { 5.5f, 5.5f },
            { 6.6f, 6.6f },
            { 7.7f, 7.7f },
            { 8.8f, 8.8f } };

        String[] resSourceAsString = {
            "{\"image\":[1.1,1.1]}",
            "{\"image\":[2.2,2.2]}",
            "{\"image\":[3.3,3.3]}",
            "{\"image\":[4.4,4.4]}",
            "{\"image\":[5.5,5.5]}",
            "{\"image\":[6.6,6.6]}",
            "{\"image\":[7.7,7.7]}",
            "{\"image\":[8.8,8.8]}" };

        String[] expectedId = { "1", "2", "3", "4", "5", "6", "7", "8" };
        float[] expectedScores = { 1.0f, 0.29239765f, 0.093632974f, 0.04389815f, 0.025176233f, 0.016260162f, 0.011348162f, 0.0083626015f };

        ClusterHealthResponse chResponse = highLevelClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(2, 6);
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

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(new IndexRequest(index).id(ids[i]).source(Map.of(fieldName, images[i]), XContentType.JSON));
        }

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get data with _search
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        KnnQueryBuilder knnQueryBuilder = new KnnQueryBuilder(fieldName, new float[] { 1.1f, 1.1f }, 10);
        searchSourceBuilder.query(knnQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);

        for (int i = 0; i < dataNum; i++) {
            assertEquals(expectedId[i], searchResponse.getHits().getHits()[i].getId());
            assertEquals(resSourceAsString[i], searchResponse.getHits().getHits()[i].getSourceAsString());
            assertEquals(expectedScores[i], searchResponse.getHits().getHits()[i].getScore(), delta);
        }

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    public void testMultiKnnQuery() throws Exception {
        String index = "multi_vector_test";
        SearchITIndices.add(index);

        String[] fieldNames = { "field1", "field2" };
        int[] multiVectorDims = { 2, 2 };
        String[] similarities = { "l2_norm", "dot_product" };
        int dataNum = 4;
        double delta = 0.0000001;

        String[] ids = { "1", "2", "3", "4" };
        float[][] field0Values = { { 1f, 1f }, { 2f, 2f }, { 3f, 3f }, { 4f, 4f } };
        float[][] field1Values = { { 0.6f, 0.8f }, { 0.8f, 0.6f }, { 1.0f, 0.0f }, { 0.0f, 1.0f } };

        String[][] resSourceAsString = {
            { "\"field1\":[1.0,1.0]", "\"field2\":[0.6,0.8]" },
            { "\"field1\":[2.0,2.0]", "\"field2\":[0.8,0.6]" },
            { "\"field1\":[4.0,4.0]", "\"field2\":[0.0,1.0]" },
            { "\"field1\":[3.0,3.0]", "\"field2\":[1.0,0.0]" } };

        String[] expectedId = { "1", "2", "4", "3" };
        float[] expectedScores = { 2f, 1.3133334f, 0.95263153f, 0.9111111f };

        ClusterHealthResponse chResponse = highLevelClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(2, 6);
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

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(
                new IndexRequest(index).id(ids[i])
                    .source(Map.of(fieldNames[0], field0Values[i], fieldNames[1], field1Values[i]), XContentType.JSON)
            );
        }

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get data with _search
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.knnSearch(
            List.of(
                new KnnSearchBuilder(fieldNames[0], new float[] { 1f, 1f }, 20, 20, null),
                new KnnSearchBuilder(fieldNames[1], new float[] { 0.6f, 0.8f }, 10, 10, null)
            )
        );
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);

        for (int i = 0; i < dataNum; i++) {
            assertEquals(expectedId[i], searchResponse.getHits().getHits()[i].getId());
            assertTrue(
                searchResponse.getHits().getHits()[i].getSourceAsString().contains(resSourceAsString[i][0])
                    && searchResponse.getHits().getHits()[i].getSourceAsString().contains(resSourceAsString[i][1])
            );
            assertEquals(expectedScores[i], searchResponse.getHits().getHits()[i].getScore(), delta);
        }

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    public void testObjectSearch() throws Exception {
        String index = "object_search_test";
        SearchITIndices.add(index);

        int dataNum = 3;
        String[] userNames = { "Alice", "Bob", "Eve" };
        float[][] userImages = { { 1.1f, 1.1f }, { 2.2f, 2.2f }, { 3.3f, 3.3f } };

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
                        mappingBuilder.startObject("image");
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

        int shardsNum = randomIntBetween(2, 6);
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
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(Map.of("user", Map.of("name", userNames[i], "image", userImages[i])), XContentType.JSON)
            );
        }
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // search object field
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("user_name", "Alice"));
        searchRequest.source(searchSourceBuilder);
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
        }, 2, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertTrue(searchResponse.getHits().getHits()[0].getSourceAsString().contains("\"image\":[1.1,1.1]"));
        assertTrue(searchResponse.getHits().getHits()[0].getSourceAsString().contains("\"name\":\"Alice\""));

        // search object knn
        SearchRequest knnSearchRequest = new SearchRequest(index);
        SearchSourceBuilder knnSearchSourceBuilder = new SearchSourceBuilder();
        knnSearchSourceBuilder.query(QueryBuilders.matchAllQuery());
        knnSearchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder("user_image", new float[] { 1.5f, 1.2f }, 20, 20, null)));
        knnSearchRequest.source(knnSearchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse knnSearchResponse = highLevelClient().search(knnSearchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, knnSearchResponse.getHits().getTotalHits().value);
        }, 10, TimeUnit.SECONDS);

        deleteAndHeadIndex(index);
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
}
