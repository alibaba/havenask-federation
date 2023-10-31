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
import java.util.concurrent.TimeUnit;

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
import org.havenask.engine.index.query.HnswQueryBuilder;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;

public class SearchIT extends AbstractHavenaskRestTestCase {
    public void testSingleShardKnn() throws Exception {
        String index = "single_shard_test";
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
        HnswQueryBuilder hnswQueryBuilder = new HnswQueryBuilder(fieldName, new float[] { 1.5f, 2.5f }, 10);
        searchSourceBuilder.query(hnswQueryBuilder);
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

        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            // TODO 暂时只支持单shard
                            // .put("index.number_of_shards", randomIntBetween(2, 5))
                            // TODO 目前 replicas 不为零时索引会一直是 yellow，将replicas指定为0，后续增加相关测试
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
        HnswQueryBuilder hnswQueryBuilder = new HnswQueryBuilder(fieldName, new float[] { 1.1f, 1.1f }, 10);
        searchSourceBuilder.query(hnswQueryBuilder);
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

        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
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

    private static XContentBuilder createMapping(String fieldName, int vectorDims, String similarity) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "dense_vector")
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
                        mappingBuilder.field("type", "dense_vector");
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
