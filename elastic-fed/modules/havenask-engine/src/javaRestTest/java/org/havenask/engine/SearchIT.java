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
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.query.HnswQueryBuilder;
import org.havenask.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SearchIT extends AbstractHavenaskRestTestCase {
    public void testSingleShardKnn() throws Exception {
        String index = "single_shard_test";
        String fieldName = "image";
        int vectorDims = 2;
        int dataNum = 4;

        String[] ids = { "1", "2", "3", "4" };
        float[][] images = new float[][] { { 1.1f, 1.1f }, { 2.2f, 2.2f }, { 3.3f, 3.3f }, { 4.4f, 4.4f } };

        String[] resSourceAsString = { "{\"image\":[1.1,1.1]}", "{\"image\":[2.2,2.2]}", "{\"image\":[3.3,3.3]}", "{\"image\":[4.4,4.4]}" };
        double delta = 0.001;

        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .build()
                    ).mapping(createMapping(vectorDims, fieldName)),
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
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);

        for (int i = 0; i < dataNum; i++) {
            assertEquals(Integer.toString(i + 1), searchResponse.getHits().getHits()[i].getId());
            assertEquals(resSourceAsString[i], searchResponse.getHits().getHits()[i].getSourceAsString());
            assertEquals(dataNum - i, searchResponse.getHits().getHits()[i].getScore(), delta);
        }

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    public void testMultiShardKnn() throws Exception {
        String index = "multi_shard_test";
        String fieldName = "image";
        int vectorDims = 2;
        int dataNum = 8;

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

        Set<String> resSourceAsString = new HashSet<>();
        resSourceAsString.add("{\"image\":[1.1,1.1]}");
        resSourceAsString.add("{\"image\":[2.2,2.2]}");
        resSourceAsString.add("{\"image\":[3.3,3.3]}");
        resSourceAsString.add("{\"image\":[4.4,4.4]}");
        resSourceAsString.add("{\"image\":[5.5,5.5]}");
        resSourceAsString.add("{\"image\":[6.6,6.6]}");
        resSourceAsString.add("{\"image\":[7.7,7.7]}");
        resSourceAsString.add("{\"image\":[8.8,8.8]}");

        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put("index.number_of_shards", randomIntBetween(2, 5))
                            // TODO 目前 replicas 不为零时索引会一直是 yellow，将replicas指定为0，后续增加相关测试
                            .put("index.number_of_replicas", 0)
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .build()
                    ).mapping(createMapping(vectorDims, fieldName)),
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
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);
        for (int i = 0; i < dataNum; i++) {
            assertTrue(resSourceAsString.contains(searchResponse.getHits().getHits()[i].getSourceAsString()));
            // TODO 后续返回结果支持包含score以后, 增加对score的相关测试
        }

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    private static XContentBuilder createMapping(int vectorDims, String fieldName) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "dense_vector")
            .field("dims", vectorDims)
            .endObject()
            .endObject()
            .endObject();
        return mappingBuilder;
    }
}
