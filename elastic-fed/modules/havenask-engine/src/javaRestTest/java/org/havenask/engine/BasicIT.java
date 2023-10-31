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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.get.GetRequest;
import org.havenask.action.get.GetResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.client.RequestOptions;
import org.havenask.client.ha.SqlClientInfoRequest;
import org.havenask.client.ha.SqlClientInfoResponse;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.client.indices.GetIndexResponse;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.collect.Map;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.Maps;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.index.mapper.MapperService;

public class BasicIT extends AbstractHavenaskRestTestCase {
    public void testCRUD() throws Exception {
        String index = "index_crud";
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .put("number_of_replicas", 0)
                            .build()
                    ),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );

        GetIndexResponse getIndexResponse = highLevelClient().indices().get(new GetIndexRequest(index), RequestOptions.DEFAULT);
        assertEquals(getIndexResponse.getIndices().length, 1);
        assertEquals(getIndexResponse.getSetting(index, EngineSettings.ENGINE_TYPE_SETTING.getKey()), EngineSettings.ENGINE_HAVENASK);
        assertEquals(getIndexResponse.getSetting(index, "index.number_of_replicas"), "0");
        assertEquals(getIndexResponse.getMappings().get(index), new MappingMetadata("_doc", Map.of("dynamic", "false")));

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        SqlClientInfoResponse sqlClientInfoResponse = highLevelClient().havenask()
            .sqlClientInfo(new SqlClientInfoRequest(), RequestOptions.DEFAULT);
        assertEquals(sqlClientInfoResponse.getErrorCode(), 0);
        assertEquals(sqlClientInfoResponse.getErrorMessage(), "");

        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> tables = (java.util.Map<String, Object>) ((java.util.Map<String, Object>) (((java.util.Map<
            String,
            Object>) (sqlClientInfoResponse.getResult().get("default"))).get("general"))).get("tables");
        assertTrue(tables.containsKey(index));

        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
    }

    // create index, get index, delete index, HEAD index and set mapping
    public void testIndexMethod() throws Exception {
        String index = "index_index_method";
        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .put("number_of_replicas", 0)
                            .build()
                    )
                        .mapping(
                            Map.of(
                                "properties",
                                Map.of(
                                    "seq",
                                    Map.of("type", "integer"),
                                    "content",
                                    Map.of("type", "keyword"),
                                    "time",
                                    Map.of("type", "date")
                                )
                            )
                        ),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        // get index
        GetIndexResponse getIndexResponse = highLevelClient().indices().get(new GetIndexRequest(index), RequestOptions.DEFAULT);
        assertEquals(getIndexResponse.getIndices().length, 1);
        assertEquals(getIndexResponse.getSetting(index, EngineSettings.ENGINE_TYPE_SETTING.getKey()), EngineSettings.ENGINE_HAVENASK);
        assertEquals(getIndexResponse.getSetting(index, "index.number_of_replicas"), "0");

        MappingMetadata expectedMappingMetaData = new MappingMetadata(
            "_doc",
            Map.of(
                "dynamic",
                "false",
                "properties",
                Map.of("content", Map.of("type", "keyword"), "seq", Map.of("type", "integer"), "time", Map.of("type", "date"))
            )
        );
        MappingMetadata resMappingMetaData = getIndexResponse.getMappings().get(index);
        assertEquals(expectedMappingMetaData.type(), resMappingMetaData.type());
        assertEquals(expectedMappingMetaData.routing(), resMappingMetaData.routing());
        assertTrue(mappingsEquals(expectedMappingMetaData.source(), resMappingMetaData.source()));

        // delete index and HEAD index
        assertEquals(true, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    public void testCreateAndDeleteSameIndex() throws Exception {
        int randomTimes = randomIntBetween(2, 6);
        for (int i = 0; i < randomTimes; i++) {
            int shardsNum = randomIntBetween(1, 6);
            String index = "create_and_delete_test";
            // create index
            assertTrue(
                highLevelClient().indices()
                    .create(
                        new CreateIndexRequest(index).settings(
                            Settings.builder()
                                .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                                // TODO 暂时只支持单shard
                                //.put("index.number_of_shards", shardsNum)
                                .put("number_of_replicas", 0)
                                .build()
                        ).mapping(Map.of("properties", Map.of("content" + i, Map.of("type", "keyword")))),
                        RequestOptions.DEFAULT
                    )
                    .isAcknowledged()
            );
            assertBusy(() -> {
                ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                    .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
                assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
            }, 2, TimeUnit.MINUTES);

            // GET index
            assertEquals(true, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
            GetIndexResponse getIndexResponse = highLevelClient().indices().get(new GetIndexRequest(index), RequestOptions.DEFAULT);
            MappingMetadata expectedMappingMetaData = new MappingMetadata(
                "_doc",
                Map.of("dynamic", "false", "properties", Map.of("content" + i, Map.of("type", "keyword")))
            );
            assertTrue(mappingsEquals(expectedMappingMetaData.source(), getIndexResponse.getMappings().get(index).source()));

            // put doc
            int randomDocNum = randomIntBetween(1, 4);
            for (int j = 0; j < randomDocNum; j++) {
                highLevelClient().index(
                    new IndexRequest(index).id(String.valueOf(j)).source(Map.of("content" + i, "欢迎使用" + j), XContentType.JSON),
                    RequestOptions.DEFAULT
                );
            }

            // get doc
            for (int j = 0; j < randomDocNum; j++) {
                String curId = String.valueOf(j);
                assertBusy(() -> {
                    GetResponse getResponse = highLevelClient().get(new GetRequest(index, curId), RequestOptions.DEFAULT);
                    assertEquals(true, getResponse.isExists());
                }, 10, TimeUnit.SECONDS);

                GetResponse getResponse = highLevelClient().get(new GetRequest(index, String.valueOf(j)), RequestOptions.DEFAULT);
                assertEquals("欢迎使用" + j, getResponse.getSourceAsMap().get("content" + i));
            }

            // delete index
            assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
            assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
        }
    }

    public void testCreateAndDeleteDiffIndex() throws Exception {
        int randomNum = randomIntBetween(2, 6);
        String baseName = "create_and_delete_test";
        List<String> indexs = new ArrayList<>();
        for (int i = 0; i < randomNum; i++) {
            indexs.add(baseName + i);
        }

        // create indexs
        for (int i = 0; i < randomNum; i++) {
            int shardsNum = randomIntBetween(1, 6);
            assertTrue(
                highLevelClient().indices()
                    .create(
                        new CreateIndexRequest(indexs.get(i)).settings(
                            Settings.builder()
                                .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                                // TODO 暂时只支持单shard
                                //.put("index.number_of_shards", shardsNum)
                                .put("number_of_replicas", 0)
                                .build()
                        ).mapping(Map.of("properties", Map.of("content" + i, Map.of("type", "keyword")))),
                        RequestOptions.DEFAULT
                    )
                    .isAcknowledged()
            );
        }

        for (int i = 0; i < randomNum; i++) {
            String curIndex = indexs.get(i);
            assertBusy(() -> {
                ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                    .health(new ClusterHealthRequest(curIndex), RequestOptions.DEFAULT);
                assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
            }, 2, TimeUnit.MINUTES);
        }

        // get index
        for (int i = 0; i < randomNum; i++) {
            assertEquals(true, highLevelClient().indices().exists(new GetIndexRequest(indexs.get(i)), RequestOptions.DEFAULT));
            GetIndexResponse getIndexResponse = highLevelClient().indices().get(new GetIndexRequest(indexs.get(i)), RequestOptions.DEFAULT);
            MappingMetadata expectedMappingMetaData = new MappingMetadata(
                "_doc",
                Map.of("dynamic", "false", "properties", Map.of("content" + i, Map.of("type", "keyword")))
            );
            assertTrue(mappingsEquals(expectedMappingMetaData.source(), getIndexResponse.getMappings().get(indexs.get(i)).source()));
        }

        // put and get doc
        for (int i = 0; i < randomNum; i++) {
            int randomDocNum = randomIntBetween(1, 4);
            for (int j = 0; j < randomDocNum; j++) {
                String curId = String.valueOf(i) + String.valueOf(j);
                highLevelClient().index(
                    new IndexRequest(indexs.get(i)).id(curId).source(Map.of("content" + i, "欢迎使用" + j), XContentType.JSON),
                    RequestOptions.DEFAULT
                );
            }

            for (int j = 0; j < randomDocNum; j++) {
                String curId = String.valueOf(i) + String.valueOf(j);
                String curIndex = indexs.get(i);
                assertBusy(() -> {
                    GetResponse getResponse = highLevelClient().get(new GetRequest(curIndex, curId), RequestOptions.DEFAULT);
                    assertEquals(true, getResponse.isExists());
                }, 10, TimeUnit.SECONDS);
                GetResponse getResponse = highLevelClient().get(new GetRequest(curIndex, curId), RequestOptions.DEFAULT);
                assertEquals(true, getResponse.isExists());
                assertEquals("欢迎使用" + j, getResponse.getSourceAsMap().get("content" + i));
            }
        }

        // delete index
        for (int i = 0; i < randomNum; i++) {
            assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(indexs.get(i)), RequestOptions.DEFAULT).isAcknowledged());
            assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(indexs.get(i)), RequestOptions.DEFAULT));
        }
    }

    @SuppressWarnings("unchecked")
    public static java.util.Map<String, Object> reduceMapping(java.util.Map<String, Object> mapping) {
        if (mapping.size() == 1 && MapperService.SINGLE_MAPPING_NAME.equals(mapping.keySet().iterator().next())) {
            return (java.util.Map<String, Object>) mapping.values().iterator().next();
        } else {
            return mapping;
        }
    }

    static boolean mappingsEquals(CompressedXContent m1, CompressedXContent m2) {
        if (m1 == m2) {
            return true;
        }

        if (m1 == null || m2 == null) {
            return false;
        }

        if (m1.equals(m2)) {
            return true;
        }

        java.util.Map<String, Object> thisUncompressedMapping = reduceMapping(
            XContentHelper.convertToMap(m1.uncompressed(), true, XContentType.JSON).v2()
        );
        java.util.Map<String, Object> otherUncompressedMapping = reduceMapping(
            XContentHelper.convertToMap(m2.uncompressed(), true, XContentType.JSON).v2()
        );
        return Maps.deepEquals(thisUncompressedMapping, otherUncompressedMapping);
    }
}
