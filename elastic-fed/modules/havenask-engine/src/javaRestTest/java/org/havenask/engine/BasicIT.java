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

import java.util.concurrent.TimeUnit;

import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.delete.DeleteRequest;
import org.havenask.action.index.IndexRequest;
import org.havenask.client.RequestOptions;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.client.indices.GetIndexResponse;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;

public class BasicIT extends AbstractHavenaskRestTestCase {
    public void testCRUD() throws Exception {
        String index = "test";
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
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

        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
    }

    // create index, get index, delete index, HEAD index and set mapping
    public void testIndexMethod() throws Exception {
        String index = "my_index1";
        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
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
        assertEquals(
            getIndexResponse.getMappings().get(index),
            new MappingMetadata(
                "_doc",
                Map.of(
                    "dynamic",
                    "false",
                    "properties",
                    Map.of("content", Map.of("type", "keyword"), "seq", Map.of("type", "integer"), "time", Map.of("type", "date"))
                )
            )
        );

        // delete index and HEAD index
        assertEquals(true, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    // test document api, PUT/POST/DELETE and bulk
    public void testDocMethod() throws Exception {
        String index = "my_index2";
        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
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

        // PUT doc
        highLevelClient().index(
            new IndexRequest(index).id("1").source(Map.of("seq", 1, "content", "欢迎使用1", "time", "20230718"), XContentType.JSON),
            RequestOptions.DEFAULT
        );
        highLevelClient().index(
            new IndexRequest(index).id("2").source(Map.of("seq", 2, "content", "欢迎使用2", "time", "20230717"), XContentType.JSON),
            RequestOptions.DEFAULT
        );
        highLevelClient().index(
            new IndexRequest(index).id("3").source(Map.of("seq", 3, "content", "欢迎使用3", "time", "20230716"), XContentType.JSON),
            RequestOptions.DEFAULT
        );

        // POST doc
        highLevelClient().index(
            new IndexRequest(index).source(Map.of("seq", 4, "content", "欢迎使用4", "time", "20230715"), XContentType.JSON),
            RequestOptions.DEFAULT
        );

        // DELETE doc
        highLevelClient().delete(new DeleteRequest(index, "1"), RequestOptions.DEFAULT);
        highLevelClient().delete(new DeleteRequest(index, "2"), RequestOptions.DEFAULT);
        highLevelClient().delete(new DeleteRequest(index, "3"), RequestOptions.DEFAULT);

        // bulk doc
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest(index).id("1").source(Map.of("seq", 1, "content", "欢迎使用1", "time", "20230718"), XContentType.JSON)
        );
        bulkRequest.add(
            new IndexRequest(index).opType("create")
                .id("2")
                .source(Map.of("seq", 2, "content", "欢迎使用2", "time", "20230717"), XContentType.JSON)
        );
        bulkRequest.add(
            new IndexRequest(index).id("3").source(Map.of("seq", 3, "content", "欢迎使用3", "time", "20230716"), XContentType.JSON)
        );
        bulkRequest.add(new DeleteRequest(index, "3"));
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));

        // todo: check data using sql search api
    }

    // test common data type(int, double, boolean, date, text, keyword, array)
    public void testMultiDataType() throws Exception {
        String index = "my_index3";
        // create index with multi data type
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
                    )
                        .mapping(
                            Map.of(
                                "properties",
                                Map.of(
                                    "keyword",
                                    Map.of("type", "keyword"),
                                    "text",
                                    Map.of("type", "text"),
                                    "integer",
                                    Map.of("type", "integer"),
                                    "double",
                                    Map.of("type", "double"),
                                    "date",
                                    Map.of("type", "date"),
                                    "boolean",
                                    Map.of("type", "boolean"),
                                    "integer_array",
                                    Map.of("type", "integer")
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

        highLevelClient().index(
            new IndexRequest(index).id("1").source(Map.of("keyword", "keyword_test"), XContentType.JSON),
            RequestOptions.DEFAULT
        );

        highLevelClient().bulk(
            new BulkRequest().add(
                new IndexRequest(index).id("2")
                    .source(
                        Map.of(
                            "keyword",
                            "keyword",
                            "text",
                            "text",
                            "integer",
                            1,
                            "double",
                            1.0,
                            "date",
                            "2020-01-01",
                            "boolean",
                            true,
                            "integer_array",
                            new int[] { 1, 2, 3 }
                        ),
                        XContentType.JSON
                    )
            )
                .add(
                    new IndexRequest(index).id("3")
                        .source(
                            Map.of(
                                "keyword",
                                1,
                                "text",
                                2,
                                "integer",
                                "-32768",
                                "double",
                                "3.14",
                                "date",
                                "20230718",
                                "boolean",
                                "false",
                                "integer_array",
                                new int[] { 2, 3, 4 }
                            ),
                            XContentType.JSON
                        )
                ),
            RequestOptions.DEFAULT
        );

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));

        // todo: check data using sql search api
    }

}
