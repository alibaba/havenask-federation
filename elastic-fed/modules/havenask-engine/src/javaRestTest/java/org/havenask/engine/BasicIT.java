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
import org.havenask.client.RequestOptions;
import org.havenask.client.ha.SqlClientInfoRequest;
import org.havenask.client.ha.SqlClientInfoResponse;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.client.indices.GetIndexResponse;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.engine.EngineSettings;

public class BasicIT extends AbstractHavenaskRestTestCase {
    public void testCRUD() throws Exception {
        String index = "test";
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
        assertTrue(tables.containsKey(index + "_0"));

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

}
