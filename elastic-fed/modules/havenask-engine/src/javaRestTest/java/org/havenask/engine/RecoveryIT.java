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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.index.IndexRequest;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.client.ha.SqlRequest;
import org.havenask.client.ha.SqlResponse;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;
import org.junit.AfterClass;

public class RecoveryIT extends AbstractHavenaskRestTestCase {

    private static final String index = "index_recovery";

    private static final Logger logger = LogManager.getLogger(RecoveryIT.class);

    private final long testDocCount = 100;

    public void testRecoverySingleShard() throws Exception {
        assumeTrue("number_of_nodes more than 1, Skip func: testRecoverySingleShard()", clusterIsSingleNode());

        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .put("number_of_replicas", 0)
                            .build()
                    ).mapping(Map.of("properties", Map.of("foo", Map.of("type", "keyword")))),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            logger.info("creating index, cluster health is {}", clusterHealthResponse.getStatus());
            assertEquals(ClusterHealthStatus.GREEN, clusterHealthResponse.getStatus());
        }, 2, TimeUnit.MINUTES);

        // write doc
        for (int i = 0; i < testDocCount; i++) {
            highLevelClient().index(
                new IndexRequest(index).source(Map.of("foo", "recovery test " + i), XContentType.JSON),
                RequestOptions.DEFAULT
            );
        }

        // waiting for finishing writing
        assertBusy(() -> {
            SqlResponse beforeStopResponse = highLevelClient().havenask()
                .sql(new SqlRequest("select * from " + index), RequestOptions.DEFAULT);
            logger.info("waiting for finishing writing, now count is {}", beforeStopResponse.getRowCount());
            assertEquals(testDocCount, beforeStopResponse.getRowCount());
        }, 2, TimeUnit.MINUTES);

        // stop searcher
        Response response = highLevelClient().getLowLevelClient().performRequest(new Request("POST", "/_havenask/stop?role=searcher"));
        assertEquals(200, response.getStatusLine().getStatusCode());

        // waiting for clearing doc
        assertBusy(() -> {
            SqlResponse afterStopResponse = highLevelClient().havenask()
                .sql(new SqlRequest("select * from " + index), RequestOptions.DEFAULT);
            logger.info(
                "waiting for sql error or doc clear, now code is {}, doc count is {}",
                afterStopResponse.getErrorInfo().getErrorCode(),
                afterStopResponse.getRowCount()
            );
            assert 8020 == afterStopResponse.getErrorInfo().getErrorCode() || 0 == afterStopResponse.getRowCount();
        }, 2, TimeUnit.MINUTES);

        // wait for cluster health turning to be red
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            logger.info("waiting for status turning to be RED, now is {}", clusterHealthResponse.getStatus());
            assertEquals(ClusterHealthStatus.RED, clusterHealthResponse.getStatus());
        }, 2, TimeUnit.MINUTES);

        // wait for recovery finish and cluster health turns to be green
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            logger.info("recovering from translog, cluster health is {}", clusterHealthResponse.getStatus());
            assertEquals(ClusterHealthStatus.GREEN, clusterHealthResponse.getStatus());
        }, 2, TimeUnit.MINUTES);

        // check recovery result
        assertBusy(() -> {
            SqlResponse recoveryResponse = highLevelClient().havenask()
                .sql(new SqlRequest("select * from " + index), RequestOptions.DEFAULT);
            assertEquals(
                "expected doc count : " + testDocCount + ", but get : " + recoveryResponse.getRowCount(),
                testDocCount,
                recoveryResponse.getRowCount()
            );
        }, 10, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void cleanNeededIndex() {
        try {
            if (highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                logger.info("deleting index {}", index);
            }
        } catch (IOException e) {
            logger.error("deleting index failed", e);
        }
    }
}
