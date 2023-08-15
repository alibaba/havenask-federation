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

import com.alibaba.fastjson.JSONObject;
import org.apache.http.util.EntityUtils;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.index.IndexRequest;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.engine.EngineSettings;

import java.util.concurrent.TimeUnit;

public class FlushIT extends AbstractHavenaskRestTestCase {
    public void testFlush() throws Exception {
        // create index with havenask engine and 1s refresh_interval
        String index = "flush_test";
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .put("refresh_interval", "1s")
                            .put("index.havenask.flush.max_doc_count", 5)
                            .build()
                    ).mapping(Map.of("properties", Map.of("foo", Map.of("type", "keyword")))),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        assertEquals(-1, getLocalCheckpoint(index));
        assertEquals(-1, getMaxSeqNo(index));

        // create 4 docs
        for (int i = 0; i <= 3; i++) {
            highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test " + i)), RequestOptions.DEFAULT);
        }

        assertEquals(-1, getLocalCheckpoint(index));
        assertEquals(3, getMaxSeqNo(index));

        // sleep 6s
        Thread.sleep(6000);

        // create 5 docs
        for (int i = 4; i <= 8; i++) {
            highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test " + i)), RequestOptions.DEFAULT);
        }

        assertEquals(-1, getLocalCheckpoint(index));
        assertEquals(8, getMaxSeqNo(index));

        // sleep 10s and add 1 doc, so fed meta data can trigger flush
        Thread.sleep(10000);
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test " + 9)), RequestOptions.DEFAULT);
        assertEquals(3, getLocalCheckpoint(index));

        for (int i = 10; i <= 20; i++) {
            highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test " + i)), RequestOptions.DEFAULT);
        }

        assertEquals(3, getLocalCheckpoint(index));
        assertEquals(20, getMaxSeqNo(index));

        Thread.sleep(10000);
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test " + 21)), RequestOptions.DEFAULT);
        assertEquals(8, getLocalCheckpoint(index));

        Thread.sleep(6000);
        for (int i = 22; i <= 26; i++) {
            highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test " + i)), RequestOptions.DEFAULT);
        }
        Thread.sleep(10000);
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test " + 27)), RequestOptions.DEFAULT);
        assertEquals(21, getLocalCheckpoint(index));
        assertEquals(27, getMaxSeqNo(index));

        // delete index and HEAD index
        assertEquals(true, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    // get index stats
    private JSONObject getIndexStats(String index) throws Exception {
        Response indexStatsResponse = highLevelClient().getLowLevelClient()
            .performRequest(new Request("GET", "/" + index + "/_stats?level=shards"));
        String indexStats = EntityUtils.toString(indexStatsResponse.getEntity());
        return JSONObject.parseObject(indexStats);
    }

    // get local_checkpoint in disk
    private long getLocalCheckpoint(String index) throws Exception {
        JSONObject indexStatsJson = getIndexStats(index);
        return indexStatsJson.getJSONObject("indices")
            .getJSONObject(index)
            .getJSONObject("shards")
            .getJSONArray("0")
            .getJSONObject(0)
            .getJSONObject("commit")
            .getJSONObject("user_data")
            .getLong("local_checkpoint");
    }

    // get max_seq_no in memory
    private long getMaxSeqNo(String index) throws Exception {
        JSONObject indexStatsJson = getIndexStats(index);
        return indexStatsJson.getJSONObject("indices")
            .getJSONObject(index)
            .getJSONObject("shards")
            .getJSONArray("0")
            .getJSONObject(0)
            .getJSONObject("seq_no")
            .getLong("max_seq_no");
    }
}
