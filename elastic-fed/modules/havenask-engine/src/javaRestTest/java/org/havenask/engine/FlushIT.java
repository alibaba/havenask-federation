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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class FlushIT extends AbstractHavenaskRestTestCase {

    private static final long recordInterval = 1000;
    private static final long waitDeleteTime = 0;
    private static final long waitFlushTime = 10000;

    // static logger
    private static final Logger logger = LogManager.getLogger(FlushIT.class);

    // claim a class contain index name and max_doc_count
    private static class IndexAndMaxDocCount {
        private String name;
        private long maxDocCount;

        IndexAndMaxDocCount(String name, long maxDocCount) {
            this.name = name;
            this.maxDocCount = maxDocCount;
        }

        public String getName() {
            return name;
        }

        public long getMaxDocCount() {
            return maxDocCount;
        }
    }

    // IndexAndMaxDocCount array
    static IndexAndMaxDocCount[] indices = new IndexAndMaxDocCount[] {
        new IndexAndMaxDocCount("flush_test", 5),
        new IndexAndMaxDocCount("multi_flush_test", 2), };

    @AfterClass
    public static void cleanNeededIndex() {
        try {
            for (IndexAndMaxDocCount index : indices) {
                if (highLevelClient().indices().exists(new GetIndexRequest(index.name), RequestOptions.DEFAULT)) {
                    highLevelClient().indices().delete(new DeleteIndexRequest(index.name), RequestOptions.DEFAULT);
                    logger.info("delete index {}", index);
                }
            }
        } catch (IOException e) {
            logger.error("delete index failed", e);
        }
    }

    @Before
    public void createNeededIndex() throws Exception {
        for (IndexAndMaxDocCount index : indices) {
            if (!highLevelClient().indices().exists(new GetIndexRequest(index.name), RequestOptions.DEFAULT)) {
                try {
                    assertTrue(
                        highLevelClient().indices()
                            .create(
                                new CreateIndexRequest(index.name).settings(
                                    Settings.builder()
                                        .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                                        .put("refresh_interval", "200ms")
                                        .put("index.havenask.flush.max_doc_count", index.maxDocCount)
                                        .build()
                                ).mapping(Map.of("properties", Map.of("foo", Map.of("type", "keyword")))),
                                RequestOptions.DEFAULT
                            )
                            .isAcknowledged()
                    );
                } catch (Exception e) {
                    logger.error("create index {} failed", index, e);
                }
            }
        }
    }

    public void testFlush() throws Exception {
        // Thread.sleep(waitDeleteTime);
        // create index with havenask engine and 1s refresh_interval

        String index = indices[0].name;

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        assertEquals(-1, getLocalCheckpoint(index));
        assertEquals(-1, getMaxSeqNo(index));

        // create 4 docs and all docs willed flushed
        for (int i = 0; i <= 3; i++) {
            highLevelClient().index(
                new IndexRequest(index).source(Map.of("foo", "flush test " + i), XContentType.JSON),
                RequestOptions.DEFAULT
            );
        }

        assertEquals(-1, getLocalCheckpoint(index));
        assertEquals(3, getMaxSeqNo(index));

        // sleep 6s and create 2 docs, doc0 - doc4 will be flushed
        Thread.sleep(recordInterval);
        for (int i = 4; i <= 5; i++) {
            highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test " + i)), RequestOptions.DEFAULT);
        }

        // wait for 10s for flush and create a doc to trigger flush
        Thread.sleep(waitFlushTime);
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "flush test 6")), RequestOptions.DEFAULT);

        // checkpoint will be 3 or 4, in most case it will be 3
        if (getLocalCheckpoint(index) == 3) {
            // print log
            logger.info("common case: checkpoint is 3");
        } else if (getLocalCheckpoint(index) == 4) {
            // print log
            logger.info("corner case: checkpoint is 4");
        } else {
            fail("checkpoint should be 3 or 4, but get " + getLocalCheckpoint(index));
        }
    }

    public void testMultiFlush() throws Exception {
        // Thread.sleep(waitDeleteTime);
        // create index with havenask engine, 1s refresh_interval and 1 max_doc_count
        String index = indices[1].name;

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        // check before write
        assertEquals(-1, getLocalCheckpoint(index));
        assertEquals(-1, getMaxSeqNo(index));

        // create 1 doc
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "multi flush test 1")), RequestOptions.DEFAULT);
        assertEquals(0, getMaxSeqNo(index));
        Thread.sleep(recordInterval);

        // create 2 doc, doc 0 and doc 1 will be flushed
        for (int i = 1; i <= 2; i++) {
            highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "multi flush test " + i)), RequestOptions.DEFAULT);
        }
        // wait for 10s for flush and create a doc to trigger flush
        Thread.sleep(waitFlushTime);
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "multi flush test 3")), RequestOptions.DEFAULT);

        // checkpoint will be 0 or 1, in most case it will be 0
        if (getLocalCheckpoint(index) == 0) {
            // print log
            logger.info("common case: checkpoint is 0");
        } else if (getLocalCheckpoint(index) == 1) {
            // print log
            logger.info("corner case: checkpoint is 1");
        } else {
            fail("checkpoint should be 0 or 1, but get " + getLocalCheckpoint(index));
        }

        // check max_seq_no
        assertEquals(3, getMaxSeqNo(index));

        // wait for 6s and create 1 doc, doc 2 and doc 3 will be flushed
        Thread.sleep(recordInterval);
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "multi flush test 4")), RequestOptions.DEFAULT);
        // wait for 10s for flush and create a doc to trigger flush
        Thread.sleep(waitFlushTime);
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "multi flush test 5")), RequestOptions.DEFAULT);

        // checkpoint will be 2 or 3, in most case it will be 2
        if (getLocalCheckpoint(index) == 2) {
            // print log
            logger.info("common case: checkpoint is 2");
        } else if (getLocalCheckpoint(index) == 3) {
            // print log
            logger.info("corner case: checkpoint is 3");
        } else {
            fail("checkpoint should be 2 or 3, but get " + getLocalCheckpoint(index));
        }

        // create many docs
        for (int i = 6; i <= 16; i++) {
            highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "multi flush test " + i)), RequestOptions.DEFAULT);
        }

        // wait for 6s and create 2 doc, doc 16 will be flushed, doc 17 maybe flushed
        Thread.sleep(recordInterval);
        for (int i = 17; i <= 18; i++) {
            highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "multi flush test " + i)), RequestOptions.DEFAULT);
        }
        // wait for 15s for flush and create a doc to trigger flush
        Thread.sleep(waitFlushTime);
        highLevelClient().index(new IndexRequest(index).source(Map.of("foo", "multi flush test 19")), RequestOptions.DEFAULT);

        // checkpoint will be 15, 16 or 17, in most case it will be 15 or 16
        if (getLocalCheckpoint(index) == 15) {
            // print log
            logger.info("common case: checkpoint is 15");
        } else if (getLocalCheckpoint(index) == 16) {
            // print log
            logger.info("common case: checkpoint is 16");
        } else if (getLocalCheckpoint(index) == 17) {
            // print log
            logger.info("corner case: checkpoint is 17");
        } else {
            fail("checkpoint should be 15, 16 or 17, but get " + getLocalCheckpoint(index));
        }
        // check max_seq_no
        assertEquals(19, getMaxSeqNo(index));
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
