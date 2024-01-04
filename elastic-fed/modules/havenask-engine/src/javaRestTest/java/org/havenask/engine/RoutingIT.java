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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.client.RequestOptions;
import org.havenask.client.ha.SqlResponse;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.engine.HashAlgorithm;
import org.havenask.engine.util.RangeUtil;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RoutingIT extends AbstractHavenaskRestTestCase {
    private static final Logger logger = LogManager.getLogger(RoutingIT.class);
    private static Set<String> routingITIndices = new HashSet<>();
    private static String HAVENASK_HASH_FIELD = "index.havenask.hash.field";

    @AfterClass
    public static void cleanIndices() {
        try {
            for (String index : routingITIndices) {
                if (highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                    highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                    logger.info("clean index {}", index);
                }
            }
        } catch (IOException e) {
            logger.error("clean index failed", e);
        }
    }

    public void testDefaultShardRouting() throws Exception {
        String index = "default_shard_routing_test";
        routingITIndices.add(index);

        // create index
        ClusterHealthResponse chResponse = highLevelClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);

        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put(NUMBER_OF_SHARDS, shardsNum)
            .put(NUMBER_OF_REPLICAS, replicasNum)
            .build();

        java.util.Map<String, ?> map = Map.of("properties", Map.of("seq", Map.of("type", "integer"), "content", Map.of("type", "keyword")));
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        // put docs
        int dataNum = randomIntBetween(20, 40);
        for (int i = 0; i < dataNum; i++) {
            putDoc(index, String.valueOf(i), Map.of("seq", i, "content", "欢迎使用 " + i));
        }

        // shard routing test
        for (int i = 0; i < dataNum; i++) {
            int shardId = RangeUtil.getRangeIdxByHashId(
                0,
                HashAlgorithm.HASH_SIZE - 1,
                shardsNum,
                (int) HashAlgorithm.getHashId(String.valueOf(i))
            );
            String summarySql = String.format(
                Locale.ROOT,
                "select /*+ SCAN_ATTR(partitionIds='%d')*/ _id, seq, content from `%s_summary_` where _id='%s'",
                shardId,
                index,
                i
            );
            int curNum = i;
            assertBusy(() -> {
                SqlResponse summarySqlResponse = getSqlResponse(summarySql);
                assertEquals(1, summarySqlResponse.getRowCount());
                assertEquals(String.valueOf(curNum), summarySqlResponse.getSqlResult().getData()[0][0]);
                assertEquals(curNum, summarySqlResponse.getSqlResult().getData()[0][1]);
                assertEquals("欢迎使用 " + curNum, summarySqlResponse.getSqlResult().getData()[0][2]);
            }, 1, TimeUnit.SECONDS);
        }

        // delete index
        deleteAndHeadIndex(index);
    }

    public void testHashFieldShardRouting() throws Exception {
        String index = "hash_field_shard_routing_test";
        routingITIndices.add(index);

        // create index
        ClusterHealthResponse chResponse = highLevelClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);

        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put(NUMBER_OF_SHARDS, shardsNum)
            .put(NUMBER_OF_REPLICAS, replicasNum)
            .put(HAVENASK_HASH_FIELD, "content")
            .build();

        java.util.Map<String, ?> map = Map.of("properties", Map.of("seq", Map.of("type", "integer"), "content", Map.of("type", "keyword")));
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        // put doc
        int dataNum = randomIntBetween(20, 40);
        for (int i = 0; i < dataNum; i++) {
            putDoc(index, String.valueOf(i), Map.of("seq", i, "content", "欢迎使用 " + i));
        }

        // test shard routing
        for (int i = 0; i < dataNum; i++) {
            int shardId = RangeUtil.getRangeIdxByHashId(
                0,
                HashAlgorithm.HASH_SIZE - 1,
                shardsNum,
                (int) HashAlgorithm.getHashId("欢迎使用 " + i)
            );
            String summarySql = String.format(
                Locale.ROOT,
                "select /*+ SCAN_ATTR(partitionIds='%d')*/ _id, seq, content from `%s_summary_` where _id='%s'",
                shardId,
                index,
                i
            );
            int curNum = i;
            assertBusy(() -> {
                SqlResponse summarySqlResponse = getSqlResponse(summarySql);
                assertEquals(1, summarySqlResponse.getRowCount());
                assertEquals(String.valueOf(curNum), summarySqlResponse.getSqlResult().getData()[0][0]);
                assertEquals(curNum, summarySqlResponse.getSqlResult().getData()[0][1]);
                assertEquals("欢迎使用 " + curNum, summarySqlResponse.getSqlResult().getData()[0][2]);
            }, 1, TimeUnit.SECONDS);
        }

        // delete index
        deleteAndHeadIndex(index);

    }
}
