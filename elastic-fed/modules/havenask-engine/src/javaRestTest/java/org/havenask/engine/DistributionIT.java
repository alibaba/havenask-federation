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

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.get.GetRequest;
import org.havenask.action.get.GetResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DistributionIT extends AbstractHavenaskRestTestCase {
    private static final Logger logger = LogManager.getLogger(DistributionIT.class);
    private static Set<String> distributionITIndices = new HashSet<>();

    @AfterClass
    public static void cleanIndices() {
        try {
            for (String index : distributionITIndices) {
                if (highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                    highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                    logger.info("clean index {}", index);
                }
            }
        } catch (IOException e) {
            logger.error("clean index failed", e);
        }
    }

    public void testMultiNodeShardRerouteMethod() throws Exception {
        assumeTrue("number_of_nodes less then 2, Skip func: testTwoShardPeerRecovery()", clusterIsMultiNodes());

        String index = "multi_node_shard_reroute_test";
        distributionITIndices.add(index);

        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        distributionITIndices.add(index);
        createTestIndex(
            index,
            Settings.builder()
                .put("index.number_of_shards", clusterHealthResponse.getNumberOfDataNodes() - 1)
                .put("index.number_of_replicas", 0)
                .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                .build(),
            Map.of("properties", Map.of("content", Map.of("type", "keyword")))
        );
        waitIndexGreen(index);

        int randomDocNum = randomIntBetween(
            clusterHealthResponse.getNumberOfDataNodes() + 1,
            clusterHealthResponse.getNumberOfDataNodes() * 2 + 1
        );
        // put some doc
        for (int i = 0; i < randomDocNum; i++) {
            highLevelClient().index(
                new IndexRequest(index).id(String.valueOf(i)).source(Map.of("content", "test" + i), XContentType.JSON),
                RequestOptions.DEFAULT
            );
        }

        // get doc
        for (int i = 0; i < randomDocNum; i++) {
            String contentVal = "test" + i;
            String id = String.valueOf(i);
            assertBusy(() -> {
                GetResponse getResponse = highLevelClient().get(new GetRequest(index, id), RequestOptions.DEFAULT);
                assertEquals(true, getResponse.isExists());
                assertEquals(contentVal, getResponse.getSourceAsMap().get("content"));
            }, 10, TimeUnit.SECONDS);
        }

        // test reroute func
        for (int i = 0; i < clusterHealthResponse.getNumberOfDataNodes(); i++) {
            // 统计数据节点
            Response catNodesResponse = highLevelClient().getLowLevelClient()
                .performRequest(new Request("GET", "/_cat" + "/nodes?format=json"));
            String catNodesStr = EntityUtils.toString(catNodesResponse.getEntity());
            CatNodes catNodes = CatNodes.parse(catNodesStr);
            Set<String> dataNodesSet = new HashSet<>();
            for (int j = 0; j < catNodes.getNodes().length; j++) {
                if (catNodes.getNodes()[j].nodeRole.contains("d")) {
                    dataNodesSet.add(catNodes.getNodes()[j].name);
                }
            }

            // 统计索引所在的数据节点
            Response catShardssResponse = highLevelClient().getLowLevelClient()
                .performRequest(new Request("GET", String.format(Locale.ROOT, "/_cat" + "/shards/%s?format=json", index)));
            String catShardsStr = EntityUtils.toString(catShardssResponse.getEntity());
            CatShards catShards = CatShards.parse(catShardsStr);

            Set<String> indexDataNodesSet = new HashSet<>();
            for (int j = 0; j < catShards.getShards().length; j++) {
                indexDataNodesSet.add(catShards.getShards()[j].node);
            }

            // 找到没有shard的节点
            Set<String> res = new HashSet<>(dataNodesSet);
            res.removeAll(indexDataNodesSet);
            assertEquals(1, res.size());
            String EmptyNode = (String) res.stream().toArray()[0];

            // reroute
            CatShards.ShardInfo moveSourceShard = catShards.getShards()[randomIntBetween(0, catShards.getShards().length - 1)];

            Request request = new Request("POST", "/_cluster/reroute");
            String jsonString = String.format(
                Locale.ROOT,
                "{"
                    + "\"commands\": ["
                    + "    {"
                    + "      \"move\":{"
                    + "        \"index\" : \"%s\", \"shard\" : %s,"
                    + "        \"from_node\": \"%s\", \"to_node\": \"%s\""
                    + "      }"
                    + "    }"
                    + "  ]"
                    + "}",
                index,
                moveSourceShard.shard,
                moveSourceShard.node,
                EmptyNode
            );
            request.setJsonEntity(jsonString);

            Response rerouteResponse = highLevelClient().getLowLevelClient().performRequest(request);
            assertEquals(200, rerouteResponse.getStatusLine().getStatusCode());
            assertBusy(() -> {
                Response rerouteCatShardssResponse = highLevelClient().getLowLevelClient()
                    .performRequest(new Request("GET", String.format(Locale.ROOT, "/_cat" + "/shards/%s?format=json", index)));
                String rerouteCatShardsStr = EntityUtils.toString(rerouteCatShardssResponse.getEntity());
                CatShards rerouteCatShards = CatShards.parse(rerouteCatShardsStr);
                for (int j = 0; j < rerouteCatShards.getShards().length; j++) {
                    if (!rerouteCatShards.getShards()[j].state.equals("STARTED")) {
                        logger.info(
                            String.format(
                                Locale.ROOT,
                                "%s, node:%s, shard:%s, state is %s, not STARTED",
                                rerouteCatShards.getShards()[j].index,
                                rerouteCatShards.getShards()[j].node,
                                rerouteCatShards.getShards()[j].shard,
                                rerouteCatShards.getShards()[j].state
                            )
                        );
                    }
                    assertEquals("STARTED", rerouteCatShards.getShards()[j].state);
                }
            }, 2, TimeUnit.MINUTES);

            // get doc
            for (int j = 0; j < randomDocNum; j++) {
                String contentVal = "test" + j;
                String id = String.valueOf(j);
                assertBusy(() -> {
                    GetResponse getResponse = highLevelClient().get(new GetRequest(index, id), RequestOptions.DEFAULT);
                    assertEquals(true, getResponse.isExists());
                    assertEquals(contentVal, getResponse.getSourceAsMap().get("content"));
                }, 10, TimeUnit.SECONDS);
            }
        }

        deleteAndHeadIndex(index);
    }
}
