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
import org.havenask.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class PeerRecoveryIT extends AbstractHavenaskRestTestCase {
    // static logger
    private static final Logger logger = LogManager.getLogger(PeerRecoveryIT.class);
    private static Set<String> peerRecoveryITIndices = new HashSet<>();

    @AfterClass
    public static void cleanIndices() {
        try {
            for (String index : peerRecoveryITIndices) {
                if (highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                    highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                    logger.info("clean index {}", index);
                }
            }
        } catch (IOException e) {
            logger.error("clean index failed", e);
        }
    }

    public void testTwoShardPeerRecovery() throws Exception {
        assumeTrue("number_of_nodes less then 2, Skip func: testTwoShardPeerRecovery()", clusterIsMultiNodes());

        String index = "two_shard_peer_recovery_test";
        peerRecoveryITIndices.add(index);

        int loopCount = 5;
        int querySize = 250;
        String primaryPreference = "primary";
        String replicaPreference = "replication";

        // create index
        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put("number_of_replicas", 1)
            .put(EngineSettings.HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.getKey(), 2)
            .build();

        java.util.Map<String, ?> map = Map.of("properties", Map.of("foo", Map.of("type", "keyword")));
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        int docValue = 0;
        int docNum = 0;
        for (int i = 0; i < loopCount; i++) {
            // 减少replica，写入doc
            assertTrue(setReplicaNum(index, 0));

            for (int j = 0; j < 50; j++) {
                putDoc(index, Map.of("foo", "value" + docValue));
                docValue++;
            }
            docNum += 50;
            int currentDocNum = docNum;

            // 增加replica，触发peer recovery
            assertTrue(setReplicaNum(index, 1));
            waitIndexGreen(index);

            assertBusy(() -> {
                SearchResponse primarySearchResponse = getSearchResponseWithPreference(index, querySize, primaryPreference);
                SearchResponse replicaSearchResponse = getSearchResponseWithPreference(index, querySize, replicaPreference);

                logger.info("primarySearchResponse: " + primarySearchResponse.getHits().getHits().length);
                logger.info("replicaSearchResponse: " + replicaSearchResponse.getHits().getHits().length);
                assertEquals(currentDocNum, primarySearchResponse.getHits().getHits().length);
                assertEquals(currentDocNum, replicaSearchResponse.getHits().getHits().length);
            }, 10, TimeUnit.SECONDS);

            compareResponsesHits(
                getSearchResponseWithPreference(index, querySize, primaryPreference),
                getSearchResponseWithPreference(index, querySize, replicaPreference)
            );
        }
        deleteAndHeadIndex(index);
    }

    public void testKillSearcherThenPeerRecovery() throws Exception {
        assumeTrue("number_of_nodes less then 2, Skip func: testTwoShardPeerRecovery()", clusterIsMultiNodes());

        String index = "kill_searcher_then_peer_recovery_test";
        peerRecoveryITIndices.add(index);

        int loopCount = 5;
        int querySize = 250;
        String primaryPreference = "primary";
        String replicaPreference = "replication";

        String dataNodeName = null;
        Response catNodesResponse = highLevelClient().getLowLevelClient()
            .performRequest(new Request("GET", "/_cat" + "/nodes?format=json"));
        String catNodesStr = EntityUtils.toString(catNodesResponse.getEntity());
        CatNodes catNodes = CatNodes.parse(catNodesStr);
        for (int i = 0; i < catNodes.getNodes().length; i++) {
            CatNodes.NodeInfo nodeInfo = catNodes.getNodes()[i];
            if (nodeInfo.nodeRole.contains("d")) {
                dataNodeName = nodeInfo.name;
                break;
            }
        }
        if (dataNodeName == null) {
            throw new RuntimeException("no data node found");
        }
        final String stopSearcherNodeName = dataNodeName;

        // create index
        ClusterHealthResponse chResponse = highLevelClient().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put("number_of_shards", chResponse.getNumberOfDataNodes())
            .put("number_of_replicas", 1)
            .put(EngineSettings.HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.getKey(), 2)
            .build();

        java.util.Map<String, ?> map = Map.of("properties", Map.of("foo", Map.of("type", "keyword")));
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        int docNum = 0;
        for (int i = 0; i < loopCount; i++) {
            // wait cluster health turns to be yellow
            assertBusy(() -> {
                // stop searcher
                Request request = new Request(
                    "POST",
                    String.format(Locale.ROOT, "/_havenask/stop?role=searcher&node=%s", stopSearcherNodeName)
                );
                Response response = highLevelClient().getLowLevelClient().performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
                ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                    .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
                logger.info("peer recovery now, cluster health is {}", clusterHealthResponse.getStatus());
                assertEquals(ClusterHealthStatus.YELLOW, clusterHealthResponse.getStatus());
            }, 2, TimeUnit.MINUTES);

            for (int j = 0; j < 50; j++) {
                putDoc(index, Map.of("foo", "value" + i));
            }
            docNum += 50;
            int curDocNum = docNum;

            // wait cluster health turns to be green
            waitIndexGreen(index);

            assertBusy(() -> {
                SearchResponse primarySearchResponse = getSearchResponseWithPreference(index, querySize, primaryPreference);
                SearchResponse replicaSearchResponse = getSearchResponseWithPreference(index, querySize, replicaPreference);

                logger.info("primarySearchResponse: " + primarySearchResponse.getHits().getHits().length);
                logger.info("replicaSearchResponse: " + replicaSearchResponse.getHits().getHits().length);
                assertEquals(curDocNum, primarySearchResponse.getHits().getHits().length);
                assertEquals(curDocNum, replicaSearchResponse.getHits().getHits().length);
                compareResponsesHits(primarySearchResponse, replicaSearchResponse);
            });
        }

        deleteAndHeadIndex(index);
    }

    private void compareResponsesHits(SearchResponse response1, SearchResponse response2) {
        Set<java.util.Map<String, Object>> response1DocSet = new HashSet<>();
        Set<java.util.Map<String, Object>> response2DocSet = new HashSet<>();
        for (int j = 0; j < response1.getHits().getHits().length; j++) {
            response1DocSet.add(response1.getHits().getHits()[j].getSourceAsMap());
            response2DocSet.add(response2.getHits().getHits()[j].getSourceAsMap());
        }
        assertTrue(response1DocSet.containsAll(response2DocSet));
    }

    private boolean setReplicaNum(String index, int replicaNum) throws Exception {
        UpdateSettingsRequest request = new UpdateSettingsRequest(index);
        Settings settings = Settings.builder().put("index.number_of_replicas", replicaNum).build();
        request.settings(settings);
        return highLevelClient().indices().putSettings(request, RequestOptions.DEFAULT).isAcknowledged();
    }

    private SearchResponse getSearchResponseWithPreference(String index, int querySize, String preference) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.preference(preference);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        searchSourceBuilder.size(querySize);

        searchRequest.source(searchSourceBuilder);
        return highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
    }
}
