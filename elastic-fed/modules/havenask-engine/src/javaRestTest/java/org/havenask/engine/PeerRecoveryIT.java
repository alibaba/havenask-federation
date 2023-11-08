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
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class PeerRecoveryIT extends AbstractHavenaskRestTestCase {
    // static logger
    private static final Logger logger = LogManager.getLogger(PeerRecoveryIT.class);
    private static final String[] PeerRecoveryITIndices = { "two_shard_peer_recovery_test", "kill_searcher_then_peer_recovery_test" };
    private static final int TEST_TWO_SHARD_PEER_RECOVERY_INDEX_POS = 0;
    private static final int TEST_KILL_SEARCHER_THEN_PEER_RECOVERY_INDEX_POS = 1;

    @AfterClass
    public static void cleanIndices() {
        try {
            for (String index : PeerRecoveryITIndices) {
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
        String index = PeerRecoveryITIndices[TEST_TWO_SHARD_PEER_RECOVERY_INDEX_POS];
        int loopCount = 5;
        int querySize = 250;
        int waitIndexReduce = 10000;  // 10s
        String primaryPreference = "primary";
        String replicaPreference = "replication";

        // create index
        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put("number_of_replicas", 1)
            .put("index.havenask.flush.max_doc_count", 2)
            .build();

        java.util.Map<String, ?> map = Map.of("properties", Map.of("foo", Map.of("type", "keyword")));
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        int docValue = 0;
        for (int i = 0; i < loopCount; i++) {
            // 减少replica，写入文档
            assertTrue(setReplicaNum(index, 0));

            for (int j = 0; j < 50; j++) {
                putDoc(index, Map.of("foo", "value" + docValue));
                docValue++;
            }

            // TODO:防止减少replication时相关索引信息还没有清空就进行peer recovery流程，加入等待时间，待优化
            Thread.sleep(waitIndexReduce);

            // 增加replica，触发peer recovery
            assertTrue(setReplicaNum(index, 1));
            waitIndexGreen(index);

            assertBusy(() -> {
                SearchResponse primarySearchResponse = getSearchResponseWithPreference(index, querySize, primaryPreference);
                SearchResponse replicaSearchResponse = getSearchResponseWithPreference(index, querySize, replicaPreference);

                logger.info("primarySearchResponse: " + primarySearchResponse.getHits().getTotalHits());
                logger.info("replicaSearchResponse: " + replicaSearchResponse.getHits().getTotalHits());
                assertEquals(primarySearchResponse.getHits().getTotalHits(), replicaSearchResponse.getHits().getTotalHits());
            }, 10, TimeUnit.SECONDS);

            compareResponsesHits(
                getSearchResponseWithPreference(index, querySize, primaryPreference),
                getSearchResponseWithPreference(index, querySize, replicaPreference)
            );
        }
        deleteAndHeadIndex(index);
    }

    public void testKillSearcherThenPeerRecovery() throws Exception {
        String index = PeerRecoveryITIndices[TEST_KILL_SEARCHER_THEN_PEER_RECOVERY_INDEX_POS];

        int loopCount = 5;
        int querySize = 250;
        String primaryPreference = "primary";
        String replicaPreference = "replication";

        // create index
        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put("number_of_replicas", 1)
            // .put("refresh_interval", "200ms")
            .put("index.havenask.flush.max_doc_count", 2)
            .build();

        java.util.Map<String, ?> map = Map.of("properties", Map.of("foo", Map.of("type", "keyword")));
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        for (int i = 0; i < loopCount; i++) {
            // stop searcher
            logger.info("prepare peer recovery, kill searcher!");
            Request request = new Request("POST", "/_havenask/stop?role=searcher");
            Response response = highLevelClient().getLowLevelClient().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());

            // wait cluster health turns to be yellow
            assertBusy(() -> {
                ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                    .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
                logger.info("peer recovery now, cluster health is {}", clusterHealthResponse.getStatus());
                assertEquals(ClusterHealthStatus.YELLOW, clusterHealthResponse.getStatus());
            }, 2, TimeUnit.MINUTES);

            for (int j = 0; j < 50; j++) {
                putDoc(index, Map.of("foo", "value" + i));
            }

            // wait cluster health turns to be green
            waitIndexGreen(index);

            assertBusy(() -> {
                SearchResponse primarySearchResponse = getSearchResponseWithPreference(index, querySize, primaryPreference);
                SearchResponse replicaSearchResponse = getSearchResponseWithPreference(index, querySize, replicaPreference);

                logger.info("primarySearchResponse: " + primarySearchResponse.getHits().getTotalHits());
                logger.info("replicaSearchResponse: " + replicaSearchResponse.getHits().getTotalHits());
                assertEquals(primarySearchResponse.getHits().getTotalHits(), replicaSearchResponse.getHits().getTotalHits());
            });

            compareResponsesHits(
                getSearchResponseWithPreference(index, querySize, primaryPreference),
                getSearchResponseWithPreference(index, querySize, replicaPreference)
            );
        }

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
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
