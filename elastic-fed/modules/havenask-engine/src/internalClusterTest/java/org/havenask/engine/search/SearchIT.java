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

package org.havenask.engine.search;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.ArpcThreadLeakFilter;
import org.havenask.HttpThreadLeakFilter;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.query.KnnQueryBuilder;
import org.havenask.plugins.Plugin;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@ThreadLeakFilters(filters = { HttpThreadLeakFilter.class, ArpcThreadLeakFilter.class })
@HavenaskIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, scope = HavenaskIntegTestCase.Scope.TEST)
public class SearchIT extends HavenaskIntegTestCase {
    private static final Logger logger = LogManager.getLogger(SearchIT.class);
    private static String relativeBinPath = "/distribution/src/bin";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String usrDir = System.getProperty("user.dir");
        String pattern = "/havenask-federation/elastic-fed";
        String binDir = "";

        int lastIndex = usrDir.lastIndexOf(pattern);
        if (lastIndex != -1) {
            String projectDir = usrDir.substring(0, lastIndex + pattern.length());
            binDir = projectDir + relativeBinPath;
        }

        int searcherHttpPort = 39200 + nodeOrdinal;
        int searcherTcpPort = 39300 + nodeOrdinal;
        int searcherGrpcPort = 39400 + nodeOrdinal;
        int qrsHttpPort = 49200 + nodeOrdinal;
        int qrsTcpPort = 49300 + nodeOrdinal;

        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .putList("node.roles", Arrays.asList("master", "data", "ingest"))
            .put("havenask.engine.enabled", true)
            .put("havenask.binfile.path", binDir)
            .put(NativeProcessControlService.HAVENASK_SEARCHER_HTTP_PORT_SETTING.getKey(), String.valueOf(searcherHttpPort))
            .put(NativeProcessControlService.HAVENASK_SEARCHER_TCP_PORT_SETTING.getKey(), String.valueOf(searcherTcpPort))
            .put(NativeProcessControlService.HAVENASK_SEARCHER_GRPC_PORT_SETTING.getKey(), String.valueOf(searcherGrpcPort))
            .put(NativeProcessControlService.HAVENASK_QRS_HTTP_PORT_SETTING.getKey(), String.valueOf(qrsHttpPort))
            .put(NativeProcessControlService.HAVENASK_QRS_TCP_PORT_SETTING.getKey(), String.valueOf(qrsTcpPort))
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testSearch() throws Exception {
        String index = "keyword_test";
        prepareKeywordIndex(index);

        // PUT docs
        int dataNum = randomIntBetween(100, 200);
        for (int i = 0; i < dataNum; i++) {
            client().index(new IndexRequest(index).id(String.valueOf(i)).source(Map.of("content", "keyword " + i), XContentType.JSON));
        }

        // get data with _search API
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(dataNum);
        searchRequest.source(searchSourceBuilder);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(index).setSource(searchSourceBuilder).get();
            assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);

            logger.info("searchResponse.getHits().getTotalHits().value: " + searchResponse.getHits().getTotalHits().value);
        }, 5, TimeUnit.SECONDS);
    }

    public void testKnnSearch() throws Exception {
        String index = "knn_search";
        prepareIndex(index);

        // put doc
        int dataNum = randomIntBetween(100, 200);
        for (int i = 0; i < dataNum; i++) {
            client().index(
                new IndexRequest(index).id(String.valueOf(i))
                    .source(Map.of("vector", new float[] { 0.1f + i, 0.1f + i }), XContentType.JSON)
            );
        }

        // search doc
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        KnnQueryBuilder knnQueryBuilder = new KnnQueryBuilder("vector", new float[] { 1.5f, 2.5f }, dataNum);
        searchSourceBuilder.query(knnQueryBuilder);
        searchSourceBuilder.size(dataNum);

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(index).setSource(searchSourceBuilder).get();
            assertEquals(dataNum, searchResponse.getHits().getTotalHits().value);

            logger.info("searchResponse.getHits().getTotalHits().value: " + searchResponse.getHits().getTotalHits().value);
        }, 5, TimeUnit.SECONDS);
    }

    private void prepareKeywordIndex(String index) throws Exception {
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"content\": {\n"
            + "      \"type\": \"keyword\""
            + "    }\n"
            + "  }\n"
            + "}";

        ClusterHealthResponse chResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);

        logger.info("shardsNum : " + shardsNum);
        logger.info("replicasNum : " + replicasNum);

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shardsNum)
                        .put("index.number_of_replicas", replicasNum)
                        .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                        .build()
                )
                .addMapping("_doc", mapping, XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 30, TimeUnit.SECONDS);
    }

    private void prepareIndex(String index) throws Exception {
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"vector\": {\n"
            + "      \"type\": \"vector\",\n"
            + "      \"dims\": 2\n"
            + "    }\n"
            + "  }\n"
            + "}";

        ClusterHealthResponse chResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
        int numberOfDataNodes = chResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);

        logger.info("shardsNum : " + shardsNum);
        logger.info("replicasNum : " + replicasNum);

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shardsNum)
                        .put("index.number_of_replicas", replicasNum)
                        .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                        .build()
                )
                .addMapping("_doc", mapping, XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 30, TimeUnit.SECONDS);
    }

    public void testSourceFilter() throws Exception {
        String index = "test2";
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"vector\": {\n"
            + "      \"type\": \"vector\",\n"
            + "      \"dims\": 2\n"
            + "    }\n"
            + "  }\n"
            + "}";

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                        .build()
                )
                .addMapping("_doc", mapping, XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 30, TimeUnit.SECONDS);

        String[] include1 = new String[] { "name", "key1" };
        String[] exclude1 = new String[] {};

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        KnnQueryBuilder knnQueryBuilder = new KnnQueryBuilder("vector", new float[] { 1.5f, 2.5f }, 10);
        searchSourceBuilder.query(knnQueryBuilder);
        searchSourceBuilder.fetchSource(include1, exclude1);
        SearchResponse searchResponse = client().prepareSearch(index).setSource(searchSourceBuilder).get();
        assertEquals(searchResponse.getHits().getTotalHits().value, 2);

        String[] include2 = new String[] {};
        String[] exclude2 = new String[] { "key1" };

        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        KnnQueryBuilder knnQueryBuilder2 = new KnnQueryBuilder("vector", new float[] { 1.5f, 2.5f }, 10);
        searchSourceBuilder2.query(knnQueryBuilder2);
        searchSourceBuilder2.fetchSource(include2, exclude2);
        SearchResponse searchResponse2 = client().prepareSearch(index).setSource(searchSourceBuilder2).get();
        assertEquals(searchResponse2.getHits().getTotalHits().value, 2);
    }
}
