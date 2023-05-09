/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.search;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.lucene.search.TotalHits;
import org.havenask.HavenaskException;
import org.havenask.Version;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.havenask.action.admin.cluster.state.ClusterStateAction;
import org.havenask.action.admin.cluster.state.ClusterStateRequest;
import org.havenask.action.admin.cluster.state.ClusterStateResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.search.SearchAction;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.search.SearchScrollRequest;
import org.havenask.action.search.ShardSearchFailure;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.client.ResponseException;
import org.havenask.client.RestClient;
import org.havenask.client.RestHighLevelClient;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.Strings;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.internal.InternalSearchResponse;
import org.havenask.test.rest.HavenaskRestTestCase;
import org.havenask.test.transport.MockTransportService;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;

public class CrossClusterSearchUnavailableClusterIT extends HavenaskRestTestCase {

    private static RestHighLevelClient restHighLevelClient;

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void initHighLevelClient() throws IOException {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static MockTransportService startTransport(
            final String id,
            final List<DiscoveryNode> knownNodes,
            final Version version,
            final ThreadPool threadPool) {
        boolean success = false;
        final Settings s = Settings.builder().put("node.name", id).build();
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(s);
        MockTransportService newService = MockTransportService.createNewService(s, version, threadPool, null);
        try {
            newService.registerRequestHandler(ClusterSearchShardsAction.NAME, ThreadPool.Names.SAME, ClusterSearchShardsRequest::new,
                (request, channel, task) -> {
                        channel.sendResponse(new ClusterSearchShardsResponse(new ClusterSearchShardsGroup[0],
                                knownNodes.toArray(new DiscoveryNode[0]), Collections.emptyMap()));
                    });
            newService.registerRequestHandler(SearchAction.NAME, ThreadPool.Names.SAME, SearchRequest::new,
                (request, channel, task) -> {
                    InternalSearchResponse response = new InternalSearchResponse(new SearchHits(new SearchHit[0],
                        new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN), InternalAggregations.EMPTY, null, null, false, null, 1);
                    SearchResponse searchResponse = new SearchResponse(response, null, 1, 1, 0, 100, ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY);
                    channel.sendResponse(searchResponse);
                });
            newService.registerRequestHandler(ClusterStateAction.NAME, ThreadPool.Names.SAME, ClusterStateRequest::new,
                (request, channel, task) -> {
                        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
                        for (DiscoveryNode node : knownNodes) {
                            builder.add(node);
                        }
                        ClusterState build = ClusterState.builder(clusterName).nodes(builder.build()).build();
                        channel.sendResponse(new ClusterStateResponse(clusterName, build, false));
                    });
            newService.start();
            newService.acceptIncomingRequests();
            success = true;
            return newService;
        } finally {
            if (success == false) {
                newService.close();
            }
        }
    }

    public void testSearchSkipUnavailable() throws IOException {
        try (MockTransportService remoteTransport = startTransport("node0", new CopyOnWriteArrayList<>(), Version.CURRENT, threadPool)) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();

            updateRemoteClusterSettings(Collections.singletonMap("seeds", remoteNode.getAddress().toString()));

            for (int i = 0; i < 10; i++) {
                restHighLevelClient.index(
                        new IndexRequest("index").id(String.valueOf(i)).source("field", "value"), RequestOptions.DEFAULT);
            }
            Response refreshResponse = client().performRequest(new Request("POST", "/index/_refresh"));
            assertEquals(200, refreshResponse.getStatusLine().getStatusCode());

            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index"), RequestOptions.DEFAULT);
                assertSame(SearchResponse.Clusters.EMPTY, response.getClusters());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
            }
            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index", "remote1:index"), RequestOptions.DEFAULT);
                assertEquals(2, response.getClusters().getTotal());
                assertEquals(2, response.getClusters().getSuccessful());
                assertEquals(0, response.getClusters().getSkipped());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
            }
            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("remote1:index"), RequestOptions.DEFAULT);
                assertEquals(1, response.getClusters().getTotal());
                assertEquals(1, response.getClusters().getSuccessful());
                assertEquals(0, response.getClusters().getSkipped());
                assertEquals(0, response.getHits().getTotalHits().value);
            }

            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index", "remote1:index").scroll("1m"),
                        RequestOptions.DEFAULT);
                assertEquals(2, response.getClusters().getTotal());
                assertEquals(2, response.getClusters().getSuccessful());
                assertEquals(0, response.getClusters().getSkipped());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
                String scrollId = response.getScrollId();
                SearchResponse scrollResponse = restHighLevelClient.scroll(new SearchScrollRequest(scrollId), RequestOptions.DEFAULT);
                assertSame(SearchResponse.Clusters.EMPTY, scrollResponse.getClusters());
                assertEquals(10, scrollResponse.getHits().getTotalHits().value);
                assertEquals(0, scrollResponse.getHits().getHits().length);
            }

            remoteTransport.close();

            updateRemoteClusterSettings(Collections.singletonMap("skip_unavailable", true));

            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index", "remote1:index"), RequestOptions.DEFAULT);
                assertEquals(2, response.getClusters().getTotal());
                assertEquals(1, response.getClusters().getSuccessful());
                assertEquals(1, response.getClusters().getSkipped());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
            }
            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("remote1:index"), RequestOptions.DEFAULT);
                assertEquals(1, response.getClusters().getTotal());
                assertEquals(0, response.getClusters().getSuccessful());
                assertEquals(1, response.getClusters().getSkipped());
                assertEquals(0, response.getHits().getTotalHits().value);
            }

            {
                SearchResponse response = restHighLevelClient.search(new SearchRequest("index", "remote1:index").scroll("1m"),
                        RequestOptions.DEFAULT);
                assertEquals(2, response.getClusters().getTotal());
                assertEquals(1, response.getClusters().getSuccessful());
                assertEquals(1, response.getClusters().getSkipped());
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
                String scrollId = response.getScrollId();
                SearchResponse scrollResponse = restHighLevelClient.scroll(new SearchScrollRequest(scrollId), RequestOptions.DEFAULT);
                assertSame(SearchResponse.Clusters.EMPTY, scrollResponse.getClusters());
                assertEquals(10, scrollResponse.getHits().getTotalHits().value);
                assertEquals(0, scrollResponse.getHits().getHits().length);
            }

            updateRemoteClusterSettings(Collections.singletonMap("skip_unavailable", false));
            assertSearchConnectFailure();

            Map<String, Object> map = new HashMap<>();
            map.put("seeds", null);
            map.put("skip_unavailable", null);
            updateRemoteClusterSettings(map);
        }
    }

    public void testSkipUnavailableDependsOnSeeds() throws IOException {
        try (MockTransportService remoteTransport = startTransport("node0", new CopyOnWriteArrayList<>(), Version.CURRENT, threadPool)) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();

            {
                //check that skip_unavailable alone cannot be set
                Request request = new Request("PUT", "/_cluster/settings");
                request.setEntity(buildUpdateSettingsRequestBody(
                    Collections.singletonMap("skip_unavailable", randomBoolean())));
                ResponseException responseException = expectThrows(ResponseException.class,
                        () -> client().performRequest(request));
                assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
                assertThat(responseException.getMessage(),
                        containsString("Cannot configure setting [cluster.remote.remote1.skip_unavailable] if remote cluster is " +
                            "not enabled."));
            }

            Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put("seeds", remoteNode.getAddress().toString());
            settingsMap.put("skip_unavailable", randomBoolean());
            updateRemoteClusterSettings(settingsMap);

            {
                //check that seeds cannot be reset alone if skip_unavailable is set
                Request request = new Request("PUT", "/_cluster/settings");
                request.setEntity(buildUpdateSettingsRequestBody(Collections.singletonMap("seeds", null)));
                ResponseException responseException = expectThrows(ResponseException.class,
                        () -> client().performRequest(request));
                assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
                assertThat(responseException.getMessage(), containsString("Cannot configure setting " +
                    "[cluster.remote.remote1.skip_unavailable] if remote cluster is not enabled."));
            }

            if (randomBoolean()) {
                updateRemoteClusterSettings(Collections.singletonMap("skip_unavailable", null));
                updateRemoteClusterSettings(Collections.singletonMap("seeds", null));
            } else {
                Map<String, Object> nullMap = new HashMap<>();
                nullMap.put("seeds", null);
                nullMap.put("skip_unavailable", null);
                updateRemoteClusterSettings(nullMap);
            }
        }
    }

    private static void assertSearchConnectFailure() {
        {
            HavenaskException exception = expectThrows(HavenaskException.class,
                    () -> restHighLevelClient.search(new SearchRequest("index", "remote1:index"), RequestOptions.DEFAULT));
            HavenaskException rootCause = (HavenaskException)exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("connect_exception"));
        }
        {
            HavenaskException exception = expectThrows(HavenaskException.class,
                    () -> restHighLevelClient.search(new SearchRequest("remote1:index"), RequestOptions.DEFAULT));
            HavenaskException rootCause = (HavenaskException)exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("connect_exception"));
        }
        {
            HavenaskException exception = expectThrows(HavenaskException.class,
                    () -> restHighLevelClient.search(new SearchRequest("remote1:index").scroll("1m"), RequestOptions.DEFAULT));
            HavenaskException rootCause = (HavenaskException)exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("connect_exception"));
        }
    }



    private static void updateRemoteClusterSettings(Map<String, Object> settings) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setEntity(buildUpdateSettingsRequestBody(settings));
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private static HttpEntity buildUpdateSettingsRequestBody(Map<String, Object> settings) throws IOException {
        String requestBody;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject("persistent");
                {
                    builder.startObject("cluster.remote.remote1");
                    {
                        for (Map.Entry<String, Object> entry : settings.entrySet()) {
                            builder.field(entry.getKey(), entry.getValue());
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            requestBody = Strings.toString(builder);
        }
        return new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, Collections.emptyList());
        }
    }
}
