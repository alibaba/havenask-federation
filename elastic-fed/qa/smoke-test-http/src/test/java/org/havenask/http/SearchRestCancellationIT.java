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

package org.havenask.http;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.SetOnce;
import org.havenask.action.admin.cluster.node.info.NodeInfo;
import org.havenask.action.admin.cluster.node.info.NodesInfoResponse;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.havenask.action.bulk.BulkRequestBuilder;
import org.havenask.action.search.MultiSearchAction;
import org.havenask.action.search.MultiSearchRequest;
import org.havenask.action.search.SearchAction;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.support.WriteRequest;
import org.havenask.client.Cancellable;
import org.havenask.client.Request;
import org.havenask.client.Response;
import org.havenask.client.ResponseListener;
import org.havenask.common.Strings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.PluginsService;
import org.havenask.script.MockScriptPlugin;
import org.havenask.script.Script;
import org.havenask.script.ScriptType;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.lookup.LeafFieldsLookup;
import org.havenask.tasks.CancellableTask;
import org.havenask.tasks.Task;
import org.havenask.tasks.TaskId;
import org.havenask.tasks.TaskInfo;
import org.havenask.tasks.TaskManager;
import org.havenask.transport.TransportService;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.havenask.index.query.QueryBuilders.scriptQuery;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class SearchRestCancellationIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(ScriptedBlockPlugin.class);
        plugins.addAll(super.nodePlugins());
        return plugins;
    }

    public void testAutomaticCancellationDuringQueryPhase() throws Exception {
        Request searchRequest = new Request("GET", "/test/_search");
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(scriptQuery(
            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())));
        searchRequest.setJsonEntity(Strings.toString(searchSource));
        verifyCancellationDuringQueryPhase(SearchAction.NAME, searchRequest);
    }

    public void testAutomaticCancellationMultiSearchDuringQueryPhase() throws Exception {
        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(new SearchRequest("test")
            .source(new SearchSourceBuilder().scriptField("test_field",
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))));
        Request restRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        restRequest.setEntity(new NByteArrayEntity(requestBody, createContentType(contentType)));
        verifyCancellationDuringQueryPhase(MultiSearchAction.NAME, restRequest);
    }

    void verifyCancellationDuringQueryPhase(String searchAction, Request searchRequest) throws Exception {
        Map<String, String> nodeIdToName = readNodesInfo();

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        Cancellable cancellable = getRestClient().performRequestAsync(searchRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                latch.countDown();
            }
        });

        awaitForBlock(plugins);
        cancellable.cancel();
        ensureSearchTaskIsCancelled(searchAction, nodeIdToName::get);

        disableBlocks(plugins);
        latch.await();
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    public void testAutomaticCancellationDuringFetchPhase() throws Exception {
        Request searchRequest = new Request("GET", "/test/_search");
        SearchSourceBuilder searchSource = new SearchSourceBuilder().scriptField("test_field",
            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()));
        searchRequest.setJsonEntity(Strings.toString(searchSource));
        verifyCancellationDuringFetchPhase(SearchAction.NAME, searchRequest);
    }

    public void testAutomaticCancellationMultiSearchDuringFetchPhase() throws Exception {
        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(new SearchRequest("test")
            .source(new SearchSourceBuilder().scriptField("test_field",
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))));
        Request restRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        restRequest.setEntity(new NByteArrayEntity(requestBody, createContentType(contentType)));
        verifyCancellationDuringFetchPhase(MultiSearchAction.NAME, restRequest);
    }

    void verifyCancellationDuringFetchPhase(String searchAction, Request searchRequest) throws Exception {
        Map<String, String> nodeIdToName = readNodesInfo();

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        Cancellable cancellable = getRestClient().performRequestAsync(searchRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                latch.countDown();
            }
        });

        awaitForBlock(plugins);
        cancellable.cancel();
        ensureSearchTaskIsCancelled(searchAction, nodeIdToName::get);

        disableBlocks(plugins);
        latch.await();
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    private static Map<String, String> readNodesInfo() {
        Map<String, String> nodeIdToName = new HashMap<>();
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        for (NodeInfo node : nodesInfoResponse.getNodes()) {
            nodeIdToName.put(node.getNode().getId(), node.getNode().getName());
        }
        return nodeIdToName;
    }

    private static void ensureSearchTaskIsCancelled(String transportAction, Function<String, String> nodeIdToName) throws Exception {
        SetOnce<TaskInfo> searchTask = new SetOnce<>();
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().get();
        for (TaskInfo task : listTasksResponse.getTasks()) {
            if (task.getAction().equals(transportAction)) {
                searchTask.set(task);
            }
        }
        assertNotNull(searchTask.get());
        TaskId taskId = searchTask.get().getTaskId();
        String nodeName = nodeIdToName.apply(taskId.getNodeId());
        assertBusy(() -> {
            TaskManager taskManager = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager();
            Task task = taskManager.getTask(taskId.getId());
            assertThat(task, instanceOf(CancellableTask.class));
            assertTrue(((CancellableTask)task).isCancelled());
        });
    }

    private static void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test", "_doc", Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    private static List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        int numberOfShards = getNumShards("test").numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                numberOfBlockedPlugins += plugin.hits.get();
            }
            logger.info("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        }, 10, TimeUnit.SECONDS);
    }

    private static void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        void reset() {
            hits.set(0);
        }

        void disableBlock() {
            shouldBlock.set(false);
        }

        void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(SearchRestCancellationIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    waitUntil(() -> shouldBlock.get() == false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

    private static ContentType createContentType(final XContentType xContentType) {
        return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
    }
}
