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

package org.havenask.index.reindex;

import org.apache.lucene.util.SetOnce;
import org.havenask.HavenaskSecurityException;
import org.havenask.HavenaskStatusException;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionResponse;
import org.havenask.action.admin.cluster.node.info.NodeInfo;
import org.havenask.action.search.SearchAction;
import org.havenask.action.support.ActionFilter;
import org.havenask.action.support.ActionFilterChain;
import org.havenask.action.support.WriteRequest.RefreshPolicy;
import org.havenask.client.Client;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.network.NetworkModule;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.http.HttpInfo;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.RepositoriesService;
import org.havenask.rest.RestHeaderDefinition;
import org.havenask.rest.RestStatus;
import org.havenask.script.ScriptService;
import org.havenask.tasks.Task;
import org.havenask.test.HavenaskSingleNodeTestCase;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.Netty4Plugin;
import org.havenask.watcher.ResourceWatcherService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.havenask.index.reindex.ReindexTestCase.matcher;
import static org.hamcrest.Matchers.containsString;

public class ReindexFromRemoteWithAuthTests extends HavenaskSingleNodeTestCase {
    private TransportAddress address;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(
            Netty4Plugin.class,
            ReindexFromRemoteWithAuthTests.TestPlugin.class,
            ReindexPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings());
        // Whitelist reindexing from the http host we're going to use
        settings.put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "127.0.0.1:*");
        settings.put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME);
        return settings.build();
    }

    @Before
    public void setupSourceIndex() {
        client().prepareIndex("source", "test").setSource("test", "test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
    }

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        address = nodeInfo.getInfo(HttpInfo.class).getAddress().publishAddress();
    }

    /**
     * Build a {@link RemoteInfo}, defaulting values that we don't care about in this test to values that don't hurt anything.
     */
    private RemoteInfo newRemoteInfo(String username, String password, Map<String, String> headers) {
        return new RemoteInfo("http", address.getAddress(), address.getPort(), null,
            new BytesArray("{\"match_all\":{}}"), username, password, headers,
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT, RemoteInfo.DEFAULT_CONNECT_TIMEOUT);
    }

    public void testReindexFromRemoteWithAuthentication() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source").destination("dest")
                .setRemoteInfo(newRemoteInfo("Aladdin", "open sesame", emptyMap()));
        assertThat(request.get(), matcher().created(1));
    }

    public void testReindexSendsHeaders() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source").destination("dest")
                .setRemoteInfo(newRemoteInfo(null, null, singletonMap(TestFilter.EXAMPLE_HEADER, "doesn't matter")));
        HavenaskStatusException e = expectThrows(HavenaskStatusException.class, () -> request.get());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertThat(e.getMessage(), containsString("Hurray! Sent the header!"));
    }

    public void testReindexWithoutAuthenticationWhenRequired() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source").destination("dest")
                .setRemoteInfo(newRemoteInfo(null, null, emptyMap()));
        HavenaskStatusException e = expectThrows(HavenaskStatusException.class, () -> request.get());
        assertEquals(RestStatus.UNAUTHORIZED, e.status());
        assertThat(e.getMessage(), containsString("\"reason\":\"Authentication required\""));
        assertThat(e.getMessage(), containsString("\"WWW-Authenticate\":\"Basic realm=auth-realm\""));
    }

    public void testReindexWithBadAuthentication() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source").destination("dest")
                .setRemoteInfo(newRemoteInfo("junk", "auth", emptyMap()));
        HavenaskStatusException e = expectThrows(HavenaskStatusException.class, () -> request.get());
        assertThat(e.getMessage(), containsString("\"reason\":\"Bad Authorization\""));
    }

    /**
     * Plugin that demands authentication.
     */
    public static class TestPlugin extends Plugin implements ActionPlugin {

        private final SetOnce<ReindexFromRemoteWithAuthTests.TestFilter> testFilter = new SetOnce<>();

        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                                   IndexNameExpressionResolver expressionResolver,
                                                   Supplier<RepositoriesService> repositoriesServiceSupplier) {
            testFilter.set(new ReindexFromRemoteWithAuthTests.TestFilter(threadPool));
            return Collections.emptyList();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(testFilter.get());
        }

        @Override
        public Collection<RestHeaderDefinition> getRestHeaders() {
            return Arrays.asList(new RestHeaderDefinition(TestFilter.AUTHORIZATION_HEADER, false),
                new RestHeaderDefinition(TestFilter.EXAMPLE_HEADER, false));
        }
    }

    /**
     * ActionType filter that will reject the request if it isn't authenticated.
     */
    public static class TestFilter implements ActionFilter {
        /**
         * The authorization required. Corresponds to username="Aladdin" and password="open sesame". It is the example in
         * <a href="https://tools.ietf.org/html/rfc1945#section-11.1">HTTP/1.0's RFC</a>.
         */
        private static final String REQUIRED_AUTH = "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==";
        private static final String AUTHORIZATION_HEADER = "Authorization";
        private static final String EXAMPLE_HEADER = "Example-Header";
        private final ThreadContext context;

        public TestFilter(ThreadPool threadPool) {
            context = threadPool.getThreadContext();
        }

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action,
                Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
            if (false == action.equals(SearchAction.NAME)) {
                chain.proceed(task, action, request, listener);
                return;
            }
            if (context.getHeader(EXAMPLE_HEADER) != null) {
                throw new IllegalArgumentException("Hurray! Sent the header!");
            }
            String auth = context.getHeader(AUTHORIZATION_HEADER);
            if (auth == null) {
                HavenaskSecurityException e = new HavenaskSecurityException("Authentication required",
                        RestStatus.UNAUTHORIZED);
                e.addHeader("WWW-Authenticate", "Basic realm=auth-realm");
                throw e;
            }
            if (false == REQUIRED_AUTH.equals(auth)) {
                throw new HavenaskSecurityException("Bad Authorization", RestStatus.FORBIDDEN);
            }
            chain.proceed(task, action, request, listener);
        }
    }
}
