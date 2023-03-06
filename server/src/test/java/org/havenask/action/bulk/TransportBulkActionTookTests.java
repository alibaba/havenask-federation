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

package org.havenask.action.bulk;

import org.apache.lucene.util.Constants;
import org.havenask.action.ActionType;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionResponse;
import org.havenask.action.IndicesRequest;
import org.havenask.action.admin.indices.create.CreateIndexResponse;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.AutoCreateIndex;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Strings;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.AtomicArray;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.IndexNotFoundException;
import org.havenask.rest.action.document.RestBulkAction;
import org.havenask.index.IndexingPressure;
import org.havenask.indices.SystemIndices;
import org.havenask.tasks.Task;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;
import org.havenask.test.transport.CapturingTransport;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.bulk.BulkResponse;
import org.havenask.action.bulk.TransportBulkAction;
import org.havenask.action.bulk.TransportShardBulkAction;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static java.util.Collections.emptyMap;
import static org.havenask.test.ClusterServiceUtils.createClusterService;
import static org.havenask.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class TransportBulkActionTookTests extends HavenaskTestCase {

    private static ThreadPool threadPool;
    private ClusterService clusterService;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportBulkActionTookTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DiscoveryNode discoveryNode = new DiscoveryNode("node", HavenaskTestCase.buildNewFakeTransportAddress(), emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES, VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
        clusterService = createClusterService(threadPool, discoveryNode);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    private TransportBulkAction createAction(boolean controlled, AtomicLong expected) {
        CapturingTransport capturingTransport = new CapturingTransport();
        TransportService transportService = capturingTransport.createTransportService(clusterService.getSettings(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        IndexNameExpressionResolver resolver = new Resolver();
        ActionFilters actionFilters = new ActionFilters(new HashSet<>());

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                listener.onResponse((Response)new CreateIndexResponse(false, false, null));
            }
        };

        if (controlled) {

            return new TestTransportBulkAction(
                    threadPool,
                    transportService,
                    clusterService,
                    null,
                    client,
                    actionFilters,
                    resolver,
                    null,
                    expected::get) {

                @Override
                void executeBulk(
                        Task task,
                        BulkRequest bulkRequest,
                        long startTimeNanos,
                        ActionListener<BulkResponse> listener,
                        AtomicArray<BulkItemResponse> responses,
                        Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
                    expected.set(1000000);
                    super.executeBulk(task, bulkRequest, startTimeNanos, listener, responses, indicesThatCannotBeCreated);
                }
            };
        } else {
            return new TestTransportBulkAction(
                    threadPool,
                    transportService,
                    clusterService,
                    null,
                    client,
                    actionFilters,
                    resolver,
                    null,
                    System::nanoTime) {

                @Override
                void executeBulk(
                        Task task,
                        BulkRequest bulkRequest,
                        long startTimeNanos,
                        ActionListener<BulkResponse> listener,
                        AtomicArray<BulkItemResponse> responses,
                        Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
                    long elapsed = spinForAtLeastOneMillisecond();
                    expected.set(elapsed);
                    super.executeBulk(task, bulkRequest, startTimeNanos, listener, responses, indicesThatCannotBeCreated);
                }
            };
        }
    }

    // test unit conversion with a controlled clock
    public void testTookWithControlledClock() throws Exception {
        runTestTook(true);
    }

    // test took advances with System#nanoTime
    public void testTookWithRealClock() throws Exception {
        runTestTook(false);
    }

    private void runTestTook(boolean controlled) throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/havenask/action/bulk/simple-bulk.json");
        // translate Windows line endings (\r\n) to standard ones (\n)
        if (Constants.WINDOWS) {
            bulkAction = Strings.replace(bulkAction, "\r\n", "\n");
        }
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        AtomicLong expected = new AtomicLong();
        TransportBulkAction action = createAction(controlled, expected);
        action.doExecute(null, bulkRequest, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                if (controlled) {
                    assertThat(
                            bulkItemResponses.getTook().getMillis(),
                            equalTo(TimeUnit.MILLISECONDS.convert(expected.get(), TimeUnit.NANOSECONDS)));
                } else {
                    assertThat(
                            bulkItemResponses.getTook().getMillis(),
                            greaterThanOrEqualTo(TimeUnit.MILLISECONDS.convert(expected.get(), TimeUnit.NANOSECONDS)));
                }
            }

            @Override
            public void onFailure(Exception e) {

            }
        });
        //This test's JSON contains outdated references to types
        assertWarnings(RestBulkAction.TYPES_DEPRECATION_MESSAGE);
    }

    static class Resolver extends IndexNameExpressionResolver {
        Resolver() {
            super(new ThreadContext(Settings.EMPTY));
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }
    }

    static class TestTransportBulkAction extends TransportBulkAction {

        TestTransportBulkAction(
                ThreadPool threadPool,
                TransportService transportService,
                ClusterService clusterService,
                TransportShardBulkAction shardBulkAction,
                NodeClient client,
                ActionFilters actionFilters,
                IndexNameExpressionResolver indexNameExpressionResolver,
                AutoCreateIndex autoCreateIndex,
                LongSupplier relativeTimeProvider) {
            super(
                    threadPool,
                    transportService,
                    clusterService,
                    null,
                    shardBulkAction,
                    client,
                    actionFilters,
                    indexNameExpressionResolver,
                    autoCreateIndex,
                    new IndexingPressure(Settings.EMPTY),
                    new SystemIndices(emptyMap()),
                    relativeTimeProvider);
        }

        @Override
        boolean needToCheck() {
            return randomBoolean();
        }

        @Override
        boolean shouldAutoCreate(String index, ClusterState state) {
            return randomBoolean();
        }

    }
}
