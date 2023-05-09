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

package org.havenask.action.support.replication;

import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.NoShardAvailableActionException;
import org.havenask.action.UnavailableShardsException;
import org.havenask.action.admin.indices.flush.FlushRequest;
import org.havenask.action.admin.indices.flush.FlushResponse;
import org.havenask.action.admin.indices.flush.TransportFlushAction;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.ActionTestUtils;
import org.havenask.action.support.DefaultShardOperationFailedException;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.action.support.broadcast.BroadcastRequest;
import org.havenask.action.support.broadcast.BroadcastResponse;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.collect.Tuple;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.network.NetworkService;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.PageCacheRecycler;
import org.havenask.common.util.concurrent.ConcurrentCollections;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.indices.breaker.NoneCircuitBreakerService;
import org.havenask.rest.RestStatus;
import org.havenask.tasks.Task;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;
import org.havenask.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.havenask.action.support.replication.ClusterStateCreationUtils.state;
import static org.havenask.action.support.replication.ClusterStateCreationUtils.stateWithAssignedPrimariesAndOneReplica;
import static org.havenask.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.havenask.test.ClusterServiceUtils.createClusterService;
import static org.havenask.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BroadcastReplicationTests extends HavenaskTestCase {

    private static ThreadPool threadPool;
    private static CircuitBreakerService circuitBreakerService;
    private ClusterService clusterService;
    private TransportService transportService;
    private TestBroadcastReplicationAction broadcastReplicationAction;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("BroadcastReplicationTests");
        circuitBreakerService = new NoneCircuitBreakerService();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockNioTransport transport = new MockNioTransport(Settings.EMPTY, Version.CURRENT,
            threadPool, new NetworkService(Collections.emptyList()), PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()), circuitBreakerService);
        clusterService = createClusterService(threadPool);
        transportService = new TransportService(clusterService.getSettings(), transport, threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        broadcastReplicationAction = new TestBroadcastReplicationAction(clusterService, transportService,
                new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)), null);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(clusterService, transportService);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testNotStartedPrimary() throws InterruptedException, ExecutionException {
        final String index = "test";
        setState(clusterService, state(index, randomBoolean(),
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED, ShardRoutingState.UNASSIGNED));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        PlainActionFuture<BroadcastResponse> response = PlainActionFuture.newFuture();
        broadcastReplicationAction.execute(new DummyBroadcastRequest(index), response);
        for (Tuple<ShardId, ActionListener<ReplicationResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            if (randomBoolean()) {
                shardRequests.v2().onFailure(new NoShardAvailableActionException(shardRequests.v1()));
            } else {
                shardRequests.v2().onFailure(new UnavailableShardsException(shardRequests.v1(), "test exception"));
            }
        }
        response.get();
        logger.info("total shards: {}, ", response.get().getTotalShards());
        // we expect no failures here because UnavailableShardsException does not count as failed
        assertBroadcastResponse(2, 0, 0, response.get(), null);
    }

    public void testStartedPrimary() throws InterruptedException, ExecutionException {
        final String index = "test";
        setState(clusterService, state(index, randomBoolean(),
                ShardRoutingState.STARTED));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        PlainActionFuture<BroadcastResponse> response = PlainActionFuture.newFuture();
        broadcastReplicationAction.execute(new DummyBroadcastRequest(index), response);
        for (Tuple<ShardId, ActionListener<ReplicationResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            ReplicationResponse replicationResponse = new ReplicationResponse();
            replicationResponse.setShardInfo(new ReplicationResponse.ShardInfo(1, 1));
            shardRequests.v2().onResponse(replicationResponse);
        }
        logger.info("total shards: {}, ", response.get().getTotalShards());
        assertBroadcastResponse(1, 1, 0, response.get(), null);
    }

    public void testResultCombine() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        int numShards = 1 + randomInt(3);
        setState(clusterService, stateWithAssignedPrimariesAndOneReplica(index, numShards));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        PlainActionFuture<BroadcastResponse> response = PlainActionFuture.newFuture();
        broadcastReplicationAction.execute(new DummyBroadcastRequest().indices(index), response);
        int succeeded = 0;
        int failed = 0;
        for (Tuple<ShardId, ActionListener<ReplicationResponse>> shardRequests : broadcastReplicationAction.capturedShardRequests) {
            if (randomBoolean()) {
                ReplicationResponse.ShardInfo.Failure[] failures = new ReplicationResponse.ShardInfo.Failure[0];
                int shardsSucceeded = randomInt(1) + 1;
                succeeded += shardsSucceeded;
                ReplicationResponse replicationResponse = new ReplicationResponse();
                if (shardsSucceeded == 1 && randomBoolean()) {
                    //sometimes add failure (no failure means shard unavailable)
                    failures = new ReplicationResponse.ShardInfo.Failure[1];
                    failures[0] = new ReplicationResponse.ShardInfo.Failure(shardRequests.v1(), null,
                        new Exception("pretend shard failed"), RestStatus.GATEWAY_TIMEOUT, false);
                    failed++;
                }
                replicationResponse.setShardInfo(new ReplicationResponse.ShardInfo(2, shardsSucceeded, failures));
                shardRequests.v2().onResponse(replicationResponse);
            } else {
                // sometimes fail
                failed += 2;
                // just add a general exception and see if failed shards will be incremented by 2
                shardRequests.v2().onFailure(new Exception("pretend shard failed"));
            }
        }
        assertBroadcastResponse(2 * numShards, succeeded, failed, response.get(), Exception.class);
    }

    public void testNoShards() throws InterruptedException, ExecutionException, IOException {
        setState(clusterService, stateWithNoShard());
        logger.debug("--> using initial state:\n{}", clusterService.state());
        BroadcastResponse response = executeAndAssertImmediateResponse(broadcastReplicationAction, new DummyBroadcastRequest());
        assertBroadcastResponse(0, 0, 0, response, null);
    }

    public void testShardsList() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState clusterState = state(index, randomBoolean(),
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED, ShardRoutingState.UNASSIGNED);
        logger.debug("--> using initial state:\n{}", clusterService.state());
        List<ShardId> shards = broadcastReplicationAction.shards(new DummyBroadcastRequest().indices(shardId.getIndexName()), clusterState);
        assertThat(shards.size(), equalTo(1));
        assertThat(shards.get(0), equalTo(shardId));
    }

    private class TestBroadcastReplicationAction extends TransportBroadcastReplicationAction<DummyBroadcastRequest, BroadcastResponse,
            BasicReplicationRequest, ReplicationResponse> {
        protected final Set<Tuple<ShardId, ActionListener<ReplicationResponse>>> capturedShardRequests =
            ConcurrentCollections.newConcurrentSet();

        TestBroadcastReplicationAction(ClusterService clusterService, TransportService transportService,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                TransportReplicationAction<BasicReplicationRequest, BasicReplicationRequest, ReplicationResponse> action) {
            super("internal:test-broadcast-replication-action", DummyBroadcastRequest::new, clusterService, transportService,
                    actionFilters, indexNameExpressionResolver, action);
        }

        @Override
        protected ReplicationResponse newShardResponse() {
            return new ReplicationResponse();
        }

        @Override
        protected BasicReplicationRequest newShardRequest(DummyBroadcastRequest request, ShardId shardId) {
            return new BasicReplicationRequest(shardId);
        }

        @Override
        protected BroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies,
                                                List<DefaultShardOperationFailedException> shardFailures) {
            return new BroadcastResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
        }

        @Override
        protected void shardExecute(Task task, DummyBroadcastRequest request, ShardId shardId,
                                    ActionListener<ReplicationResponse> shardActionListener) {
            capturedShardRequests.add(new Tuple<>(shardId, shardActionListener));
        }
    }

    public FlushResponse assertImmediateResponse(String index, TransportFlushAction flushAction) {
        Date beginDate = new Date();
        FlushResponse flushResponse = ActionTestUtils.executeBlocking(flushAction, new FlushRequest(index));
        Date endDate = new Date();
        long maxTime = 500;
        assertThat("this should not take longer than " + maxTime + " ms. The request hangs somewhere",
            endDate.getTime() - beginDate.getTime(), lessThanOrEqualTo(maxTime));
        return flushResponse;
    }

    public BroadcastResponse executeAndAssertImmediateResponse(
            TransportBroadcastReplicationAction<DummyBroadcastRequest, BroadcastResponse, ?, ?> broadcastAction,
            DummyBroadcastRequest request) {
        PlainActionFuture<BroadcastResponse> response = PlainActionFuture.newFuture();
        broadcastAction.execute(request, response);
        return response.actionGet("5s");
    }

    private void assertBroadcastResponse(int total, int successful, int failed, BroadcastResponse response, Class<?> exceptionClass) {
        assertThat(response.getSuccessfulShards(), equalTo(successful));
        assertThat(response.getTotalShards(), equalTo(total));
        assertThat(response.getFailedShards(), equalTo(failed));
        for (int i = 0; i < failed; i++) {
            assertThat(response.getShardFailures()[0].getCause().getCause(), instanceOf(exceptionClass));
        }
    }

    public static class DummyBroadcastRequest extends BroadcastRequest<DummyBroadcastRequest> {
        DummyBroadcastRequest(String... indices) {
            super(indices);
        }

        DummyBroadcastRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
