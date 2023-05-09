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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.cluster.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.Version;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateObserver;
import org.havenask.cluster.LocalNodeMasterListener;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.coordination.NoMasterBlockService;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.havenask.cluster.service.ClusterApplier.ClusterApplyListener;
import org.havenask.common.logging.Loggers;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.MockLogAppender;
import org.havenask.test.junit.annotations.TestLogging;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.havenask.test.ClusterServiceUtils.createNoOpNodeConnectionsService;
import static org.havenask.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ClusterApplierServiceTests extends HavenaskTestCase {

    private static ThreadPool threadPool;
    private TimedClusterApplierService clusterApplierService;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(ClusterApplierServiceTests.class.getName());
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterApplierService = createTimedClusterService(true);
    }

    @After
    public void tearDown() throws Exception {
        clusterApplierService.close();
        super.tearDown();
    }

    private TimedClusterApplierService createTimedClusterService(boolean makeMaster) {
        DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(),
            emptySet(), Version.CURRENT);
        TimedClusterApplierService timedClusterApplierService = new TimedClusterApplierService(Settings.builder().put("cluster.name",
            "ClusterApplierServiceTests").build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool);
        timedClusterApplierService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        timedClusterApplierService.setInitialState(ClusterState.builder(new ClusterName("ClusterApplierServiceTests"))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(makeMaster ? localNode.getId() : null))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build());
        timedClusterApplierService.start();
        return timedClusterApplierService;
    }

    @TestLogging(value = "org.havenask.cluster.service:TRACE", reason = "to ensure that we log cluster state events on TRACE level")
    public void testClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test1",
                        ClusterApplierService.class.getCanonicalName(),
                        Level.DEBUG,
                        "*processing [test1]: took [1s] no change in cluster state"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test2",
                        ClusterApplierService.class.getCanonicalName(),
                        Level.TRACE,
                        "*failed to execute cluster state applier in [2s]*"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3",
                ClusterApplierService.class.getCanonicalName(),
                Level.DEBUG,
                "*processing [test3]: took [0s] no change in cluster state*"));

        Logger clusterLogger = LogManager.getLogger(ClusterApplierService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
        try {
            clusterApplierService.currentTimeOverride = threadPool.relativeTimeInMillis();
            clusterApplierService.runOnApplierThread("test1",
                currentState -> clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(1).millis(),
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) { }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
            });
            clusterApplierService.runOnApplierThread("test2",
                currentState -> {
                    clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(2).millis();
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                },
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {
                        fail();
                    }

                    @Override
                    public void onFailure(String source, Exception e) { }
                });
            // Additional update task to make sure all previous logging made it to the loggerName
            clusterApplierService.runOnApplierThread("test3",
                currentState -> {},
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) { }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
            assertBusy(mockAppender::assertAllExpectationsMatched);
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
    }

    @TestLogging(value = "org.havenask.cluster.service:WARN", reason = "to ensure that we log cluster state events on WARN level")
    public void testLongClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                        "test1 shouldn't see because setting is too low",
                        ClusterApplierService.class.getCanonicalName(),
                        Level.WARN,
                        "*cluster state applier task [test1] took [*] which is above the warn threshold of *"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test2",
                        ClusterApplierService.class.getCanonicalName(),
                        Level.WARN,
                        "*cluster state applier task [test2] took [32s] which is above the warn threshold of [*]: " +
                            "[running task [test2]] took [*"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test4",
                        ClusterApplierService.class.getCanonicalName(),
                        Level.WARN,
                        "*cluster state applier task [test3] took [34s] which is above the warn threshold of [*]: " +
                            "[running task [test3]] took [*"));

        Logger clusterLogger = LogManager.getLogger(ClusterApplierService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
        try {
            final CountDownLatch latch = new CountDownLatch(4);
            final CountDownLatch processedFirstTask = new CountDownLatch(1);
            clusterApplierService.currentTimeOverride = threadPool.relativeTimeInMillis();
            clusterApplierService.runOnApplierThread("test1",
                currentState -> clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(1).millis(),
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {
                        latch.countDown();
                        processedFirstTask.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
            processedFirstTask.await();
            clusterApplierService.runOnApplierThread("test2",
                currentState -> {
                    clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(32).millis();
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                },
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {
                        fail();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        latch.countDown();
                    }
                });
            clusterApplierService.runOnApplierThread("test3",
                currentState -> clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(34).millis(),
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            clusterApplierService.runOnApplierThread("test4",
                currentState -> {},
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                });
            latch.await();
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }

    public void testLocalNodeMasterListenerCallbacks() {
        TimedClusterApplierService timedClusterApplierService = createTimedClusterService(false);

        AtomicBoolean isMaster = new AtomicBoolean();
        timedClusterApplierService.addLocalNodeMasterListener(new LocalNodeMasterListener() {
            @Override
            public void onMaster() {
                isMaster.set(true);
            }

            @Override
            public void offMaster() {
                isMaster.set(false);
            }
        });

        ClusterState state = timedClusterApplierService.state();
        DiscoveryNodes nodes = state.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isMaster.get(), is(true));

        nodes = state.nodes();
        nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(null);
        state = ClusterState.builder(state).blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES))
            .nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isMaster.get(), is(false));
        nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isMaster.get(), is(true));

        timedClusterApplierService.close();
    }

    public void testClusterStateApplierCantSampleClusterState() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                clusterApplierService.state();
                error.set(new AssertionError("successfully sampled state"));
            } catch (AssertionError e) {
                if (e.getMessage().contains("should not be called by a cluster state applier") == false) {
                    error.set(e);
                }
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState("test", () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    error.compareAndSet(null, e);
                }
            }
        );

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    public void testClusterStateApplierBubblesUpExceptionsInApplier() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        clusterApplierService.addStateApplier(event -> {
            throw new RuntimeException("dummy exception");
        });
        clusterApplierService.allowClusterStateApplicationFailure();

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState("test", () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                    fail("should not be called");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    assertTrue(error.compareAndSet(null, e));
                    latch.countDown();
                }
            }
        );

        latch.await();
        assertNotNull(error.get());
        assertThat(error.get().getMessage(), containsString("dummy exception"));
    }

    public void testClusterStateApplierBubblesUpExceptionsInSettingsApplier() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        clusterApplierService.clusterSettings.addSettingsUpdateConsumer(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
            v -> {});
        clusterApplierService.allowClusterStateApplicationFailure();

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState("test", () -> ClusterState.builder(clusterApplierService.state())
                .metadata(Metadata.builder(clusterApplierService.state().metadata())
                    .persistentSettings(
                        Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), false).build())
                    .build())
                .build(),
            new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                    fail("should not be called");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    assertTrue(error.compareAndSet(null, e));
                    latch.countDown();
                }
            }
        );

        latch.await();
        assertNotNull(error.get());
        assertThat(error.get().getMessage(), containsString("illegal value can't update"));
    }

    public void testClusterStateApplierSwallowsExceptionInListener() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addListener(event -> {
            assertTrue(applierCalled.compareAndSet(false, true));
            throw new RuntimeException("dummy exception");
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState("test", () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    error.compareAndSet(null, e);
                }
            }
        );

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    public void testClusterStateApplierCanCreateAnObserver() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                ClusterStateObserver observer = new ClusterStateObserver(event.state(),
                    clusterApplierService, null, logger, threadPool.getThreadContext());
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {

                    }

                    @Override
                    public void onClusterServiceClose() {

                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {

                    }
                });
            } catch (AssertionError e) {
                error.set(e);
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState("test", () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    error.compareAndSet(null, e);
                }
        });

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    public void testThreadContext() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            final Map<String, String> expectedHeaders = Collections.singletonMap("test", "test");
            final Map<String, List<String>> expectedResponseHeaders = Collections.singletonMap("testResponse",
                Collections.singletonList("testResponse"));
            threadPool.getThreadContext().putHeader(expectedHeaders);

            clusterApplierService.onNewClusterState("test", () -> {
                assertTrue(threadPool.getThreadContext().isSystemContext());
                assertEquals(Collections.emptyMap(), threadPool.getThreadContext().getHeaders());
                threadPool.getThreadContext().addResponseHeader("testResponse", "testResponse");
                assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                if (randomBoolean()) {
                    return ClusterState.builder(clusterApplierService.state()).build();
                } else {
                    throw new IllegalArgumentException("mock failure");
                }
            }, new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }
            });
        }

        latch.await();
    }

    static class TimedClusterApplierService extends ClusterApplierService {

        final ClusterSettings clusterSettings;
        volatile Long currentTimeOverride = null;
        boolean applicationMayFail;

        TimedClusterApplierService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
            super("test_node", settings, clusterSettings, threadPool);
            this.clusterSettings = clusterSettings;
        }

        @Override
        protected long currentTimeInMillis() {
            if (currentTimeOverride != null) {
                return currentTimeOverride;
            }
            return super.currentTimeInMillis();
        }

        @Override
        protected boolean applicationMayFail() {
            return this.applicationMayFail;
        }

        void allowClusterStateApplicationFailure() {
            this.applicationMayFail = true;
        }
    }

}
