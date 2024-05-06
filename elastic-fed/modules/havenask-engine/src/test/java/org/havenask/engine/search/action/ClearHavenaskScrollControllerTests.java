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

package org.havenask.engine.search.action;

import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.LatchedActionListener;
import org.havenask.action.search.ClearScrollRequest;
import org.havenask.action.search.ClearScrollResponse;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.test.HavenaskTestCase;
import org.havenask.transport.NodeNotConnectedException;
import org.havenask.transport.Transport;
import org.havenask.transport.TransportException;
import org.havenask.transport.TransportRequest;
import org.havenask.transport.TransportRequestOptions;
import org.havenask.transport.TransportResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ClearHavenaskScrollControllerTests extends HavenaskTestCase {
    public void testClearAll() throws InterruptedException {
        DiscoveryNode node1 = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node3 = new DiscoveryNode("node_3", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build();
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ClearScrollResponse> listener = new LatchedActionListener<>(new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse clearScrollResponse) {
                assertEquals(3, clearScrollResponse.getNumFreed());
                assertTrue(clearScrollResponse.isSucceeded());
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }, latch);
        List<DiscoveryNode> nodesInvoked = new CopyOnWriteArrayList<>();
        HavenaskSearchTransportService havenaskSearchTransportService = new HavenaskSearchTransportService(null) {
            @Override
            public void sendClearAllHavenaskScrollContexts(
                Transport.Connection connection,
                final ActionListener<TransportResponse> listener
            ) {
                nodesInvoked.add(connection.getNode());
                Thread t = new Thread(() -> listener.onResponse(new SearchHavenaskScrollFreeContextResponse(true)));
                t.start();
            }

            @Override
            public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new MockConnection(node);
            }
        };
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.scrollIds(Arrays.asList("_all"));
        ClearHavenaskScrollController controller = new ClearHavenaskScrollController(
            clearScrollRequest,
            listener,
            nodes,
            logger,
            havenaskSearchTransportService
        );
        controller.run();
        latch.await();
        assertEquals(3, nodesInvoked.size());
        Collections.sort(nodesInvoked, Comparator.comparing(DiscoveryNode::getId));
        assertEquals(nodesInvoked, Arrays.asList(node1, node2, node3));
    }

    public void testClearScrollIds() throws InterruptedException {
        DiscoveryNode node1 = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node3 = new DiscoveryNode("node_3", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build();
        AtomicInteger numFreed = new AtomicInteger(0);
        List<String> scrollIds = new ArrayList<>();
        scrollIds.add(TransportHavenaskSearchHelper.buildHavenaskScrollId(node1.getId(), "scroll_session_1"));
        scrollIds.add(TransportHavenaskSearchHelper.buildHavenaskScrollId(node2.getId(), "scroll_session_2"));
        scrollIds.add(TransportHavenaskSearchHelper.buildHavenaskScrollId(node3.getId(), "scroll_session_3"));
        scrollIds.add(TransportHavenaskSearchHelper.buildHavenaskScrollId(node3.getId(), "scroll_session_4"));
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ClearScrollResponse> listener = new LatchedActionListener<>(new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse clearScrollResponse) {
                assertEquals(numFreed.get(), clearScrollResponse.getNumFreed());
                assertTrue(clearScrollResponse.isSucceeded());
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }, latch);
        List<DiscoveryNode> nodesInvoked = new CopyOnWriteArrayList<>();
        HavenaskSearchTransportService havenaskSearchTransportService = new HavenaskSearchTransportService(null) {
            @Override
            public void sendFreeHavenaskScrollContext(
                Transport.Connection connection,
                List<String> scrollSessionId,
                final ActionListener<SearchHavenaskScrollFreeContextResponse> listener
            ) {
                nodesInvoked.add(connection.getNode());
                boolean freed = randomBoolean();
                if (freed) {
                    numFreed.incrementAndGet();
                }
                Thread t = new Thread(
                    () -> listener.onResponse(new HavenaskSearchTransportService.SearchHavenaskScrollFreeContextResponse(freed))
                );
                t.start();
            }

            @Override
            public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new MockConnection(node);
            }
        };
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.scrollIds(scrollIds);
        ClearHavenaskScrollController controller = new ClearHavenaskScrollController(
            clearScrollRequest,
            listener,
            nodes,
            logger,
            havenaskSearchTransportService
        );
        controller.run();
        latch.await();
        assertEquals(3, nodesInvoked.size());
        Collections.sort(nodesInvoked, Comparator.comparing(DiscoveryNode::getId));
        assertEquals(nodesInvoked, Arrays.asList(node1, node2, node3));
    }

    public void testClearScrollIdsWithFailure() throws InterruptedException {
        DiscoveryNode node1 = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node3 = new DiscoveryNode("node_3", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build();
        AtomicInteger numFreed = new AtomicInteger(0);
        AtomicInteger numFailures = new AtomicInteger(0);
        AtomicInteger numConnectionFailures = new AtomicInteger(0);
        List<String> scrollIds = new ArrayList<>();
        scrollIds.add(TransportHavenaskSearchHelper.buildHavenaskScrollId(node1.getId(), "scroll_session_1"));
        scrollIds.add(TransportHavenaskSearchHelper.buildHavenaskScrollId(node2.getId(), "scroll_session_2"));
        scrollIds.add(TransportHavenaskSearchHelper.buildHavenaskScrollId(node3.getId(), "scroll_session_3"));
        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<ClearScrollResponse> listener = new LatchedActionListener<>(new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse clearScrollResponse) {
                assertEquals(numFreed.get(), clearScrollResponse.getNumFreed());
                if (numFailures.get() > 0) {
                    assertFalse(clearScrollResponse.isSucceeded());
                } else {
                    assertTrue(clearScrollResponse.isSucceeded());
                }
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }, latch);
        List<DiscoveryNode> nodesInvoked = new CopyOnWriteArrayList<>();
        HavenaskSearchTransportService havenaskSearchTransportService = new HavenaskSearchTransportService(null) {
            @Override
            public void sendFreeHavenaskScrollContext(
                Transport.Connection connection,
                List<String> scrollSessionId,
                final ActionListener<SearchHavenaskScrollFreeContextResponse> listener
            ) {
                nodesInvoked.add(connection.getNode());
                boolean freed = randomBoolean();
                boolean fail = randomBoolean();
                Thread t = new Thread(() -> {
                    if (fail) {
                        numFailures.incrementAndGet();
                        listener.onFailure(new IllegalArgumentException("boom"));
                    } else {
                        if (freed) {
                            numFreed.incrementAndGet();
                        }
                        listener.onResponse(new HavenaskSearchTransportService.SearchHavenaskScrollFreeContextResponse(freed));
                    }
                });
                t.start();
            }

            @Override
            public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                if (randomBoolean()) {
                    numFailures.incrementAndGet();
                    numConnectionFailures.incrementAndGet();
                    throw new NodeNotConnectedException(node, "boom");
                }
                return new MockConnection(node);
            }
        };
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.scrollIds(scrollIds);
        ClearHavenaskScrollController controller = new ClearHavenaskScrollController(
            clearScrollRequest,
            listener,
            nodes,
            logger,
            havenaskSearchTransportService
        );
        controller.run();
        latch.await();
        assertEquals(3 - numConnectionFailures.get(), nodesInvoked.size());
    }

    public static final class MockConnection implements Transport.Connection {

        private final DiscoveryNode node;

        MockConnection(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public DiscoveryNode getNode() {
            return node;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }
}
