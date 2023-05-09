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

package org.havenask.test.disruption;

import org.havenask.Version;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.cluster.coordination.DeterministicTaskQueue;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.collect.Tuple;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.node.Node;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.disruption.DisruptableMockTransport.ConnectionStatus;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.ConnectTransportException;
import org.havenask.transport.TransportChannel;
import org.havenask.transport.TransportException;
import org.havenask.transport.TransportRequest;
import org.havenask.transport.TransportRequestHandler;
import org.havenask.transport.TransportResponse;
import org.havenask.transport.TransportResponse.Empty;
import org.havenask.transport.TransportResponseHandler;
import org.havenask.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.havenask.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;

public class DisruptableMockTransportTests extends HavenaskTestCase {

    private DiscoveryNode node1;
    private DiscoveryNode node2;

    private TransportService service1;
    private TransportService service2;

    private DeterministicTaskQueue deterministicTaskQueue;

    private Set<Tuple<DiscoveryNode, DiscoveryNode>> disconnectedLinks;
    private Set<Tuple<DiscoveryNode, DiscoveryNode>> blackholedLinks;
    private Set<Tuple<DiscoveryNode, DiscoveryNode>> blackholedRequestLinks;

    private ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination) {
        Tuple<DiscoveryNode, DiscoveryNode> link = Tuple.tuple(sender, destination);
        if (disconnectedLinks.contains(link)) {
            assert blackholedLinks.contains(link) == false;
            assert blackholedRequestLinks.contains(link) == false;
            return ConnectionStatus.DISCONNECTED;
        }
        if (blackholedLinks.contains(link)) {
            assert blackholedRequestLinks.contains(link) == false;
            return ConnectionStatus.BLACK_HOLE;
        }
        if (blackholedRequestLinks.contains(link)) {
            return ConnectionStatus.BLACK_HOLE_REQUESTS_ONLY;
        }
        return ConnectionStatus.CONNECTED;
    }

    @Before
    public void initTransports() {
        node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);

        disconnectedLinks = new HashSet<>();
        blackholedLinks = new HashSet<>();
        blackholedRequestLinks = new HashSet<>();

        List<DisruptableMockTransport> transports = new ArrayList<>();

        deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "dummy").build(), random());

        final DisruptableMockTransport transport1 = new DisruptableMockTransport(node1, logger, deterministicTaskQueue) {
            @Override
            protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                return DisruptableMockTransportTests.this.getConnectionStatus(getLocalNode(), destination);
            }

            @Override
            protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                return transports.stream().filter(t -> t.getLocalNode().getAddress().equals(address)).findAny();
            }

            @Override
            protected void execute(Runnable runnable) {
                deterministicTaskQueue.scheduleNow(runnable);
            }
        };

        final DisruptableMockTransport transport2 = new DisruptableMockTransport(node2, logger, deterministicTaskQueue) {
            @Override
            protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                return DisruptableMockTransportTests.this.getConnectionStatus(getLocalNode(), destination);
            }

            @Override
            protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                return transports.stream().filter(t -> t.getLocalNode().getAddress().equals(address)).findAny();
            }

            @Override
            protected void execute(Runnable runnable) {
                deterministicTaskQueue.scheduleNow(runnable);
            }
        };

        transports.add(transport1);
        transports.add(transport2);

        service1 = transport1.createTransportService(Settings.EMPTY, deterministicTaskQueue.getThreadPool(),
            NOOP_TRANSPORT_INTERCEPTOR, a -> node1, null, Collections.emptySet());
        service2 = transport2.createTransportService(Settings.EMPTY, deterministicTaskQueue.getThreadPool(),
            NOOP_TRANSPORT_INTERCEPTOR, a -> node2, null, Collections.emptySet());

        service1.start();
        service2.start();

        final PlainActionFuture<Void> fut1 = new PlainActionFuture<>();
        service1.connectToNode(node2, fut1);
        final PlainActionFuture<Void> fut2 = new PlainActionFuture<>();
        service2.connectToNode(node1, fut2);
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(fut1.isDone());
        assertTrue(fut2.isDone());
    }

    private TransportRequestHandler<TransportRequest.Empty> requestHandlerShouldNotBeCalled() {
        return (request, channel, task) -> {
            throw new AssertionError("should not be called");
        };
    }

    private TransportRequestHandler<TransportRequest.Empty> requestHandlerRepliesNormally() {
        return (request, channel, task) -> {
            logger.debug("got a dummy request, replying normally...");
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        };
    }

    private TransportRequestHandler<TransportRequest.Empty> requestHandlerRepliesExceptionally(Exception e) {
        return (request, channel, task) -> {
            logger.debug("got a dummy request, replying exceptionally...");
            channel.sendResponse(e);
        };
    }

    private TransportRequestHandler<TransportRequest.Empty> requestHandlerCaptures(Consumer<TransportChannel> channelConsumer) {
        return (request, channel, task) -> {
            logger.debug("got a dummy request...");
            channelConsumer.accept(channel);
        };
    }

    private TransportResponseHandler<TransportResponse> responseHandlerShouldNotBeCalled() {
        return new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse read(StreamInput in) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleResponse(TransportResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                throw new AssertionError("should not be called");
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };
    }

    private TransportResponseHandler<TransportResponse> responseHandlerShouldBeCalledNormally(Runnable onCalled) {
        return new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse read(StreamInput in) {
                return Empty.INSTANCE;
            }

            @Override
            public void handleResponse(TransportResponse response) {
                onCalled.run();
            }

            @Override
            public void handleException(TransportException exp) {
                throw new AssertionError("should not be called");
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };
    }

    private TransportResponseHandler<TransportResponse> responseHandlerShouldBeCalledExceptionally(Consumer<TransportException> onCalled) {
        return new TransportResponseHandler<TransportResponse>() {
            @Override
            public TransportResponse read(StreamInput in) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleResponse(TransportResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                onCalled.accept(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };
    }

    private void registerRequestHandler(TransportService transportService, TransportRequestHandler<TransportRequest.Empty> handler) {
        transportService.registerRequestHandler("internal:dummy", ThreadPool.Names.GENERIC, TransportRequest.Empty::new, handler);
    }

    private void send(TransportService transportService, DiscoveryNode destinationNode,
                      TransportResponseHandler<TransportResponse> responseHandler) {
        transportService.sendRequest(destinationNode, "internal:dummy", TransportRequest.Empty.INSTANCE, responseHandler);
    }

    public void testSuccessfulResponse() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerRepliesNormally());
        AtomicBoolean responseHandlerCalled = new AtomicBoolean();
        send(service1, node2, responseHandlerShouldBeCalledNormally(() -> responseHandlerCalled.set(true)));
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(responseHandlerCalled.get());
    }

    public void testExceptionalResponse() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        Exception e = new Exception("dummy exception");
        registerRequestHandler(service2, requestHandlerRepliesExceptionally(e));
        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerException.get());
        assertNotNull(responseHandlerException.get().getCause());
        assertThat(responseHandlerException.get().getCause().getMessage(), containsString("dummy exception"));
    }

    public void testDisconnectedOnRequest() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerShouldNotBeCalled());
        disconnectedLinks.add(Tuple.tuple(node1, node2));
        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerException.get());
        assertThat(responseHandlerException.get().getMessage(), containsString("disconnected"));
    }

    public void testUnavailableOnRequest() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerShouldNotBeCalled());
        blackholedLinks.add(Tuple.tuple(node1, node2));
        send(service1, node2, responseHandlerShouldNotBeCalled());
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testUnavailableOnRequestOnly() {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        registerRequestHandler(service2, requestHandlerShouldNotBeCalled());
        blackholedRequestLinks.add(Tuple.tuple(node1, node2));
        send(service1, node2, responseHandlerShouldNotBeCalled());
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testDisconnectedOnSuccessfulResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerChannel.get());
        assertNull(responseHandlerException.get());

        disconnectedLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(TransportResponse.Empty.INSTANCE);
        deterministicTaskQueue.runAllTasks();
        assertThat(responseHandlerException.get(), instanceOf(ConnectTransportException.class));
    }

    public void testDisconnectedOnExceptionalResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        AtomicReference<TransportException> responseHandlerException = new AtomicReference<>();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(responseHandlerException::set));
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerChannel.get());
        assertNull(responseHandlerException.get());

        disconnectedLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(new Exception());
        deterministicTaskQueue.runAllTasks();
        assertThat(responseHandlerException.get(), instanceOf(ConnectTransportException.class));
    }

    public void testUnavailableOnSuccessfulResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        send(service1, node2, responseHandlerShouldNotBeCalled());
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerChannel.get());

        blackholedLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(TransportResponse.Empty.INSTANCE);
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testUnavailableOnExceptionalResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        send(service1, node2, responseHandlerShouldNotBeCalled());
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotNull(responseHandlerChannel.get());

        blackholedLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(new Exception());
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testUnavailableOnRequestOnlyReceivesSuccessfulResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        AtomicBoolean responseHandlerCalled = new AtomicBoolean();
        send(service1, node2, responseHandlerShouldBeCalledNormally(() -> responseHandlerCalled.set(true)));

        deterministicTaskQueue.runAllTasks();
        assertNotNull(responseHandlerChannel.get());
        assertFalse(responseHandlerCalled.get());

        blackholedRequestLinks.add(Tuple.tuple(node1, node2));
        blackholedRequestLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(TransportResponse.Empty.INSTANCE);

        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(responseHandlerCalled.get());
    }

    public void testUnavailableOnRequestOnlyReceivesExceptionalResponse() throws IOException {
        registerRequestHandler(service1, requestHandlerShouldNotBeCalled());
        AtomicReference<TransportChannel> responseHandlerChannel = new AtomicReference<>();
        registerRequestHandler(service2, requestHandlerCaptures(responseHandlerChannel::set));

        AtomicBoolean responseHandlerCalled = new AtomicBoolean();
        send(service1, node2, responseHandlerShouldBeCalledExceptionally(e -> responseHandlerCalled.set(true)));

        deterministicTaskQueue.runAllTasks();
        assertNotNull(responseHandlerChannel.get());
        assertFalse(responseHandlerCalled.get());

        blackholedRequestLinks.add(Tuple.tuple(node1, node2));
        blackholedRequestLinks.add(Tuple.tuple(node2, node1));
        responseHandlerChannel.get().sendResponse(new Exception());

        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(responseHandlerCalled.get());
    }

    public void testBrokenLinkFailsToConnect() {
        service1.disconnectFromNode(node2);

        disconnectedLinks.add(Tuple.tuple(node1, node2));
        assertThat(expectThrows(ConnectTransportException.class, () -> service1.connectToNode(node2)).getMessage(),
            endsWith("is [DISCONNECTED] not [CONNECTED]"));
        disconnectedLinks.clear();

        blackholedLinks.add(Tuple.tuple(node1, node2));
        assertThat(expectThrows(ConnectTransportException.class, () -> service1.connectToNode(node2)).getMessage(),
            endsWith("is [BLACK_HOLE] not [CONNECTED]"));
        blackholedLinks.clear();

        blackholedRequestLinks.add(Tuple.tuple(node1, node2));
        assertThat(expectThrows(ConnectTransportException.class, () -> service1.connectToNode(node2)).getMessage(),
            endsWith("is [BLACK_HOLE_REQUESTS_ONLY] not [CONNECTED]"));
        blackholedRequestLinks.clear();

        final DiscoveryNode node3 = new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(expectThrows(ConnectTransportException.class, () -> service1.connectToNode(node3)).getMessage(),
            endsWith("does not exist"));
    }
}
