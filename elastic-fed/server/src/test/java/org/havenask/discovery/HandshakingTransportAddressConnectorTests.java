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

package org.havenask.discovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.havenask.HavenaskException;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.Nullable;
import org.havenask.common.logging.Loggers;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.MockLogAppender;
import org.havenask.test.junit.annotations.TestLogging;
import org.havenask.test.transport.MockTransport;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.ConnectTransportException;
import org.havenask.transport.TransportException;
import org.havenask.transport.TransportRequest;
import org.havenask.transport.TransportService;
import org.havenask.transport.TransportService.HandshakeResponse;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.havenask.discovery.HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING;
import static org.havenask.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class HandshakingTransportAddressConnectorTests extends HavenaskTestCase {

    private DiscoveryNode remoteNode;
    private TransportAddress discoveryAddress;
    private TransportService transportService;
    private ThreadPool threadPool;
    private String remoteClusterName;
    private HandshakingTransportAddressConnector handshakingTransportAddressConnector;
    private DiscoveryNode localNode;

    private boolean dropHandshake;
    @Nullable // unless we want the full connection to fail
    private TransportException fullConnectionFailure;

    @Before
    public void startServices() {
        localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(CLUSTER_NAME_SETTING.getKey(), "local-cluster")
            .build();
        threadPool = new TestThreadPool("node", settings);

        remoteNode = null;
        discoveryAddress = null;
        remoteClusterName = null;
        dropHandshake = false;
        fullConnectionFailure = null;

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                assertThat(action, equalTo(TransportService.HANDSHAKE_ACTION_NAME));
                assertThat(discoveryAddress, notNullValue());
                assertThat(node.getAddress(), oneOf(discoveryAddress, remoteNode.getAddress()));
                if (dropHandshake == false) {
                    if (fullConnectionFailure != null && node.getAddress().equals(remoteNode.getAddress())) {
                        handleError(requestId, fullConnectionFailure);
                    } else {
                        handleResponse(requestId, new HandshakeResponse(remoteNode, new ClusterName(remoteClusterName), Version.CURRENT));
                    }
                }
            }
        };

        transportService = mockTransport.createTransportService(settings, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, address -> localNode, null, emptySet());

        transportService.start();
        transportService.acceptIncomingRequests();

        handshakingTransportAddressConnector = new HandshakingTransportAddressConnector(settings, transportService);
    }

    @After
    public void stopServices() {
        transportService.stop();
        terminate(threadPool);
    }

    public void testConnectsToMasterNode() throws InterruptedException {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final SetOnce<DiscoveryNode> receivedNode = new SetOnce<>();

        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        remoteClusterName = "local-cluster";
        discoveryAddress = getDiscoveryAddress();

        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, new ActionListener<DiscoveryNode>() {
            @Override
            public void onResponse(DiscoveryNode discoveryNode) {
                receivedNode.set(discoveryNode);
                completionLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
        assertEquals(remoteNode, receivedNode.get());
    }

    @TestLogging(reason="ensure logging happens", value="org.havenask.discovery.HandshakingTransportAddressConnector:INFO")
    public void testLogsFullConnectionFailureAfterSuccessfulHandshake() throws Exception {

        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        remoteClusterName = "local-cluster";
        discoveryAddress = buildNewFakeTransportAddress();

        fullConnectionFailure = new ConnectTransportException(remoteNode, "simulated", new HavenaskException("root cause"));

        FailureListener failureListener = new FailureListener();

        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "message",
                HandshakingTransportAddressConnector.class.getCanonicalName(),
                Level.WARN,
                "*completed handshake with [*] but followup connection failed*"));
        Logger targetLogger = LogManager.getLogger(HandshakingTransportAddressConnector.class);
        Loggers.addAppender(targetLogger, mockAppender);

        try {
            handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
            failureListener.assertFailure();
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(targetLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testDoesNotConnectToNonMasterNode() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        failureListener.assertFailure();
    }

    public void testDoesNotConnectToLocalNode() throws Exception {
        remoteNode = localNode;
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        failureListener.assertFailure();
    }

    public void testDoesNotConnectToDifferentCluster() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "another-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        failureListener.assertFailure();
    }

    public void testHandshakeTimesOut() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";
        dropHandshake = true;

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        Thread.sleep(PROBE_HANDSHAKE_TIMEOUT_SETTING.get(Settings.EMPTY).millis());
        failureListener.assertFailure();
    }

    private TransportAddress getDiscoveryAddress() {
        return randomBoolean() ? remoteNode.getAddress() : buildNewFakeTransportAddress();
    }

    private class FailureListener implements ActionListener<DiscoveryNode> {
        final CountDownLatch completionLatch = new CountDownLatch(1);

        @Override
        public void onResponse(DiscoveryNode discoveryNode) {
            fail(discoveryNode.toString());
        }

        @Override
        public void onFailure(Exception e) {
            completionLatch.countDown();
        }

        void assertFailure() throws InterruptedException {
            assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
        }
    }
}
