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

package org.havenask.transport;

import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.LatchedActionListener;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.havenask.action.search.SearchRequest;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.transport.MockTransportService;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteClusterAwareClientTests extends HavenaskTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes) {
        return RemoteClusterConnectionTests.startTransport(id, knownNodes, Version.CURRENT, threadPool);
    }

    public void testSearchShards() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (MockTransportService service = MockTransportService.createNewService(builder.build(), Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();

                try (RemoteClusterAwareClient client = new RemoteClusterAwareClient(Settings.EMPTY, threadPool, service, "cluster1")) {
                    SearchRequest request = new SearchRequest("test-index");
                    CountDownLatch responseLatch = new CountDownLatch(1);
                    AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                    ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest("test-index")
                        .indicesOptions(request.indicesOptions()).local(true).preference(request.preference())
                        .routing(request.routing());
                    client.admin().cluster().searchShards(searchShardsRequest,
                        new LatchedActionListener<>(ActionListener.wrap(reference::set, e -> fail("no failures expected")), responseLatch));
                    responseLatch.await();
                    assertNotNull(reference.get());
                    ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
                    assertEquals(knownNodes, Arrays.asList(clusterSearchShardsResponse.getNodes()));
                }
            }
        }
    }

    public void testSearchShardsThreadContextHeader() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (MockTransportService service = MockTransportService.createNewService(builder.build(), Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();

                try (RemoteClusterAwareClient client = new RemoteClusterAwareClient(Settings.EMPTY, threadPool, service, "cluster1")) {
                    SearchRequest request = new SearchRequest("test-index");
                    int numThreads = 10;
                    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
                    for (int i = 0; i < numThreads; i++) {
                        final String threadId = Integer.toString(i);
                        executorService.submit(() -> {
                            ThreadContext threadContext = seedTransport.threadPool.getThreadContext();
                            threadContext.putHeader("threadId", threadId);
                            AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                            final ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest("test-index")
                                .indicesOptions(request.indicesOptions()).local(true).preference(request.preference())
                                .routing(request.routing());
                            CountDownLatch responseLatch = new CountDownLatch(1);
                            client.admin().cluster().searchShards(searchShardsRequest,
                                new LatchedActionListener<>(ActionListener.wrap(
                                    resp -> {
                                        reference.set(resp);
                                        assertEquals(threadId, seedTransport.threadPool.getThreadContext().getHeader("threadId"));
                                    },
                                    e -> fail("no failures expected")), responseLatch));
                            try {
                                responseLatch.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            assertNotNull(reference.get());
                            ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
                            assertEquals(knownNodes, Arrays.asList(clusterSearchShardsResponse.getNodes()));
                        });
                    }
                    ThreadPool.terminate(executorService, 5, TimeUnit.SECONDS);
                }
            }
        }
    }
}
