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

package org.havenask.node;

import org.havenask.cluster.ClusterChangedEvent;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ResponseCollectorServiceTests extends HavenaskTestCase {

    private ClusterService clusterService;
    private ResponseCollectorService collector;
    private ThreadPool threadpool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadpool = new TestThreadPool("response_collector_tests");
        clusterService = new ClusterService(Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadpool);
        collector = new ResponseCollectorService(clusterService);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdownNow();
    }

    public void testNodeStats() throws Exception {
        collector.addNodeStatistics("node1", 1, 100, 10);
        Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertThat(nodeStats.get("node1").queueSize, equalTo(1));
        assertThat(nodeStats.get("node1").responseTime, equalTo(100.0));
        assertThat(nodeStats.get("node1").serviceTime, equalTo(10.0));
    }

    /*
     * Test that concurrently adding values and removing nodes does not cause exceptions
     */
    public void testConcurrentAddingAndRemoving() throws Exception {
        String[] nodes = new String[] {"a", "b", "c", "d"};

        final CountDownLatch latch = new CountDownLatch(5);

        Runnable f = () -> {
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail("should not be interrupted");
            }
            for (int i = 0; i < randomIntBetween(100, 200); i++) {
                if (randomBoolean()) {
                    collector.removeNode(randomFrom(nodes));
                }
                collector.addNodeStatistics(randomFrom(nodes), randomIntBetween(1,100), randomIntBetween(1,100), randomIntBetween(1,100));
            }
        };

        Thread t1 = new Thread(f);
        Thread t2 = new Thread(f);
        Thread t3 = new Thread(f);
        Thread t4 = new Thread(f);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        latch.countDown();
        t1.join();
        t2.join();
        t3.join();
        t4.join();

        final Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        logger.info("--> got stats: {}", nodeStats);
        for (String nodeId : nodes) {
            if (nodeStats.containsKey(nodeId)) {
                assertThat(nodeStats.get(nodeId).queueSize, greaterThan(0));
                assertThat(nodeStats.get(nodeId).responseTime, greaterThan(0.0));
                assertThat(nodeStats.get(nodeId).serviceTime, greaterThan(0.0));
            }
        }
    }

    public void testNodeRemoval() throws Exception {
        collector.addNodeStatistics("node1", randomIntBetween(1,100), randomIntBetween(1,100), randomIntBetween(1,100));
        collector.addNodeStatistics("node2", randomIntBetween(1,100), randomIntBetween(1,100), randomIntBetween(1,100));

        ClusterState previousState = ClusterState.builder(new ClusterName("cluster")).nodes(DiscoveryNodes.builder()
                .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node1"))
                .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9201), "node2")))
                .build();
        ClusterState newState = ClusterState.builder(previousState).nodes(DiscoveryNodes.builder(previousState.nodes())
                .remove("node2")).build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, previousState);

        collector.clusterChanged(event);
        final Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertFalse(nodeStats.containsKey("node2"));
    }
}
