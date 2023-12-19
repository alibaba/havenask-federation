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

package org.havenask.cluster.routing;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.object.HasToString.hasToString;
import static org.havenask.cluster.routing.OperationRouting.IGNORE_AWARENESS_ATTRIBUTES_DEPRECATION_MESSAGE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.havenask.action.support.replication.ClusterStateCreationUtils;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.index.Index;
import org.havenask.index.shard.ShardId;
import org.havenask.node.ResponseCollectorService;
import org.havenask.test.ClusterServiceUtils;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;

public class OperationRoutingTests extends HavenaskTestCase {
    public void testPreferNodes() throws InterruptedException, IOException {
        TestThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool("testPreferNodes");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
            final String indexName = "test";
            ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, randomInt(8)));
            final Index index = clusterService.state().metadata().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final int count = randomIntBetween(1, shards.size());
            int position = 0;
            final List<String> nodes = new ArrayList<>();
            final List<ShardRouting> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (randomBoolean() && !shards.get(position).initializing()) {
                    nodes.add(shards.get(position).currentNodeId());
                    expected.add(shards.get(position));
                    position++;
                } else {
                    nodes.add("missing_" + i);
                }
            }
            final ShardIterator it =
                    new OperationRouting(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
                            .getShards(clusterService.state(), indexName, 0, "_prefer_nodes:" + String.join(",", nodes));
            final List<ShardRouting> all = new ArrayList<>();
            ShardRouting shard;
            while ((shard = it.nextOrNull()) != null) {
                all.add(shard);
            }
            final Set<ShardRouting> preferred = new HashSet<>();
            preferred.addAll(all.subList(0, expected.size()));
            // the preferred shards should be at the front of the list
            assertThat(preferred, containsInAnyOrder(expected.toArray()));
            // verify all the shards are there
            assertThat(all.size(), equalTo(shards.size()));
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testFairSessionIdPreferences() throws InterruptedException, IOException {
        // Ensure that a user session is re-routed back to same nodes for
        // subsequent searches and that the nodes are selected fairly i.e.
        // given identically sorted lists of nodes across all shard IDs
        // each shard ID doesn't pick the same node.
        final int numIndices = randomIntBetween(1, 3);
        final int numShards = randomIntBetween(2, 10);
        final int numReplicas = randomIntBetween(1, 3);
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        final int numRepeatedSearches = 4;
        List<ShardRouting> sessionsfirstSearch = null;
        OperationRouting opRouting = new OperationRouting(Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        String sessionKey = randomAlphaOfLength(10);
        for (int i = 0; i < numRepeatedSearches; i++) {
            List<ShardRouting> searchedShards = new ArrayList<>(numShards);
            Set<String> selectedNodes = new HashSet<>(numShards);
            final GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state, indexNames, null, sessionKey);

            assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));
            for (ShardIterator shardIterator : groupIterator) {
                assertThat(shardIterator.size(), equalTo(numReplicas + 1));

                ShardRouting firstChoice = shardIterator.nextOrNull();
                assertNotNull(firstChoice);
                ShardRouting duelFirst = duelGetShards(state, firstChoice.shardId(), sessionKey).nextOrNull();
                assertThat("Regression test failure", duelFirst, equalTo(firstChoice));

                searchedShards.add(firstChoice);
                selectedNodes.add(firstChoice.currentNodeId());
            }
            if (sessionsfirstSearch == null) {
                sessionsfirstSearch = searchedShards;
            } else {
                assertThat("Sessions must reuse same replica choices", searchedShards, equalTo(sessionsfirstSearch));
            }

            // 2 is the bare minimum number of nodes we can reliably expect from
            // randomized tests in my experiments over thousands of iterations.
            // Ideally we would test for greater levels of machine utilisation
            // given a configuration with many nodes but the nature of hash
            // collisions means we can't always rely on optimal node usage in
            // all cases.
            assertThat("Search should use more than one of the nodes", selectedNodes.size(), greaterThan(1));
        }
    }

    // Regression test for the routing logic - implements same hashing logic
    private ShardIterator duelGetShards(ClusterState clusterState, ShardId shardId, String sessionId) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(shardId.getIndexName(), shardId.getId());
        int routingHash = Murmur3HashFunction.hash(sessionId);
        routingHash = 31 * routingHash + indexShard.shardId.hashCode();
        return indexShard.activeInitializingShardsIt(routingHash);
    }

    public void testThatOnlyNodesSupportNodeIds() throws InterruptedException, IOException {
        TestThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool("testThatOnlyNodesSupportNodeIds");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
            final String indexName = "test";
            ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, randomInt(8)));
            final Index index = clusterService.state().metadata().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final int count = randomIntBetween(1, shards.size());
            int position = 0;
            final List<String> nodes = new ArrayList<>();
            final List<ShardRouting> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (randomBoolean() && !shards.get(position).initializing()) {
                    nodes.add(shards.get(position).currentNodeId());
                    expected.add(shards.get(position));
                    position++;
                } else {
                    nodes.add("missing_" + i);
                }
            }
            if (expected.size() > 0) {
                final ShardIterator it =
                    new OperationRouting(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
                        .getShards(clusterService.state(), indexName, 0, "_only_nodes:" + String.join(",", nodes));
                final List<ShardRouting> only = new ArrayList<>();
                ShardRouting shard;
                while ((shard = it.nextOrNull()) != null) {
                    only.add(shard);
                }
                assertThat(new HashSet<>(only), equalTo(new HashSet<>(expected)));
            } else {
                final ClusterService cs = clusterService;
                final IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> new OperationRouting(
                            Settings.EMPTY,
                            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
                        .getShards(cs.state(), indexName, 0, "_only_nodes:" + String.join(",", nodes)));
                if (nodes.size() == 1) {
                    assertThat(
                        e,
                        hasToString(containsString(
                            "no data nodes with criteria [" + String.join(",", nodes) + "] found for shard: [test][0]")));
                } else {
                    assertThat(
                        e,
                        hasToString(containsString(
                            "no data nodes with criterion [" + String.join(",", nodes) + "] found for shard: [test][0]")));
                }
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testAdaptiveReplicaSelection() throws Exception {
        final int numIndices = 1;
        final int numShards = 1;
        final int numReplicas = 2;
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        OperationRouting opRouting = new OperationRouting(Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        opRouting.setUseAdaptiveReplicaSelection(true);
        List<ShardRouting> searchedShards = new ArrayList<>(numShards);
        Set<String> selectedNodes = new HashSet<>(numShards);
        TestThreadPool threadPool = new TestThreadPool("testThatOnlyNodesSupportNodeIds");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ResponseCollectorService collector = new ResponseCollectorService(clusterService);
        Map<String, Long> outstandingRequests = new HashMap<>();
        GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state,
                indexNames, null, null, collector, outstandingRequests);

        assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

        // Test that the shards use a round-robin pattern when there are no stats
        assertThat(groupIterator.get(0).size(), equalTo(numReplicas + 1));
        ShardRouting firstChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(firstChoice);
        searchedShards.add(firstChoice);
        selectedNodes.add(firstChoice.currentNodeId());

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        ShardRouting secondChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(secondChoice);
        searchedShards.add(secondChoice);
        selectedNodes.add(secondChoice.currentNodeId());

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        ShardRouting thirdChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(thirdChoice);
        searchedShards.add(thirdChoice);
        selectedNodes.add(thirdChoice.currentNodeId());

        // All three shards should have been separate, because there are no stats yet so they're all ranked equally.
        assertThat(searchedShards.size(), equalTo(3));

        // Now let's start adding node metrics, since that will affect which node is chosen
        collector.addNodeStatistics("node_0", 2, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(150).nanos());
        collector.addNodeStatistics("node_1", 1, TimeValue.timeValueMillis(100).nanos(), TimeValue.timeValueMillis(50).nanos());
        collector.addNodeStatistics("node_2", 1, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(200).nanos());
        outstandingRequests.put("node_0", 1L);
        outstandingRequests.put("node_1", 1L);
        outstandingRequests.put("node_2", 1L);

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);
        ShardRouting shardChoice = groupIterator.get(0).nextOrNull();
        // node 1 should be the lowest ranked node to start
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // node 1 starts getting more loaded...
        collector.addNodeStatistics("node_1", 2, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(150).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);
        shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // and more loaded...
        collector.addNodeStatistics("node_1", 3, TimeValue.timeValueMillis(250).nanos(), TimeValue.timeValueMillis(200).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);
        shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // and even more
        collector.addNodeStatistics("node_1", 4, TimeValue.timeValueMillis(300).nanos(), TimeValue.timeValueMillis(250).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);
        shardChoice = groupIterator.get(0).nextOrNull();
        // finally, node 2 is chosen instead
        assertThat(shardChoice.currentNodeId(), equalTo("node_2"));

        IOUtils.close(clusterService);
        terminate(threadPool);
    }

    public void testAllocationAwarenessDeprecation() {
        OperationRouting routing = new OperationRouting(
            Settings.builder()
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "foo")
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertWarnings(IGNORE_AWARENESS_ATTRIBUTES_DEPRECATION_MESSAGE);
    }

}
