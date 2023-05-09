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

package org.havenask.cluster.routing.allocation;

import org.havenask.Version;
import org.havenask.cluster.ClusterInfo;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.EmptyClusterInfoService;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.IndexRoutingTable;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.UnassignedInfo;
import org.havenask.cluster.routing.allocation.AllocateUnassignedDecision;
import org.havenask.cluster.routing.allocation.AllocationDecision;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.ExistingShardsAllocator;
import org.havenask.cluster.routing.allocation.FailedShard;
import org.havenask.cluster.routing.allocation.NodeAllocationResult;
import org.havenask.cluster.routing.allocation.RoutingAllocation;
import org.havenask.cluster.routing.allocation.ShardAllocationDecision;
import org.havenask.cluster.routing.allocation.allocator.ShardsAllocator;
import org.havenask.cluster.routing.allocation.decider.AllocationDeciders;
import org.havenask.cluster.routing.allocation.decider.Decision;
import org.havenask.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.havenask.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.gateway.GatewayAllocator;
import org.havenask.snapshots.EmptySnapshotsInfoService;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.havenask.cluster.routing.UnassignedInfo.AllocationStatus.DECIDERS_NO;
import static org.havenask.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING;
import static org.havenask.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.havenask.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AllocationServiceTests extends HavenaskTestCase {

    public void testFirstListElementsToCommaDelimitedStringReportsAllElementsIfShort() {
        List<String> strings = IntStream.range(0, between(0, 10)).mapToObj(i -> randomAlphaOfLength(10)).collect(Collectors.toList());
        assertAllElementsReported(strings, randomBoolean());
    }

    public void testFirstListElementsToCommaDelimitedStringReportsAllElementsIfDebugEnabled() {
        List<String> strings = IntStream.range(0, between(0, 100)).mapToObj(i -> randomAlphaOfLength(10)).collect(Collectors.toList());
        assertAllElementsReported(strings, true);
    }

    private void assertAllElementsReported(List<String> strings, boolean isDebugEnabled) {
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, Function.identity(), isDebugEnabled);
        for (String string : strings) {
            assertThat(abbreviated, containsString(string));
        }
        assertThat(abbreviated, not(containsString("...")));
    }

    public void testFirstListElementsToCommaDelimitedStringReportsFirstElementsIfLong() {
        List<String> strings = IntStream.range(0, between(0, 100))
            .mapToObj(i -> randomAlphaOfLength(between(6, 10))).distinct().collect(Collectors.toList());
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, Function.identity(), false);
        for (int i = 0; i < strings.size(); i++) {
            if (i < 10) {
                assertThat(abbreviated, containsString(strings.get(i)));
            } else {
                assertThat(abbreviated, not(containsString(strings.get(i))));
            }
        }

        if (strings.size() > 10) {
            assertThat(abbreviated, containsString("..."));
            assertThat(abbreviated, containsString("[" + strings.size() + " items in total]"));
        } else {
            assertThat(abbreviated, not(containsString("...")));
        }
    }

    public void testFirstListElementsToCommaDelimitedStringUsesFormatterNotToString() {
        List<String> strings = IntStream.range(0, between(1, 100)).mapToObj(i -> "original").collect(Collectors.toList());
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, s -> "formatted", randomBoolean());
        assertThat(abbreviated, containsString("formatted"));
        assertThat(abbreviated, not(containsString("original")));
    }

    public void testAssignsPrimariesInPriorityOrderThenReplicas() {
        // throttle (incoming) recoveries in order to observe the order of operations, but do not throttle outgoing recoveries since
        // the effects of that depend on the earlier (random) allocations
        final Settings settings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 1)
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AllocationService allocationService = new AllocationService(
            new AllocationDeciders(Arrays.asList(
                new SameShardAllocationDecider(settings, clusterSettings),
                new ThrottlingAllocationDecider(settings, clusterSettings))),
            new ShardsAllocator() {
                @Override
                public void allocate(RoutingAllocation allocation) {
                    // all primaries are handled by existing shards allocators in these tests; even the invalid allocator prevents shards
                    // from falling through to here
                    assertThat(allocation.routingNodes().unassigned().getNumPrimaries(), equalTo(0));
                }

                @Override
                public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                    return ShardAllocationDecision.NOT_TAKEN;
                }
            }, new EmptyClusterInfoService(), EmptySnapshotsInfoService.INSTANCE);

        final String unrealisticAllocatorName = "unrealistic";
        final Map<String, ExistingShardsAllocator> allocatorMap = new HashMap<>();
        final TestGatewayAllocator testGatewayAllocator = new TestGatewayAllocator();
        allocatorMap.put(GatewayAllocator.ALLOCATOR_NAME, testGatewayAllocator);
        allocatorMap.put(unrealisticAllocatorName, new UnrealisticAllocator());
        allocationService.setExistingShardsAllocators(allocatorMap);

        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT));
        nodesBuilder.add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT));
        nodesBuilder.add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT));

        final Metadata.Builder metadata = Metadata.builder()
            // create 3 indices with different priorities. The high and low priority indices use the default allocator which (in this test)
            // does not allocate any replicas, whereas the medium priority one uses the unrealistic allocator which does allocate replicas
            .put(indexMetadata("highPriority", Settings.builder()
                .put(IndexMetadata.SETTING_PRIORITY, 10)))
            .put(indexMetadata("mediumPriority", Settings.builder()
                .put(IndexMetadata.SETTING_PRIORITY, 5)
                .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), unrealisticAllocatorName)))
            .put(indexMetadata("lowPriority", Settings.builder()
                .put(IndexMetadata.SETTING_PRIORITY, 3)))

            // also create a 4th index with arbitrary priority and an invalid allocator that we expect to ignore
            .put(indexMetadata("invalid", Settings.builder()
                .put(IndexMetadata.SETTING_PRIORITY, between(0, 15))
                .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "unknown")));

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder()
            .addAsRecovery(metadata.get("highPriority"))
            .addAsRecovery(metadata.get("mediumPriority"))
            .addAsRecovery(metadata.get("lowPriority"))
            .addAsRecovery(metadata.get("invalid"));

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        // permit the testGatewayAllocator to allocate primaries to every node
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                final ShardRouting primaryShard = indexShardRoutingTable.primaryShard();
                for (DiscoveryNode node : clusterState.nodes()) {
                    testGatewayAllocator.addKnownAllocation(primaryShard.initialize(node.getId(), FAKE_IN_SYNC_ALLOCATION_ID, 0L));
                }
            }
        }

        final ClusterState reroutedState1 = rerouteAndStartShards(allocationService, clusterState);
        final RoutingTable routingTable1 = reroutedState1.routingTable();
        // the test harness only permits one recovery per node, so we must have allocated all the high-priority primaries and one of the
        // medium-priority ones
        assertThat(routingTable1.shardsWithState(ShardRoutingState.INITIALIZING), empty());
        assertThat(routingTable1.shardsWithState(ShardRoutingState.RELOCATING), empty());
        assertTrue(routingTable1.shardsWithState(ShardRoutingState.STARTED).stream().allMatch(ShardRouting::primary));
        assertThat(routingTable1.index("highPriority").primaryShardsActive(), equalTo(2));
        assertThat(routingTable1.index("mediumPriority").primaryShardsActive(), equalTo(1));
        assertThat(routingTable1.index("lowPriority").shardsWithState(ShardRoutingState.STARTED), empty());
        assertThat(routingTable1.index("invalid").shardsWithState(ShardRoutingState.STARTED), empty());

        final ClusterState reroutedState2 = rerouteAndStartShards(allocationService, reroutedState1);
        final RoutingTable routingTable2 = reroutedState2.routingTable();
        // this reroute starts the one remaining medium-priority primary and both of the low-priority ones, but no replicas
        assertThat(routingTable2.shardsWithState(ShardRoutingState.INITIALIZING), empty());
        assertThat(routingTable2.shardsWithState(ShardRoutingState.RELOCATING), empty());
        assertTrue(routingTable2.shardsWithState(ShardRoutingState.STARTED).stream().allMatch(ShardRouting::primary));
        assertTrue(routingTable2.index("highPriority").allPrimaryShardsActive());
        assertTrue(routingTable2.index("mediumPriority").allPrimaryShardsActive());
        assertTrue(routingTable2.index("lowPriority").allPrimaryShardsActive());
        assertThat(routingTable2.index("invalid").shardsWithState(ShardRoutingState.STARTED), empty());

        final ClusterState reroutedState3 = rerouteAndStartShards(allocationService, reroutedState2);
        final RoutingTable routingTable3 = reroutedState3.routingTable();
        // this reroute starts the two medium-priority replicas since their allocator permits this
        assertThat(routingTable3.shardsWithState(ShardRoutingState.INITIALIZING), empty());
        assertThat(routingTable3.shardsWithState(ShardRoutingState.RELOCATING), empty());
        assertTrue(routingTable3.index("highPriority").allPrimaryShardsActive());
        assertThat(routingTable3.index("mediumPriority").shardsWithState(ShardRoutingState.UNASSIGNED), empty());
        assertTrue(routingTable3.index("lowPriority").allPrimaryShardsActive());
        assertThat(routingTable3.index("invalid").shardsWithState(ShardRoutingState.STARTED), empty());
    }

    public void testExplainsNonAllocationOfShardWithUnknownAllocator() {
        final AllocationService allocationService = new AllocationService(null, null, null, null);
        allocationService.setExistingShardsAllocators(
            Collections.singletonMap(GatewayAllocator.ALLOCATOR_NAME, new TestGatewayAllocator()));

        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT));
        nodesBuilder.add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT));

        final Metadata.Builder metadata = Metadata.builder().put(indexMetadata("index", Settings.builder()
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "unknown")));

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder().addAsRecovery(metadata.get("index"));

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        final RoutingAllocation allocation = new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            clusterState.getRoutingNodes(), clusterState, ClusterInfo.EMPTY, null,0L);
        allocation.setDebugMode(randomBoolean() ? RoutingAllocation.DebugMode.ON : RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);

        final ShardAllocationDecision shardAllocationDecision
            = allocationService.explainShardAllocation(clusterState.routingTable().index("index").shard(0).primaryShard(), allocation);

        assertTrue(shardAllocationDecision.isDecisionTaken());
        assertThat(shardAllocationDecision.getAllocateDecision().getAllocationStatus(),
            equalTo(UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY));
        assertThat(shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
            equalTo(AllocationDecision.NO_VALID_SHARD_COPY));
        assertThat(shardAllocationDecision.getAllocateDecision().getExplanation(), equalTo("cannot allocate because a previous copy of " +
            "the primary shard existed but can no longer be found on the nodes in the cluster"));

        for (NodeAllocationResult nodeAllocationResult : shardAllocationDecision.getAllocateDecision().nodeDecisions) {
            assertThat(nodeAllocationResult.getNodeDecision(), equalTo(AllocationDecision.NO));
            assertThat(nodeAllocationResult.getCanAllocateDecision().type(), equalTo(Decision.Type.NO));
            assertThat(nodeAllocationResult.getCanAllocateDecision().label(), equalTo("allocator_plugin"));
            assertThat(nodeAllocationResult.getCanAllocateDecision().getExplanation(), equalTo("finding the previous copies of this " +
                "shard requires an allocator called [unknown] but that allocator was not found; perhaps the corresponding plugin is " +
                "not installed"));
        }
    }

    private static final String FAKE_IN_SYNC_ALLOCATION_ID = "_in_sync_"; // so we can allocate primaries anywhere

    private static IndexMetadata.Builder indexMetadata(String name, Settings.Builder settings) {
        return IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT).put(settings.build()))
            .numberOfShards(2).numberOfReplicas(1)
            .putInSyncAllocationIds(0, Collections.singleton(FAKE_IN_SYNC_ALLOCATION_ID))
            .putInSyncAllocationIds(1, Collections.singleton(FAKE_IN_SYNC_ALLOCATION_ID));
    }

    /**
     * Allocates shards to nodes regardless of whether there's already a shard copy there.
     */
    private static class UnrealisticAllocator implements ExistingShardsAllocator {

        @Override
        public void beforeAllocation(RoutingAllocation allocation) {
        }

        @Override
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        }

        @Override
        public void allocateUnassigned(ShardRouting shardRouting, RoutingAllocation allocation,
                                       UnassignedAllocationHandler unassignedAllocationHandler) {
            final AllocateUnassignedDecision allocateUnassignedDecision = explainUnassignedShardAllocation(shardRouting, allocation);
            if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
                unassignedAllocationHandler.initialize(allocateUnassignedDecision.getTargetNode().getId(),
                    shardRouting.primary() ? FAKE_IN_SYNC_ALLOCATION_ID : null, 0L, allocation.changes());
            } else {
                unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
            }
        }

        @Override
        public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation allocation) {
            boolean throttled = false;

            for (final RoutingNode routingNode : allocation.routingNodes()) {
                final Decision decision = allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
                if (decision.type() == Decision.Type.YES) {
                    return AllocateUnassignedDecision.yes(routingNode.node(), null, null, false);
                } else {
                    if (shardRouting.index().getName().equals("mediumPriority") && shardRouting.primary() == false
                        && decision.type() == Decision.Type.THROTTLE) {
                        allocation.deciders().canAllocate(shardRouting, routingNode, allocation);
                    }
                }

                throttled = throttled || decision.type() == Decision.Type.THROTTLE;
            }

            return throttled ? AllocateUnassignedDecision.throttle(null)
                : AllocateUnassignedDecision.no(DECIDERS_NO, null);
        }

        @Override
        public void cleanCaches() {
        }

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
        }

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {
        }

        @Override
        public int getNumberOfInFlightFetches() {
            return 0;
        }
    }

    private static ClusterState rerouteAndStartShards(final AllocationService allocationService, final ClusterState clusterState) {
        final ClusterState reroutedState = allocationService.reroute(clusterState, "test");
        return allocationService.applyStartedShards(reroutedState,
            reroutedState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING));
    }

}
