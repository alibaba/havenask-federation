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

package org.havenask.cluster.routing.allocation.decider;

import org.havenask.Version;
import org.havenask.cluster.ClusterModule;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.EmptyClusterInfoService;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.RoutingAllocation;
import org.havenask.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.plugins.ClusterPlugin;
import org.havenask.cluster.routing.allocation.decider.AllocationDecider;
import org.havenask.cluster.routing.allocation.decider.AllocationDeciders;
import org.havenask.cluster.routing.allocation.decider.Decision;
import org.havenask.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.havenask.snapshots.EmptySnapshotsInfoService;
import org.havenask.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.havenask.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.havenask.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.havenask.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class EnableAllocationShortCircuitTests extends HavenaskAllocationTestCase {

    private static ClusterState createClusterStateWithAllShardsAssigned() {
        AllocationService allocationService = createAllocationService(Settings.EMPTY);

        final int numberOfNodes = randomIntBetween(1, 5);
        final DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            discoveryNodesBuilder.add(newNode("node" + i));
        }

        final Metadata.Builder metadataBuilder = Metadata.builder();
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = randomIntBetween(1, 10); i >= 0; i--) {
            final IndexMetadata indexMetadata = IndexMetadata.builder("test" + i).settings(settings(Version.CURRENT))
                .numberOfShards(1).numberOfReplicas(randomIntBetween(0, numberOfNodes - 1)).build();
            metadataBuilder.put(indexMetadata, true);
            routingTableBuilder.addAsNew(indexMetadata);
        }

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(Settings.EMPTY))
            .nodes(discoveryNodesBuilder).metadata(metadataBuilder).routingTable(routingTableBuilder.build()).build();

        while (clusterState.getRoutingNodes().hasUnassignedShards()
               || clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).isEmpty() == false) {
            clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        }

        return clusterState;
    }

    public void testRebalancingAttemptedIfPermitted() {
        ClusterState clusterState = createClusterStateWithAllShardsAssigned();

        final RebalanceShortCircuitPlugin plugin = new RebalanceShortCircuitPlugin();
        AllocationService allocationService = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                    randomFrom(EnableAllocationDecider.Rebalance.ALL,
                        EnableAllocationDecider.Rebalance.PRIMARIES,
                        EnableAllocationDecider.Rebalance.REPLICAS).name()),
            plugin);
        allocationService.reroute(clusterState, "reroute").routingTable();
        assertThat(plugin.rebalanceAttempts, greaterThan(0));
    }

    public void testRebalancingSkippedIfDisabled() {
        ClusterState clusterState = createClusterStateWithAllShardsAssigned();

        final RebalanceShortCircuitPlugin plugin = new RebalanceShortCircuitPlugin();
        AllocationService allocationService = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE.name()),
            plugin);
        allocationService.reroute(clusterState, "reroute").routingTable();
        assertThat(plugin.rebalanceAttempts, equalTo(0));
    }

    public void testRebalancingSkippedIfDisabledIncludingOnSpecificIndices() {
        ClusterState clusterState = createClusterStateWithAllShardsAssigned();
        final IndexMetadata indexMetadata = randomFrom(clusterState.metadata().indices().values().toArray(IndexMetadata.class));
        clusterState = ClusterState.builder(clusterState).metadata(Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder(indexMetadata).settings(Settings.builder().put(indexMetadata.getSettings())
                .put(INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE.name()))).build()).build();

        final RebalanceShortCircuitPlugin plugin = new RebalanceShortCircuitPlugin();
        AllocationService allocationService = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE.name()),
            plugin);
        allocationService.reroute(clusterState, "reroute").routingTable();
        assertThat(plugin.rebalanceAttempts, equalTo(0));
    }

    public void testRebalancingAttemptedIfDisabledButOverridenOnSpecificIndices() {
        ClusterState clusterState = createClusterStateWithAllShardsAssigned();
        final IndexMetadata indexMetadata = randomFrom(clusterState.metadata().indices().values().toArray(IndexMetadata.class));
        clusterState = ClusterState.builder(clusterState).metadata(Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder(indexMetadata).settings(Settings.builder().put(indexMetadata.getSettings())
                .put(INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                    randomFrom(EnableAllocationDecider.Rebalance.ALL,
                        EnableAllocationDecider.Rebalance.PRIMARIES,
                        EnableAllocationDecider.Rebalance.REPLICAS).name()))).build()).build();

        final RebalanceShortCircuitPlugin plugin = new RebalanceShortCircuitPlugin();
        AllocationService allocationService = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE.name()),
            plugin);
        allocationService.reroute(clusterState, "reroute").routingTable();
        assertThat(plugin.rebalanceAttempts, greaterThan(0));
    }

    public void testAllocationSkippedIfDisabled() {
        final AllocateShortCircuitPlugin plugin = new AllocateShortCircuitPlugin();
        AllocationService allocationService = createAllocationService(Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE.name()),
            plugin);

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        allocationService.reroute(clusterState, "reroute").routingTable();
        assertThat(plugin.canAllocateAttempts, equalTo(0));
    }

    private static AllocationService createAllocationService(Settings.Builder settings, ClusterPlugin plugin) {
        final ClusterSettings emptyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        List<AllocationDecider> deciders = new ArrayList<>(ClusterModule.createAllocationDeciders(settings.build(), emptyClusterSettings,
                Collections.singletonList(plugin)));
        return new MockAllocationService(
            new AllocationDeciders(deciders),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);
    }

    private static class RebalanceShortCircuitPlugin implements ClusterPlugin {
        int rebalanceAttempts;

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return Collections.singletonList(new RebalanceShortCircuitAllocationDecider());
        }

        private class RebalanceShortCircuitAllocationDecider extends AllocationDecider {

            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                rebalanceAttempts++;
                return super.canRebalance(shardRouting, allocation);
            }

            @Override
            public Decision canRebalance(RoutingAllocation allocation) {
                rebalanceAttempts++;
                return super.canRebalance(allocation);
            }
        }
    }

    private static class AllocateShortCircuitPlugin implements ClusterPlugin {
        int canAllocateAttempts;

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return Collections.singletonList(new AllocateShortCircuitAllocationDecider());
        }

        private class AllocateShortCircuitAllocationDecider extends AllocationDecider {

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                canAllocateAttempts++;
                return super.canAllocate(shardRouting, node, allocation);
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                canAllocateAttempts++;
                return super.canAllocate(shardRouting, allocation);
            }

            @Override
            public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
                canAllocateAttempts++;
                return super.canAllocate(indexMetadata, node, allocation);
            }
        }
    }
}
