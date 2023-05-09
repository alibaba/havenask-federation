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
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.EmptyClusterInfoService;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RecoverySource;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.TestShardRouting;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.RoutingAllocation;
import org.havenask.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.havenask.cluster.routing.allocation.decider.AllocationDeciders;
import org.havenask.cluster.routing.allocation.decider.Decision;
import org.havenask.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.index.shard.ShardId;
import org.havenask.snapshots.EmptySnapshotsInfoService;
import org.havenask.test.gateway.TestGatewayAllocator;

import java.util.Collections;

import static org.havenask.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.havenask.cluster.routing.ShardRoutingState.STARTED;
import static org.havenask.cluster.routing.ShardRoutingState.UNASSIGNED;


public class ResizeAllocationDeciderTests extends HavenaskAllocationTestCase {

    private AllocationService strategy;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        strategy = new AllocationService(new AllocationDeciders(
            Collections.singleton(new ResizeAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);
    }

    private ClusterState createInitialClusterState(boolean startShards) {
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(IndexMetadata.builder("source").settings(settings(Version.CURRENT))
            .numberOfShards(2).numberOfReplicas(0).setRoutingNumShards(16));
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index("source"));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1", Version.CURRENT)).add(newNode
            ("node2", Version.CURRENT)))
            .build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertEquals(prevRoutingTable.index("source").shards().size(), 2);
        assertEquals(prevRoutingTable.index("source").shard(0).shards().get(0).state(), UNASSIGNED);
        assertEquals(prevRoutingTable.index("source").shard(1).shards().get(0).state(), UNASSIGNED);


        assertEquals(routingTable.index("source").shards().size(), 2);

        assertEquals(routingTable.index("source").shard(0).shards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("source").shard(1).shards().get(0).state(), INITIALIZING);


        if (startShards) {
            clusterState = startShardsAndReroute(strategy, clusterState,
                routingTable.index("source").shard(0).shards().get(0),
                routingTable.index("source").shard(1).shards().get(0));
            routingTable = clusterState.routingTable();
            assertEquals(routingTable.index("source").shards().size(), 2);
            assertEquals(routingTable.index("source").shard(0).shards().get(0).state(), STARTED);
            assertEquals(routingTable.index("source").shard(1).shards().get(0).state(), STARTED);

        }
        return clusterState;
    }

    public void testNonResizeRouting() {
        ClusterState clusterState = createInitialClusterState(true);
        ResizeAllocationDecider resizeAllocationDecider = new ResizeAllocationDecider();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, null, clusterState, null, null, 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting("non-resize", 0, null, true, ShardRoutingState.UNASSIGNED);
        assertEquals(Decision.ALWAYS, resizeAllocationDecider.canAllocate(shardRouting, routingAllocation));
        assertEquals(Decision.ALWAYS, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"),
            routingAllocation));
    }

    public void testShrink() { // we don't handle shrink yet
        ClusterState clusterState = createInitialClusterState(true);
        Metadata.Builder metaBuilder = Metadata.builder(clusterState.metadata());
        metaBuilder.put(IndexMetadata.builder("target").settings(settings(Version.CURRENT)
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), "source")
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, IndexMetadata.INDEX_UUID_NA_VALUE))
            .numberOfShards(1).numberOfReplicas(0));
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.routingTable());
        routingTableBuilder.addAsNew(metadata.index("target"));

        clusterState = ClusterState.builder(clusterState)
            .routingTable(routingTableBuilder.build())
            .metadata(metadata).build();
        Index idx = clusterState.metadata().index("target").getIndex();

        ResizeAllocationDecider resizeAllocationDecider = new ResizeAllocationDecider();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, null, clusterState, null, null, 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(new ShardId(idx, 0), null, true, ShardRoutingState.UNASSIGNED,
            RecoverySource.LocalShardsRecoverySource.INSTANCE);
        assertEquals(Decision.ALWAYS, resizeAllocationDecider.canAllocate(shardRouting, routingAllocation));
        assertEquals(Decision.ALWAYS, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"),
            routingAllocation));
        assertEquals(Decision.ALWAYS, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"),
            routingAllocation));
    }

    public void testSourceNotActive() {
        ClusterState clusterState = createInitialClusterState(false);
        Metadata.Builder metaBuilder = Metadata.builder(clusterState.metadata());
        metaBuilder.put(IndexMetadata.builder("target").settings(settings(Version.CURRENT)
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), "source")
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, IndexMetadata.INDEX_UUID_NA_VALUE))
            .numberOfShards(4).numberOfReplicas(0));
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.routingTable());
        routingTableBuilder.addAsNew(metadata.index("target"));

        clusterState = ClusterState.builder(clusterState)
            .routingTable(routingTableBuilder.build())
            .metadata(metadata).build();
        Index idx = clusterState.metadata().index("target").getIndex();


        ResizeAllocationDecider resizeAllocationDecider = new ResizeAllocationDecider();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        int shardId = randomIntBetween(0, 3);
        int sourceShardId = IndexMetadata.selectSplitShard(shardId, clusterState.metadata().index("source"), 4).id();
        ShardRouting shardRouting = TestShardRouting.newShardRouting(new ShardId(idx, shardId), null, true, ShardRoutingState.UNASSIGNED,
            RecoverySource.LocalShardsRecoverySource.INSTANCE);
        assertEquals(Decision.NO, resizeAllocationDecider.canAllocate(shardRouting, routingAllocation));
        assertEquals(Decision.NO, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"),
            routingAllocation));
        assertEquals(Decision.NO, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"),
            routingAllocation));

        routingAllocation.debugDecision(true);
        assertEquals("source primary shard [[source][" + sourceShardId + "]] is not active",
            resizeAllocationDecider.canAllocate(shardRouting, routingAllocation).getExplanation());
        assertEquals("source primary shard [[source][" + sourceShardId + "]] is not active",
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node0"),
            routingAllocation).getExplanation());
        assertEquals("source primary shard [[source][" + sourceShardId + "]] is not active",
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"),
            routingAllocation).getExplanation());
    }

    public void testSourcePrimaryActive() {
        ClusterState clusterState = createInitialClusterState(true);
        Metadata.Builder metaBuilder = Metadata.builder(clusterState.metadata());
        metaBuilder.put(IndexMetadata.builder("target").settings(settings(Version.CURRENT)
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), "source")
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, IndexMetadata.INDEX_UUID_NA_VALUE))
            .numberOfShards(4).numberOfReplicas(0));
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.routingTable());
        routingTableBuilder.addAsNew(metadata.index("target"));

        clusterState = ClusterState.builder(clusterState)
            .routingTable(routingTableBuilder.build())
            .metadata(metadata).build();
        Index idx = clusterState.metadata().index("target").getIndex();


        ResizeAllocationDecider resizeAllocationDecider = new ResizeAllocationDecider();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, null, null, 0);
        int shardId = randomIntBetween(0, 3);
        int sourceShardId = IndexMetadata.selectSplitShard(shardId, clusterState.metadata().index("source"), 4).id();
        ShardRouting shardRouting = TestShardRouting.newShardRouting(new ShardId(idx, shardId), null, true, ShardRoutingState.UNASSIGNED,
            RecoverySource.LocalShardsRecoverySource.INSTANCE);
        assertEquals(Decision.YES, resizeAllocationDecider.canAllocate(shardRouting, routingAllocation));

        String allowedNode = clusterState.getRoutingTable().index("source").shard(sourceShardId).primaryShard().currentNodeId();

        if ("node1".equals(allowedNode)) {
            assertEquals(Decision.YES, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"),
                routingAllocation));
            assertEquals(Decision.NO, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"),
                routingAllocation));
        } else {
            assertEquals(Decision.NO, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"),
                routingAllocation));
            assertEquals(Decision.YES, resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"),
                routingAllocation));
        }

        routingAllocation.debugDecision(true);
        assertEquals("source primary is active", resizeAllocationDecider.canAllocate(shardRouting, routingAllocation).getExplanation());

        if ("node1".equals(allowedNode)) {
            assertEquals("source primary is allocated on this node",
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"),
                    routingAllocation).getExplanation());
            assertEquals("source primary is allocated on another node",
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"),
                    routingAllocation).getExplanation());
        } else {
            assertEquals("source primary is allocated on another node",
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"),
                    routingAllocation).getExplanation());
            assertEquals("source primary is allocated on this node",
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"),
                    routingAllocation).getExplanation());
        }
    }
}
