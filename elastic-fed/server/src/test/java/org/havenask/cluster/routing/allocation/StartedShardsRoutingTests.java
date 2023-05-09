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
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.AllocationId;
import org.havenask.cluster.routing.IndexRoutingTable;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.TestShardRouting;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.set.Sets;
import org.havenask.index.Index;
import org.havenask.index.shard.ShardId;
import org.havenask.cluster.routing.allocation.AllocationService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class StartedShardsRoutingTests extends HavenaskAllocationTestCase {
    public void testStartedShardsMatching() {
        AllocationService allocation = createAllocationService();

        logger.info("--> building initial cluster state");
        AllocationId allocationId = AllocationId.newRelocation(AllocationId.newInitializing());
        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(2).numberOfReplicas(0)
                .putInSyncAllocationIds(1, Collections.singleton(allocationId.getId()))
                .build();
        final Index index = indexMetadata.getIndex();
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
                .metadata(Metadata.builder().put(indexMetadata, false));

        final ShardRouting initShard = TestShardRouting.newShardRouting(new ShardId(index, 0), "node1",
            true, ShardRoutingState.INITIALIZING);
        final ShardRouting relocatingShard = TestShardRouting.newShardRouting(new ShardId(index, 1), "node1",
            "node2", true, ShardRoutingState.RELOCATING, allocationId);
        stateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(initShard.shardId()).addShard(initShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(relocatingShard.shardId()).addShard(relocatingShard).build())).build());

        ClusterState state = stateBuilder.build();

        logger.info("--> test starting of shard");

        ClusterState newState = startShardsAndReroute(allocation, state, initShard);
        assertThat("failed to start " + initShard + "\ncurrent routing table:" +
            newState.routingTable(), newState, not(equalTo(state)));
        assertTrue(initShard + "isn't started \ncurrent routing table:" + newState.routingTable(),
                newState.routingTable().index("test").shard(initShard.id()).allShardsStarted());
        state = newState;

        logger.info("--> testing starting of relocating shards");
        newState = startShardsAndReroute(allocation, state, relocatingShard.getTargetRelocatingShard());
        assertThat("failed to start " + relocatingShard + "\ncurrent routing table:" + newState.routingTable(),
            newState, not(equalTo(state)));
        ShardRouting shardRouting = newState.routingTable().index("test").shard(relocatingShard.id()).getShards().get(0);
        assertThat(shardRouting.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));
        assertThat(shardRouting.relocatingNodeId(), nullValue());
    }

    public void testRelocatingPrimariesWithInitializingReplicas() {
        AllocationService allocation = createAllocationService();

        logger.info("--> building initial cluster state");
        AllocationId primaryId = AllocationId.newRelocation(AllocationId.newInitializing());
        AllocationId replicaId = AllocationId.newInitializing();
        boolean relocatingReplica = randomBoolean();
        if (relocatingReplica) {
            replicaId = AllocationId.newRelocation(replicaId);
        }

        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(1)
            .putInSyncAllocationIds(0,
                relocatingReplica ? Sets.newHashSet(primaryId.getId(), replicaId.getId()) : Sets.newHashSet(primaryId.getId()))
            .build();
        final Index index = indexMetadata.getIndex();
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))
                .add(newNode("node3")).add(newNode("node4")))
            .metadata(Metadata.builder().put(indexMetadata, false));

        final ShardRouting relocatingPrimary = TestShardRouting.newShardRouting(new ShardId(index, 0), "node1",
            "node2", true, ShardRoutingState.RELOCATING, primaryId);
        final ShardRouting replica = TestShardRouting.newShardRouting(
            new ShardId(index, 0), "node3", relocatingReplica ? "node4" : null, false,
            relocatingReplica ? ShardRoutingState.RELOCATING : ShardRoutingState.INITIALIZING, replicaId);

        stateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(relocatingPrimary.shardId())
                .addShard(relocatingPrimary)
                .addShard(replica)
                .build()))
            .build());


        ClusterState state = stateBuilder.build();

        logger.info("--> test starting of relocating primary shard with initializing / relocating replica");
        ClusterState newState = startShardsAndReroute(allocation, state, relocatingPrimary.getTargetRelocatingShard());
        assertNotEquals(newState, state);
        assertTrue(newState.routingTable().index("test").allPrimaryShardsActive());
        ShardRouting startedReplica = newState.routingTable().index("test").shard(0).replicaShards().get(0);
        if (relocatingReplica) {
            assertTrue(startedReplica.relocating());
            assertEquals(replica.currentNodeId(), startedReplica.currentNodeId());
            assertEquals(replica.relocatingNodeId(), startedReplica.relocatingNodeId());
            assertEquals(replica.allocationId().getId(), startedReplica.allocationId().getId());
            assertNotEquals(replica.allocationId().getRelocationId(), startedReplica.allocationId().getRelocationId());
        } else {
            assertTrue(startedReplica.initializing());
            assertEquals(replica.currentNodeId(), startedReplica.currentNodeId());
            assertNotEquals(replica.allocationId().getId(), startedReplica.allocationId().getId());
        }

        logger.info("--> test starting of relocating primary shard together with initializing / relocating replica");
        List<ShardRouting> startedShards = new ArrayList<>();
        startedShards.add(relocatingPrimary.getTargetRelocatingShard());
        startedShards.add(relocatingReplica ? replica.getTargetRelocatingShard() : replica);
        Collections.shuffle(startedShards, random());
        newState = startShardsAndReroute(allocation, state, startedShards);
        assertNotEquals(newState, state);
        assertTrue(newState.routingTable().index("test").shard(0).allShardsStarted());
    }
}
