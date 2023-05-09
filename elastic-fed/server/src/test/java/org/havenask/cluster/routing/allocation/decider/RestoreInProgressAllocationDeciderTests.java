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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.havenask.Version;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.RestoreInProgress;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.IndexRoutingTable;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.RecoverySource;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.UnassignedInfo;
import org.havenask.cluster.routing.allocation.RoutingAllocation;
import org.havenask.common.UUIDs;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.index.shard.ShardId;
import org.havenask.cluster.routing.allocation.decider.AllocationDecider;
import org.havenask.cluster.routing.allocation.decider.AllocationDeciders;
import org.havenask.cluster.routing.allocation.decider.Decision;
import org.havenask.cluster.routing.allocation.decider.RestoreInProgressAllocationDecider;
import org.havenask.repositories.IndexId;
import org.havenask.snapshots.Snapshot;
import org.havenask.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.singletonList;

/**
 * Test {@link RestoreInProgressAllocationDecider}
 */
public class RestoreInProgressAllocationDeciderTests extends HavenaskAllocationTestCase {

    public void testCanAllocatePrimary() {
        ClusterState clusterState = createInitialClusterState();
        ShardRouting shard;
        if (randomBoolean()) {
            shard = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
            assertEquals(RecoverySource.Type.EMPTY_STORE, shard.recoverySource().getType());
        }  else {
            shard = clusterState.getRoutingTable().shardRoutingTable("test", 0).replicaShards().get(0);
            assertEquals(RecoverySource.Type.PEER, shard.recoverySource().getType());
        }

        final Decision decision = executeAllocation(clusterState, shard);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("ignored as shard is not being recovered from a snapshot", decision.getExplanation());
    }

    public void testCannotAllocatePrimaryMissingInRestoreInProgress() {
        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = RoutingTable.builder(clusterState.getRoutingTable())
            .addAsRestore(clusterState.getMetadata().index("test"), createSnapshotRecoverySource("_missing"))
            .build();

        clusterState = ClusterState.builder(clusterState)
            .routingTable(routingTable)
            .build();

        ShardRouting primary = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
        assertEquals(ShardRoutingState.UNASSIGNED, primary.state());
        assertEquals(RecoverySource.Type.SNAPSHOT, primary.recoverySource().getType());

        final Decision decision = executeAllocation(clusterState, primary);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals("shard has failed to be restored from the snapshot [_repository:_missing/_uuid] because of " +
            "[restore_source[_repository/_missing]] - manually close or delete the index [test] in order to retry to restore " +
            "the snapshot again or use the reroute API to force the allocation of an empty primary shard", decision.getExplanation());
    }

    public void testCanAllocatePrimaryExistingInRestoreInProgress() {
        RecoverySource.SnapshotRecoverySource recoverySource = createSnapshotRecoverySource("_existing");

        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = RoutingTable.builder(clusterState.getRoutingTable())
            .addAsRestore(clusterState.getMetadata().index("test"), recoverySource)
            .build();

        clusterState = ClusterState.builder(clusterState)
            .routingTable(routingTable)
            .build();

        ShardRouting primary = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
        assertEquals(ShardRoutingState.UNASSIGNED, primary.state());
        assertEquals(RecoverySource.Type.SNAPSHOT, primary.recoverySource().getType());

        routingTable = clusterState.routingTable();

        final RestoreInProgress.State shardState;
        if (randomBoolean()) {
            shardState = randomFrom(RestoreInProgress.State.STARTED, RestoreInProgress.State.INIT);
        } else {
            shardState = RestoreInProgress.State.FAILURE;

            UnassignedInfo currentInfo = primary.unassignedInfo();
            UnassignedInfo newInfo = new UnassignedInfo(currentInfo.getReason(), currentInfo.getMessage(), new IOException("i/o failure"),
                currentInfo.getNumFailedAllocations(), currentInfo.getUnassignedTimeInNanos(), currentInfo.getUnassignedTimeInMillis(),
                currentInfo.isDelayed(), currentInfo.getLastAllocationStatus(), currentInfo.getFailedNodeIds());
            primary = primary.updateUnassigned(newInfo, primary.recoverySource());

            IndexRoutingTable indexRoutingTable = routingTable.index("test");
            IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
            for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
                final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
                for (ShardRouting shardRouting : shardRoutingTable.getShards()) {
                    if (shardRouting.primary()) {
                        newIndexRoutingTable.addShard(primary);
                    } else {
                        newIndexRoutingTable.addShard(shardRouting);
                    }
                }
            }
            routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        }

        ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shards = ImmutableOpenMap.builder();
        shards.put(primary.shardId(), new RestoreInProgress.ShardRestoreStatus(clusterState.getNodes().getLocalNodeId(), shardState));

        Snapshot snapshot = recoverySource.snapshot();
        RestoreInProgress.State restoreState = RestoreInProgress.State.STARTED;
        RestoreInProgress.Entry restore =
            new RestoreInProgress.Entry(recoverySource.restoreUUID(), snapshot, restoreState, singletonList("test"), shards.build());

        clusterState = ClusterState.builder(clusterState)
            .putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(restore).build())
            .routingTable(routingTable)
            .build();

        Decision decision = executeAllocation(clusterState, primary);
        if (shardState == RestoreInProgress.State.FAILURE) {
            assertEquals(Decision.Type.NO, decision.type());
            assertEquals("shard has failed to be restored from the snapshot [_repository:_existing/_uuid] because of " +
                "[restore_source[_repository/_existing], failure IOException[i/o failure]] - manually close or delete the index " +
                "[test] in order to retry to restore the snapshot again or use the reroute API to force the allocation of " +
                "an empty primary shard", decision.getExplanation());
        } else {
            assertEquals(Decision.Type.YES, decision.type());
            assertEquals("shard is currently being restored", decision.getExplanation());
        }
    }

    private ClusterState createInitialClusterState() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test"))
            .build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(newNode("master", Collections.singleton(DiscoveryNodeRole.MASTER_ROLE)))
            .localNodeId("master")
            .masterNodeId("master")
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size());
        return clusterState;
    }

    private Decision executeAllocation(final ClusterState clusterState, final ShardRouting shardRouting) {
        final AllocationDecider decider = new RestoreInProgressAllocationDecider();
        final RoutingAllocation allocation = new RoutingAllocation(new AllocationDeciders(Collections.singleton(decider)),
            clusterState.getRoutingNodes(), clusterState, null, null, 0L);
        allocation.debugDecision(true);

        final Decision decision;
        if (randomBoolean()) {
            decision = decider.canAllocate(shardRouting, allocation);
        } else {
            DiscoveryNode node = clusterState.getNodes().getMasterNode();
            decision = decider.canAllocate(shardRouting, new RoutingNode(node.getId(), node), allocation);
        }
        return decision;
    }

    private RecoverySource.SnapshotRecoverySource createSnapshotRecoverySource(final String snapshotName) {
        Snapshot snapshot = new Snapshot("_repository", new SnapshotId(snapshotName, "_uuid"));
        return new RecoverySource.SnapshotRecoverySource(UUIDs.randomBase64UUID(), snapshot, Version.CURRENT,
            new IndexId("test", UUIDs.randomBase64UUID(random())));
    }
}
