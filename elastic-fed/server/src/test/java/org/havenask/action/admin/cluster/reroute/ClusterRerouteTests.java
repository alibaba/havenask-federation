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

package org.havenask.action.admin.cluster.reroute;

import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.EmptyClusterInfoService;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.FailedShard;
import org.havenask.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.havenask.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.havenask.cluster.routing.allocation.decider.AllocationDeciders;
import org.havenask.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.network.NetworkModule;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.snapshots.EmptySnapshotsInfoService;
import org.havenask.test.gateway.TestGatewayAllocator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.havenask.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.havenask.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ClusterRerouteTests extends HavenaskAllocationTestCase {

    public void testSerializeRequest() throws IOException {
        ClusterRerouteRequest req = new ClusterRerouteRequest();
        req.setRetryFailed(randomBoolean());
        req.dryRun(randomBoolean());
        req.explain(randomBoolean());
        req.add(new AllocateEmptyPrimaryAllocationCommand("foo", 1, "bar", randomBoolean()));
        req.timeout(TimeValue.timeValueMillis(randomIntBetween(0, 100)));
        BytesStreamOutput out = new BytesStreamOutput();
        req.writeTo(out);
        BytesReference bytes = out.bytes();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
        StreamInput wrap = new NamedWriteableAwareStreamInput(bytes.streamInput(),
            namedWriteableRegistry);
        ClusterRerouteRequest deserializedReq = new ClusterRerouteRequest(wrap);

        assertEquals(req.isRetryFailed(), deserializedReq.isRetryFailed());
        assertEquals(req.dryRun(), deserializedReq.dryRun());
        assertEquals(req.explain(), deserializedReq.explain());
        assertEquals(req.timeout(), deserializedReq.timeout());
        assertEquals(1, deserializedReq.getCommands().commands().size()); // allocation commands have their own tests
        assertEquals(req.getCommands().commands().size(), deserializedReq.getCommands().commands().size());
    }

    public void testClusterStateUpdateTask() {
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);
        ClusterState clusterState = createInitialClusterState(allocationService);
        ClusterRerouteRequest req = new ClusterRerouteRequest();
        req.dryRun(true);
        AtomicReference<ClusterRerouteResponse> responseRef = new AtomicReference<>();
        ActionListener<ClusterRerouteResponse> responseActionListener = new ActionListener<ClusterRerouteResponse>() {
            @Override
            public void onResponse(ClusterRerouteResponse clusterRerouteResponse) {
                responseRef.set(clusterRerouteResponse);
            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        TransportClusterRerouteAction.ClusterRerouteResponseAckedClusterStateUpdateTask task =
            new TransportClusterRerouteAction.ClusterRerouteResponseAckedClusterStateUpdateTask(logger, allocationService, req,
                responseActionListener );
        ClusterState execute = task.execute(clusterState);
        assertSame(execute, clusterState); // dry-run
        task.onAllNodesAcked(null);
        assertNotSame(responseRef.get().getState(), execute);

        req.dryRun(false);// now we allocate

        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        // now fail it N-1 times
        for (int i = 0; i < retries; i++) {
            ClusterState newState = task.execute(clusterState);
            assertNotSame(newState, clusterState); // dry-run=false
            clusterState = newState;
            RoutingTable routingTable = clusterState.routingTable();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), i);
            List<FailedShard> failedShards = Collections.singletonList(
                new FailedShard(routingTable.index("idx").shard(0).shards().get(0), "boom" + i,
                    new UnsupportedOperationException(), randomBoolean()));
            newState = allocationService.applyFailedShards(clusterState, failedShards);
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = clusterState.routingTable();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            if (i == retries-1) {
                assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
            } else {
                assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
            }
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), i+1);
        }


        // without retry_failed we won't allocate that shard
        ClusterState newState = task.execute(clusterState);
        assertNotSame(newState, clusterState); // dry-run=false
        task.onAllNodesAcked(null);
        assertSame(responseRef.get().getState(), newState);
        RoutingTable routingTable = clusterState.routingTable();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), retries);

        req.setRetryFailed(true); // now we manually retry and get the shard back into initializing
        newState = task.execute(clusterState);
        assertNotSame(newState, clusterState); // dry-run=false
        clusterState = newState;
        routingTable = clusterState.routingTable();
        assertEquals(1, routingTable.index("idx").shards().size());
        assertEquals(INITIALIZING, routingTable.index("idx").shard(0).shards().get(0).state());
        assertEquals(0, routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations());
    }

    private ClusterState createInitialClusterState(AllocationService service) {
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index("idx"));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertEquals(prevRoutingTable.index("idx").shards().size(), 1);
        assertEquals(prevRoutingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);

        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        return clusterState;
    }
}
