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

package org.havenask.action.admin.indices.shrink;

import org.apache.lucene.index.IndexWriter;
import org.havenask.Version;
import org.havenask.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.EmptyClusterInfoService;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.havenask.cluster.routing.allocation.decider.AllocationDeciders;
import org.havenask.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.havenask.common.settings.Settings;
import org.havenask.index.shard.DocsStats;
import org.havenask.snapshots.EmptySnapshotsInfoService;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.gateway.TestGatewayAllocator;
import org.havenask.action.admin.indices.shrink.ResizeRequest;
import org.havenask.action.admin.indices.shrink.ResizeType;
import org.havenask.action.admin.indices.shrink.TransportResizeAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class TransportResizeActionTests extends HavenaskTestCase {

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        return createClusterState(name, numShards, numReplicas, numShards, settings);
    }
    private ClusterState createClusterState(String name, int numShards, int numReplicas, int numRoutingShards, Settings settings) {
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(name).settings(settings(Version.CURRENT)
            .put(settings))
            .numberOfShards(numShards).numberOfReplicas(numReplicas).setRoutingNumShards(numRoutingShards).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index(name));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable).blocks(ClusterBlocks.builder().addBlocks(indexMetadata)).build();
        return clusterState;
    }

    public void testErrorCondition() {
        ClusterState state = createClusterState("source", randomIntBetween(2, 42), randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());
        assertTrue(
            expectThrows(IllegalStateException.class, () ->
                TransportResizeAction.prepareCreateIndexRequest(new ResizeRequest("target", "source"), state,
                (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100)), "source", "target")
        ).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards "));


        assertTrue(
            expectThrows(IllegalStateException.class, () -> {
                ResizeRequest req = new ResizeRequest("target", "source");
                req.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 4));
                ClusterState clusterState = createClusterState("source", 8, 1,
                    Settings.builder().put("index.blocks.write", true).build());
                    TransportResizeAction.prepareCreateIndexRequest(req, clusterState,
                        (i) -> i == 2 || i == 3 ? new DocsStats(Integer.MAX_VALUE / 2, between(1, 1000), between(1, 10000)) : null
                        , "source", "target");
                }
            ).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards "));


        IllegalArgumentException softDeletesError = expectThrows(IllegalArgumentException.class, () -> {
            ResizeRequest req = new ResizeRequest("target", "source");
            req.getTargetIndexRequest().settings(Settings.builder().put("index.soft_deletes.enabled", false));
            ClusterState clusterState = createClusterState("source", 8, 1,
                Settings.builder().put("index.blocks.write", true).put("index.soft_deletes.enabled", true).build());
            TransportResizeAction.prepareCreateIndexRequest(req, clusterState,
                (i) -> new DocsStats(between(10, 1000), between(1, 10), between(1, 10000)), "source", "target");
        });
        assertThat(softDeletesError.getMessage(), equalTo("Can't disable [index.soft_deletes.enabled] setting on resize"));

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState("source", randomIntBetween(2, 10), 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(new AllocationDeciders(
            Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = HavenaskAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        TransportResizeAction.prepareCreateIndexRequest(new ResizeRequest("target", "source"), clusterState,
            (i) -> new DocsStats(between(1, 1000), between(1, 1000), between(0, 10000)), "source", "target");
    }

    public void testPassNumRoutingShards() {
        ClusterState clusterState = ClusterState.builder(createClusterState("source", 1, 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(new AllocationDeciders(
            Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = HavenaskAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put("index.number_of_shards", 2).build());
        TransportResizeAction.prepareCreateIndexRequest(resizeRequest, clusterState, null, "source", "target");

        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder()
                .put("index.number_of_routing_shards", randomIntBetween(2, 10))
                .put("index.number_of_shards", 2)
                .build());
        TransportResizeAction.prepareCreateIndexRequest(resizeRequest, clusterState, null, "source", "target");
    }

    public void testPassNumRoutingShardsAndFail() {
        int numShards = randomIntBetween(2, 100);
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0, numShards * 4,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(new AllocationDeciders(
            Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = HavenaskAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put("index.number_of_shards", numShards * 2).build());
        TransportResizeAction.prepareCreateIndexRequest(resizeRequest, clusterState, null, "source", "target");

        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder()
                .put("index.number_of_shards", numShards * 2)
                .put("index.number_of_routing_shards", numShards * 2).build());
        ClusterState finalState = clusterState;
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
            () -> TransportResizeAction.prepareCreateIndexRequest(resizeRequest, finalState, null, "source", "target"));
        assertEquals("cannot provide index.number_of_routing_shards on resize", iae.getMessage());
    }

    public void testShrinkIndexSettings() {
        String indexName = randomAlphaOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState(indexName, randomIntBetween(2, 10), 0,
            Settings.builder()
                .put("index.blocks.write", true)
                .build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(new AllocationDeciders(
            Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = HavenaskAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, indexName).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int numSourceShards = clusterState.metadata().index(indexName).getNumberOfShards();
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / numSourceShards), between(1, 1000), between(1, 10000));
        ResizeRequest target = new ResizeRequest("target", indexName);
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        target.setWaitForActiveShards(activeShardCount);
        CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
            target, clusterState, (i) -> stats, indexName, "target");
        assertNotNull(request.recoverFrom());
        assertEquals(indexName, request.recoverFrom().getName());
        assertEquals("1", request.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request.cause());
        assertEquals(request.waitForActiveShards(), activeShardCount);
    }

    private DiscoveryNode newNode(String nodeId) {
        final Set<DiscoveryNodeRole> roles =
                Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)));
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }
}
