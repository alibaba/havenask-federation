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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.Version;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.common.settings.Settings;

import static org.havenask.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.havenask.cluster.routing.ShardRoutingState.STARTED;
import static org.havenask.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PrimaryElectionRoutingTests extends HavenaskAllocationTestCase {
    private final Logger logger = LogManager.getLogger(PrimaryElectionRoutingTests.class);

    public void testBackupElectionToPrimaryWhenPrimaryCanBeAllocatedToAnotherNode() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start the primary shard (on node1)");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, routingNodes.node("node1"));

        logger.info("Start the backup shard (on node2)");
        routingNodes = clusterState.getRoutingNodes();
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, routingNodes.node("node2"));

        logger.info("Adding third node and reroute and kill first node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node3")).remove("node1")).build();
        RoutingTable prevRoutingTable = clusterState.routingTable();
        clusterState = strategy.disassociateDeadNodes(clusterState, true, "reroute");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = clusterState.routingTable();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingNodes.node("node1"), nullValue());
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));
        // verify where the primary is
        assertThat(routingTable.index("test").shard(0).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(clusterState.metadata().index("test").primaryTerm(0), equalTo(2L));
        assertThat(routingTable.index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo("node3"));
    }

    public void testRemovingInitializingReplicasIfPrimariesFails() {
        AllocationService allocation = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))
            .add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        logger.info("Start the primary shards");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.shardsWithState(STARTED).size(), equalTo(2));
        assertThat(routingNodes.shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.metadata().index("test").primaryTerm(0), equalTo(1L));
        assertThat(clusterState.metadata().index("test").primaryTerm(1), equalTo(1L));

        // now, fail one node, while the replica is initializing, and it also holds a primary
        logger.info("--> fail node with primary");
        String nodeIdToFail = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String nodeIdRemaining = nodeIdToFail.equals("node1") ? "node2" : "node1";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode(nodeIdRemaining))).build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.shardsWithState(STARTED).size(), equalTo(1));
        assertThat(routingNodes.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingNodes.shardsWithState(UNASSIGNED).size(), equalTo(3)); // 2 replicas and one primary
        assertThat(routingNodes.node(nodeIdRemaining).shardsWithState(STARTED).get(0).primary(), equalTo(true));
        assertThat(clusterState.metadata().index("test").primaryTerm(0), equalTo(2L));

    }
}
