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
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.RoutingNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.common.settings.Settings;

import static org.havenask.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ElectReplicaAsPrimaryDuringRelocationTests extends HavenaskAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ElectReplicaAsPrimaryDuringRelocationTests.class);

    public void testElectReplicaAsPrimaryDuringRelocation() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))
            .add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Start the replica shards");
        ClusterState resultingState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(resultingState, not(equalTo(clusterState)));
        clusterState = resultingState;

        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(2));

        logger.info("Start another node and perform rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("find the replica shard that gets relocated");
        IndexShardRoutingTable indexShardRoutingTable = null;
        if (clusterState.routingTable().index("test").shard(0).replicaShards().get(0).relocating()) {
            indexShardRoutingTable = clusterState.routingTable().index("test").shard(0);
        } else if (clusterState.routingTable().index("test").shard(1).replicaShards().get(0).relocating()) {
            indexShardRoutingTable = clusterState.routingTable().index("test").shard(1);
        }

        // we might have primary relocating, and the test is only for replicas, so only test in the case of replica allocation
        if (indexShardRoutingTable != null) {
            logger.info("kill the node [{}] of the primary shard for the relocating replica",
                indexShardRoutingTable.primaryShard().currentNodeId());
            clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .remove(indexShardRoutingTable.primaryShard().currentNodeId())).build();
            clusterState = strategy.disassociateDeadNodes(clusterState, true, "reroute");

            logger.info("make sure all the primary shards are active");
            assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().active(), equalTo(true));
            assertThat(clusterState.routingTable().index("test").shard(1).primaryShard().active(), equalTo(true));
        }
    }
}
