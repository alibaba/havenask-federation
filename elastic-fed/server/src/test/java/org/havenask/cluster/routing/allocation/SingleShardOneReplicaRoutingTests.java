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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SingleShardOneReplicaRoutingTests extends HavenaskAllocationTestCase {
    private final Logger logger = LogManager.getLogger(SingleShardOneReplicaRoutingTests.class);

    public void testSingleIndexFirstStartPrimaryThenBackups() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(1).state(), equalTo(UNASSIGNED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), nullValue());
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(1).currentNodeId(), nullValue());

        logger.info("Adding one node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(INITIALIZING));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), nullValue());

        logger.info("Add another node and perform rerouting, nothing will happen since primary shards not started");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the primary shard (on node1)");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        newState = startInitializingShardsAndReroute(strategy, clusterState, routingNodes.node("node1"));
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        // backup shards are initializing as well, we make sure that they recover
        // from primary *started* shards in the IndicesClusterStateService
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(INITIALIZING));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(),
            equalTo("node2"));


        logger.info("Reroute, nothing should change");
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the backup shard");
        routingNodes = clusterState.getRoutingNodes();
        newState = startInitializingShardsAndReroute(strategy, clusterState, routingNodes.node("node2"));
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo("node1"));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(),
            equalTo("node2"));

        logger.info("Kill node1, backup shard should become primary");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1")).build();
        newState = strategy.disassociateDeadNodes(clusterState, true, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        // backup shards are initializing as well, we make sure that they
        // recover from primary *started* shards in the IndicesClusterStateService
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), nullValue());

        logger.info("Start another node, backup shard should start initializing");

        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        // backup shards are initializing as well, we make sure that they
        // recover from primary *started* shards in the IndicesClusterStateService
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(INITIALIZING));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(),
            equalTo("node3"));
    }
}
