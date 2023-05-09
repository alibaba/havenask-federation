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
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.cluster.routing.allocation.decider;

import org.havenask.Version;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.common.collect.Tuple;
import org.havenask.common.settings.Settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ConcurrentRecoveriesAllocationDeciderTests extends HavenaskAllocationTestCase {

    public void testClusterConcurrentRecoveries() {
        int primaryShards = 5, replicaShards = 1, numberIndices = 12;
        int clusterConcurrentRecoveries = -1;
        int nodeConcurrentRecoveries = 4;
        AllocationService initialStrategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
                    .put("cluster.routing.allocation.node_initial_primaries_recoveries", "8")
                    .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(nodeConcurrentRecoveries))
                    .put("cluster.routing.allocation.exclude.tag", "tag_0").build());

        AllocationService excludeStrategy = null;

        logger.info("Building initial routing table");

        Metadata.Builder metadataBuilder = Metadata.builder();
        for (int i = 0; i < numberIndices; i++) {
            metadataBuilder.put(IndexMetadata.builder("test_" + i).settings(settings(Version.CURRENT)).numberOfShards(primaryShards)
                                    .numberOfReplicas(replicaShards));
        }
        RoutingTable.Builder initialRoutingTableBuilder = RoutingTable.builder();
        Metadata metadata = metadataBuilder.build();
        for (int i = 0; i < numberIndices; i++) {
            initialRoutingTableBuilder.addAsNew(metadata.index("test_" + i));
        }
        RoutingTable routingTable = initialRoutingTableBuilder.build();

        logger.info("--> adding nodes and starting shards");

        List<Tuple<Integer, Integer>> srcTargetNodes = Collections.unmodifiableList(Arrays.<Tuple<Integer, Integer>>asList(
            new Tuple(10, 4),
            new Tuple(4, 10),
            new Tuple(10, 10))
        );

        for (Tuple<Integer, Integer> srcTargetNode : srcTargetNodes) {

            int srcNodes = srcTargetNode.v1();
            int targetNodes = srcTargetNode.v2();

            logger.info("Setting up tests for src node {} and target node {}", srcNodes, targetNodes);

            ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metadata(metadata)
                                            .routingTable(routingTable).nodes(setUpClusterNodes(srcNodes, targetNodes)).build();

            clusterState = initialStrategy.reroute(clusterState, "reroute");

            // Initialize shards

            logger.info("--> Starting primary shards");
            while (clusterState.getRoutingNodes().hasUnassignedShards()) {
                clusterState = startInitializingShardsAndReroute(initialStrategy, clusterState);
            }

            logger.info("--> Starting replica shards");
            while (clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() > 0) {
                clusterState = startInitializingShardsAndReroute(initialStrategy, clusterState);
            }

            assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(),
                equalTo((replicaShards + 1) * primaryShards * numberIndices));
            assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(0));
            assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(0));

            clusterConcurrentRecoveries = Math.min(srcNodes, targetNodes) * nodeConcurrentRecoveries;
            excludeStrategy = createAllocationService(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
                    .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(nodeConcurrentRecoveries))
                    .put("cluster.routing.allocation.cluster_concurrent_recoveries", String.valueOf(clusterConcurrentRecoveries))
                    .put("cluster.routing.allocation.exclude.tag", "tag_1").build());

            for (int counter = 0; counter < 3; counter++) {
                logger.info("--> Performing a reroute ");
                clusterState = excludeStrategy.reroute(clusterState, "reroute");
                assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(),
                    equalTo(clusterConcurrentRecoveries));
                for (ShardRouting startedShard : clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED)) {
                    assertThat(clusterState.getRoutingNodes().node(startedShard.currentNodeId()).node().getAttributes().get("tag"),
                        equalTo("tag_1"));
                }
            }

            // Ensure all shards are started
            while (clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() > 0) {
                clusterState = startInitializingShardsAndReroute(initialStrategy, clusterState);
            }

            clusterConcurrentRecoveries = clusterConcurrentRecoveries - randomInt(5);
            excludeStrategy = createAllocationService(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
                    .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(nodeConcurrentRecoveries))
                    .put("cluster.routing.allocation.cluster_concurrent_recoveries", String.valueOf(clusterConcurrentRecoveries))
                    .put("cluster.routing.allocation.exclude.tag", "tag_1").build());

            for (int counter = 0; counter < 3; counter++) {
                logger.info("--> Performing a reroute ");
                clusterState = excludeStrategy.reroute(clusterState, "reroute");
                assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(),
                    equalTo(clusterConcurrentRecoveries));
                for (ShardRouting startedShard : clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED)) {
                    assertThat(clusterState.getRoutingNodes().node(startedShard.currentNodeId()).node().getAttributes().get("tag"),
                        equalTo("tag_1"));
                }
            }

            // Ensure all shards are started
            while (clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() > 0) {
                clusterState = startInitializingShardsAndReroute(initialStrategy, clusterState);
            }

            logger.info("--> Disabling cluster_concurrent_recoveries and re-routing ");
            clusterConcurrentRecoveries = Math.min(srcNodes, targetNodes) * nodeConcurrentRecoveries;

            for (int counter = 0; counter < 3; counter++) {
                logger.info("--> Performing a reroute ");
                excludeStrategy = createAllocationService(
                    Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
                            .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(nodeConcurrentRecoveries))
                            .put("cluster.routing.allocation.exclude.tag", "tag_1").build());

                clusterState = excludeStrategy.reroute(clusterState, "reroute");
                //When srcNodes < targetNodes relocations go beyond the Math.min(srcNodes, targetNodes) * nodeConcurrentRecoveries limit as
                // outgoing recoveries happens target nodes which anyways doesn't get throttled on incoming recoveries
                if (srcNodes >= targetNodes) {
                    assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(),
                        equalTo(clusterConcurrentRecoveries));
                }else {
                    assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(),
                        greaterThanOrEqualTo(clusterConcurrentRecoveries));
                }

            }
            // Ensure all shards are started
            while (clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() > 0) {
                clusterState = startInitializingShardsAndReroute(initialStrategy, clusterState);
            }

            logger.info("--> Bumping cluster_concurrent_recoveries up and re-routing ");
            clusterConcurrentRecoveries = clusterConcurrentRecoveries + randomInt(5);
            int expectedClusterConcurrentRecoveries = Math.min(srcNodes, targetNodes) * nodeConcurrentRecoveries;
            for (int counter = 0; counter < 3; counter++) {
                logger.info("--> Performing a reroute ");
                excludeStrategy = createAllocationService(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
                        .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(nodeConcurrentRecoveries))
                        .put("cluster.routing.allocation.exclude.tag", "tag_1").build());
                clusterState = excludeStrategy.reroute(clusterState, "reroute");
                assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(),
                        equalTo(expectedClusterConcurrentRecoveries));

            }
        }
    }

    private DiscoveryNodes.Builder setUpClusterNodes(int sourceNodes, int targetNodes) {
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 1; i <= sourceNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("tag", "tag_" + 1);
            attributes.put("zone", "zone_" + (i % 2));
            nb.add(newNode("node_s_" + i, attributes));
        }
        for (int j = 1; j <= targetNodes; j++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("tag", "tag_" + 0);
            attributes.put("zone", "zone_" + (j % 2));
            nb.add(newNode("node_t_" + j, attributes));
        }
        return nb;
    }
}
