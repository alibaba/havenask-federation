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
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.havenask.common.settings.Settings;

import static org.havenask.cluster.routing.ShardRoutingState.INITIALIZING;

public class AllocationPriorityTests extends HavenaskAllocationTestCase {

    /**
     * Tests that higher prioritized primaries and replicas are allocated first even on the balanced shard allocator
     * See https://github.com/elastic/elasticsearch/issues/13249 for details
     */
    public void testPrioritizedIndicesAllocatedFirst() {
        AllocationService allocation = createAllocationService(Settings.builder().
                put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 10)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
            .build());
        final String highPriorityName;
        final String lowPriorityName;
        final int priorityFirst;
        final int prioritySecond;
        if (randomBoolean()) {
            highPriorityName = "first";
            lowPriorityName = "second";
            prioritySecond = 1;
            priorityFirst = 100;
        } else {
            lowPriorityName = "first";
            highPriorityName = "second";
            prioritySecond = 100;
            priorityFirst = 1;
        }
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("first").settings(settings(Version.CURRENT)
                    .put(IndexMetadata.SETTING_PRIORITY, priorityFirst)).numberOfShards(2).numberOfReplicas(1))
                .put(IndexMetadata.builder("second").settings(settings(Version.CURRENT)
                    .put(IndexMetadata.SETTING_PRIORITY, prioritySecond)).numberOfShards(2).numberOfReplicas(1))
                .build();
        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metadata.index("first"))
                .addAsNew(metadata.index("second"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))
            .add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        clusterState = allocation.reroute(clusterState, "reroute");
        assertEquals(2, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(highPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0).getIndexName());
        assertEquals(highPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(2, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(lowPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0).getIndexName());
        assertEquals(lowPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).toString(),2,
            clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(highPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0).getIndexName());
        assertEquals(highPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(2, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(lowPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0).getIndexName());
        assertEquals(lowPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(1).getIndexName());

    }
}
