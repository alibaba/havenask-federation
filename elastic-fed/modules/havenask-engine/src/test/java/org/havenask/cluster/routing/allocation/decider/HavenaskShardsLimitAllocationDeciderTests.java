/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.Version;
import org.havenask.cluster.ClusterModule;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.EmptyClusterInfoService;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class HavenaskShardsLimitAllocationDeciderTests extends HavenaskAllocationTestCase {
    private final Logger logger = LogManager.getLogger(HavenaskShardsLimitAllocationDeciderTests.class);

    public void testHavenaskShardsLimitAllocationDecider() {
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
            .put(HavenaskShardsLimitAllocationDecider.CLUSTER_TOTAL_HAVENASK_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        Set<Setting<?>> clusterSetSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSetSettings.add(HavenaskShardsLimitAllocationDecider.CLUSTER_TOTAL_HAVENASK_SHARDS_PER_NODE_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSetSettings);

        List<AllocationDecider> deciders = new ArrayList<>(
            ClusterModule.createAllocationDeciders(settings, clusterSettings, Arrays.asList(new HavenaskEnginePlugin(settings)))
        );
        Collections.shuffle(deciders, random());

        AllocationService strategy = new MockAllocationService(
            new AllocationDeciders(deciders),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(
                        settings(Version.CURRENT).put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            )
            .put(
                IndexMetadata.builder("lucene_test")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).addAsNew(metadata.index("lucene_test")).build();

        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(3));

        logger.info("Start the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(3));
        assertThat(clusterState.getRoutingNodes().unassigned().size(), equalTo(2));

        // Bump the cluster total shards to 2
        strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(HavenaskShardsLimitAllocationDecider.CLUSTER_TOTAL_HAVENASK_SHARDS_PER_NODE_SETTING.getKey(), 2)
                .build()
        );

        logger.info("Do another reroute, make sure shards are now allocated");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(4));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(4));
        assertThat(clusterState.getRoutingNodes().unassigned().size(), equalTo(0));
    }
}
