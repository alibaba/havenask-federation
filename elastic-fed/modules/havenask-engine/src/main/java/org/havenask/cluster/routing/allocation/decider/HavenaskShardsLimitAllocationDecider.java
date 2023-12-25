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

import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.allocation.RoutingAllocation;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.engine.EngineSettings;

import java.util.function.BiPredicate;

public class HavenaskShardsLimitAllocationDecider extends AllocationDecider {

    public static final String NAME = "havenask_shards_limit";
    private volatile int clusterHavenaskShardLimit;

    /**
     * Controls the maximum number of havenask shards per node on a global level.
     * Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> CLUSTER_TOTAL_HAVENASK_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "cluster.routing.allocation.total_havenask_shards_per_node",
        100,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );

    private final Settings settings;

    public HavenaskShardsLimitAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.settings = settings;
        this.clusterHavenaskShardLimit = CLUSTER_TOTAL_HAVENASK_SHARDS_PER_NODE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_TOTAL_HAVENASK_SHARDS_PER_NODE_SETTING, this::setClusterShardLimit);
    }

    private void setClusterShardLimit(int clusterHavenaskShardLimit) {
        this.clusterHavenaskShardLimit = clusterHavenaskShardLimit;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(shardRouting, node, allocation, (count, limit) -> count >= limit);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(shardRouting, node, allocation, (count, limit) -> count > limit);

    }

    private Decision doDecide(
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        BiPredicate<Integer, Integer> decider
    ) {
        // execution
        final int clusterHavenaskShardLimit = this.clusterHavenaskShardLimit;

        final int nodeHavenaskShardCount = countNodeHavenaskShard(node, allocation);

        if (clusterHavenaskShardLimit > 0 && decider.test(nodeHavenaskShardCount, clusterHavenaskShardLimit)) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "too many shards [%d] allocated to this node, cluster setting [%s=%d]",
                nodeHavenaskShardCount,
                CLUSTER_TOTAL_HAVENASK_SHARDS_PER_NODE_SETTING.getKey(),
                clusterHavenaskShardLimit
            );
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "the shard count [%d] for this node is under the cluster level node limit [%d]",
            nodeHavenaskShardCount,
            clusterHavenaskShardLimit
        );
    }

    private int countNodeHavenaskShard(RoutingNode node, RoutingAllocation allocation) {
        int nodeHavenaskShardCount = 0;
        for (ShardRouting shardRouting : node) {
            IndexMetadata indexMetadata = allocation.metadata().index(shardRouting.index());
            if (EngineSettings.isHavenaskEngine(indexMetadata.getSettings())) {
                nodeHavenaskShardCount++;
            }
        }

        return nodeHavenaskShardCount;
    }
}
