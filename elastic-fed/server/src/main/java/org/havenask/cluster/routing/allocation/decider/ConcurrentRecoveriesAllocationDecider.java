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

import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.allocation.RoutingAllocation;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This {@link AllocationDecider} controls the number of currently in-progress
 * re-balance (relocation) operations and restricts node allocations if the
 * configured threshold is reached.
 * <p>
 * Re-balance operations can be controlled in real-time via the cluster update API using
 * <code>cluster.routing.allocation.cluster_concurrent_recoveries</code>. Iff this
 * setting is set to <code>-1</code> the number of cluster concurrent recoveries operations
 * are unlimited.
 */
public class ConcurrentRecoveriesAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(ConcurrentRecoveriesAllocationDecider.class);

    public static final String NAME = "cluster_concurrent_recoveries";


    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING =
        Setting.intSetting("cluster.routing.allocation.cluster_concurrent_recoveries", -1, -1,
            Property.Dynamic, Property.NodeScope);

    private volatile int clusterConcurrentRecoveries;

    public ConcurrentRecoveriesAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.clusterConcurrentRecoveries = CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING.get(settings);
        logger.debug("using [cluster_concurrent_rebalance] with [{}]", clusterConcurrentRecoveries);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING,
            this::setClusterConcurrentRebalance);
    }

    private void setClusterConcurrentRebalance(int clusterConcurrentRecoveries) {
        this.clusterConcurrentRecoveries = clusterConcurrentRecoveries;
    }

    @Override
    public Decision canMoveAnyShard(RoutingAllocation allocation) {
        if (clusterConcurrentRecoveries == -1) {
            return allocation.decision(Decision.YES, NAME, "undefined cluster concurrent recoveries");
        }
        int relocatingShards = allocation.routingNodes().getRelocatingShardCount();
        if (relocatingShards >= clusterConcurrentRecoveries) {
            return allocation.decision(Decision.THROTTLE, NAME,
                "too many shards are concurrently relocating [%d], limit: [%d] cluster setting [%s=%d]",
                relocatingShards, clusterConcurrentRecoveries, CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING.getKey(),
                clusterConcurrentRecoveries);
        }
        return allocation.decision(Decision.YES, NAME,
            "below threshold [%d] for concurrent recoveries, current relocating shard count [%d]",
            clusterConcurrentRecoveries, relocatingShards);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canMoveAnyShard(allocation);
    }

}
