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

package org.havenask.plugins;

import org.havenask.cluster.routing.allocation.ExistingShardsAllocator;
import org.havenask.cluster.routing.allocation.allocator.ShardsAllocator;
import org.havenask.cluster.routing.allocation.decider.AllocationDecider;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An extension point for {@link Plugin} implementations to customer behavior of cluster management.
 */
public interface ClusterPlugin {

    /**
     * Return deciders used to customize where shards are allocated.
     *
     * @param settings Settings for the node
     * @param clusterSettings Settings for the cluster
     * @return Custom {@link AllocationDecider} instances
     */
    default Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return Collections.emptyList();
    }

    /**
     * Return {@link ShardsAllocator} implementations added by this plugin.
     *
     * The key of the returned {@link Map} is the name of the allocator, and the value
     * is a function to construct the allocator.
     *
     * @param settings Settings for the node
     * @param clusterSettings Settings for the cluster
     * @return A map of allocator implementations
     */
    default Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
        return Collections.emptyMap();
    }

    /**
     * Return {@link ExistingShardsAllocator} implementations added by this plugin; the index setting
     * {@link ExistingShardsAllocator#EXISTING_SHARDS_ALLOCATOR_SETTING} sets the key of the allocator to use to allocate its shards. The
     * default allocator is {@link org.havenask.gateway.GatewayAllocator}.
     */
    default Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        return Collections.emptyMap();
    }

    /**
     * Called when the node is started
     */
    default void onNodeStarted() {
    }

}
