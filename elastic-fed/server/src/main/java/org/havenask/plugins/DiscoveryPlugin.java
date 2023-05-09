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

import org.havenask.cluster.ClusterState;
import org.havenask.cluster.coordination.ElectionStrategy;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.network.NetworkService;
import org.havenask.common.settings.Settings;
import org.havenask.discovery.SeedHostsProvider;
import org.havenask.transport.TransportService;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * An additional extension point for {@link Plugin}s that extends Havenask's discovery functionality. To add an additional
 * {@link NetworkService.CustomNameResolver} just implement the interface and implement the {@link #getCustomNameResolver(Settings)} method:
 *
 * <pre>
 * public class MyDiscoveryPlugin extends Plugin implements DiscoveryPlugin {
 *     &#64;Override
 *     public NetworkService.CustomNameResolver getCustomNameResolver(Settings settings) {
 *         return new YourCustomNameResolverInstance(settings);
 *     }
 * }
 * </pre>
 */
public interface DiscoveryPlugin {

    /**
     * Override to add additional {@link NetworkService.CustomNameResolver}s.
     * This can be handy if you want to provide your own Network interface name like _mycard_
     * and implement by yourself the logic to get an actual IP address/hostname based on this
     * name.
     *
     * For example: you could call a third party service (an API) to resolve _mycard_.
     * Then you could define in havenask.yml settings like:
     *
     * <pre>{@code
     * network.host: _mycard_
     * }</pre>
     */
    default NetworkService.CustomNameResolver getCustomNameResolver(Settings settings) {
        return null;
    }

    /**
     * Returns providers of seed hosts for discovery.
     *
     * The key of the returned map is the name of the host provider
     * (see {@link org.havenask.discovery.DiscoveryModule#DISCOVERY_SEED_PROVIDERS_SETTING}), and
     * the value is a supplier to construct the host provider when it is selected for use.
     *
     * @param transportService Use to form the {@link org.havenask.common.transport.TransportAddress} portion
     *                         of a {@link org.havenask.cluster.node.DiscoveryNode}
     * @param networkService Use to find the publish host address of the current node
     */
    default Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService,
                                                                          NetworkService networkService) {
        return Collections.emptyMap();
    }

    /**
     * Returns a consumer that validate the initial join cluster state. The validator, unless <code>null</code> is called exactly once per
     * join attempt but might be called multiple times during the lifetime of a node. Validators are expected to throw a
     * {@link IllegalStateException} if the node and the cluster-state are incompatible.
     */
    default BiConsumer<DiscoveryNode,ClusterState> getJoinValidator() { return null; }

    /**
     * Allows plugging in election strategies (see {@link ElectionStrategy}) that define a customized notion of an election quorum.
     */
    default Map<String, ElectionStrategy> getElectionStrategies() {
        return Collections.emptyMap();
    }
}
