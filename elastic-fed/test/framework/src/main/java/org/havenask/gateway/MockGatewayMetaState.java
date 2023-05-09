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

package org.havenask.gateway;

import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.Manifest;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataIndexUpgradeService;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.collect.Tuple;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.BigArrays;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.env.NodeEnvironment;
import org.havenask.plugins.MetadataUpgrader;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link GatewayMetaState} constructor accepts a lot of arguments.
 * It's not always easy / convenient to construct these dependencies.
 * This class constructor takes far fewer dependencies and constructs usable {@link GatewayMetaState} with 2 restrictions:
 * no metadata upgrade will be performed and no cluster state updaters will be run. This is sufficient for most of the tests.
 */
public class MockGatewayMetaState extends GatewayMetaState {
    private final DiscoveryNode localNode;
    private final BigArrays bigArrays;

    public MockGatewayMetaState(DiscoveryNode localNode, BigArrays bigArrays) {
        this.localNode = localNode;
        this.bigArrays = bigArrays;
    }

    @Override
    Metadata upgradeMetadataForNode(Metadata metadata, MetadataIndexUpgradeService metadataIndexUpgradeService,
                                    MetadataUpgrader metadataUpgrader) {
        // Metadata upgrade is tested in GatewayMetaStateTests, we override this method to NOP to make mocking easier
        return metadata;
    }

    @Override
    ClusterState prepareInitialClusterState(TransportService transportService, ClusterService clusterService, ClusterState clusterState) {
        // Just set localNode here, not to mess with ClusterService and IndicesService mocking
        return ClusterStateUpdaters.setLocalNode(clusterState, localNode);
    }

    public void start(Settings settings, NodeEnvironment nodeEnvironment, NamedXContentRegistry xContentRegistry) {
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(mock(ThreadPool.class));
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        final MetaStateService metaStateService = mock(MetaStateService.class);
        try {
            when(metaStateService.loadFullState()).thenReturn(new Tuple<>(Manifest.empty(), Metadata.builder().build()));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        start(settings, transportService, clusterService, metaStateService,
            null, null, new PersistedClusterStateService(nodeEnvironment, xContentRegistry, bigArrays,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L));
    }
}
