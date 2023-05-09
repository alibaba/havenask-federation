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

package org.havenask.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionRequest;
import org.havenask.action.admin.cluster.node.stats.NodesStatsAction;
import org.havenask.action.admin.indices.stats.IndicesStatsAction;
import org.havenask.action.support.ActionFilter;
import org.havenask.action.support.ActionFilters;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Strings;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.index.IndexService;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.store.Store;
import org.havenask.indices.IndicesService;
import org.havenask.indices.SystemIndexDescriptor;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.SystemIndexPlugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.InternalTestCluster;
import org.havenask.test.transport.MockTransportService;
import org.havenask.transport.TransportService;

import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableSet;
import static org.havenask.common.util.set.Sets.newHashSet;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for the ClusterInfoService collecting information
 */
@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterInfoServiceIT extends HavenaskIntegTestCase {

    private static final String TEST_SYSTEM_INDEX_NAME = ".test-cluster-info-system-index";

    public static class TestPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {

        private final BlockingActionFilter blockingActionFilter;

        public TestPlugin() {
            blockingActionFilter = new BlockingActionFilter();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(blockingActionFilter);
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(TEST_SYSTEM_INDEX_NAME,
                "System index for [" + getTestClass().getName() + ']'));
        }
    }

    public static class BlockingActionFilter extends org.havenask.action.support.ActionFilter.Simple {
        private Set<String> blockedActions = emptySet();

        @Override
        protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
            if (blockedActions.contains(action)) {
                throw new HavenaskException("force exception on [" + action + "]");
            }
            return true;
        }

        @Override
        public int order() {
            return 0;
        }

        public void blockActions(String... actions) {
            blockedActions = unmodifiableSet(newHashSet(actions));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TestPlugin.class, MockTransportService.TestPlugin.class);
    }

    private void setClusterInfoTimeout(String timeValue) {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), timeValue).build()));
    }

    public void testClusterInfoServiceCollectsInformation() {
        internalCluster().startNodes(2);

        final String indexName = randomBoolean() ? randomAlphaOfLength(5).toLowerCase(Locale.ROOT) : TEST_SYSTEM_INDEX_NAME;
        assertAcked(prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), 0)
                .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
                .put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean())
                .build()));
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose(indexName));
        }
        ensureGreen(indexName);
        InternalTestCluster internalTestCluster = internalCluster();
        // Get the cluster info service on the master node
        final InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster
            .getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        infoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        ClusterInfo info = infoService.refresh();
        assertNotNull("info should not be null", info);
        ImmutableOpenMap<String, DiskUsage> leastUsages = info.getNodeLeastAvailableDiskUsages();
        ImmutableOpenMap<String, DiskUsage> mostUsages = info.getNodeMostAvailableDiskUsages();
        ImmutableOpenMap<String, Long> shardSizes = info.shardSizes;
        assertNotNull(leastUsages);
        assertNotNull(shardSizes);
        assertThat("some usages are populated", leastUsages.values().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", shardSizes.values().size(), greaterThan(0));
        for (ObjectCursor<DiskUsage> usage : leastUsages.values()) {
            logger.info("--> usage: {}", usage.value);
            assertThat("usage has be retrieved", usage.value.getFreeBytes(), greaterThan(0L));
        }
        for (ObjectCursor<DiskUsage> usage : mostUsages.values()) {
            logger.info("--> usage: {}", usage.value);
            assertThat("usage has be retrieved", usage.value.getFreeBytes(), greaterThan(0L));
        }
        for (ObjectCursor<Long> size : shardSizes.values()) {
            logger.info("--> shard size: {}", size.value);
            assertThat("shard size is greater than 0", size.value, greaterThanOrEqualTo(0L));
        }
        ClusterService clusterService = internalTestCluster.getInstance(ClusterService.class, internalTestCluster.getMasterName());
        ClusterState state = clusterService.state();
        for (ShardRouting shard : state.routingTable().allShards()) {
            String dataPath = info.getDataPath(shard);
            assertNotNull(dataPath);

            String nodeId = shard.currentNodeId();
            DiscoveryNode discoveryNode = state.getNodes().get(nodeId);
            IndicesService indicesService = internalTestCluster.getInstance(IndicesService.class, discoveryNode.getName());
            IndexService indexService = indicesService.indexService(shard.index());
            IndexShard indexShard = indexService.getShardOrNull(shard.id());
            assertEquals(indexShard.shardPath().getRootDataPath().toString(), dataPath);

            assertTrue(info.getReservedSpace(nodeId, dataPath).containsShardId(shard.shardId()));
        }
    }

    public void testClusterInfoServiceInformationClearOnError() {
        internalCluster().startNodes(2,
            // manually control publishing
            Settings.builder().put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), "60m").build());
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)).get();
        ensureGreen("test");
        InternalTestCluster internalTestCluster = internalCluster();
        InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster
            .getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        // get one healthy sample
        ClusterInfo info = infoService.refresh();
        assertNotNull("failed to collect info", info);
        assertThat("some usages are populated", info.getNodeLeastAvailableDiskUsages().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", info.shardSizes.size(), greaterThan(0));


        MockTransportService mockTransportService = (MockTransportService) internalCluster()
            .getInstance(TransportService.class, internalTestCluster.getMasterName());

        final AtomicBoolean timeout = new AtomicBoolean(false);
        final Set<String> blockedActions = newHashSet(NodesStatsAction.NAME, NodesStatsAction.NAME + "[n]",
            IndicesStatsAction.NAME, IndicesStatsAction.NAME + "[n]");
        // drop all outgoing stats requests to force a timeout.
        for (DiscoveryNode node : internalTestCluster.clusterService().state().getNodes()) {
            mockTransportService.addSendBehavior(internalTestCluster.getInstance(TransportService.class, node.getName()),
                (connection, requestId, action, request, options) -> {
                    if (blockedActions.contains(action)) {
                        if (timeout.get()) {
                            logger.info("dropping [{}] to [{}]", action, node);
                            return;
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
        }

        setClusterInfoTimeout("1s");
        // timeouts shouldn't clear the info
        timeout.set(true);
        info = infoService.refresh();
        assertNotNull("info should not be null", info);
        // node info will time out both on the request level on the count down latch. this means
        // it is likely to update the node disk usage based on the one response that came be from local
        // node.
        assertThat(info.getNodeLeastAvailableDiskUsages().size(), greaterThanOrEqualTo(1));
        assertThat(info.getNodeMostAvailableDiskUsages().size(), greaterThanOrEqualTo(1));
        // indices is guaranteed to time out on the latch, not updating anything.
        assertThat(info.shardSizes.size(), greaterThan(1));

        // now we cause an exception
        timeout.set(false);
        ActionFilters actionFilters = internalTestCluster.getInstance(ActionFilters.class, internalTestCluster.getMasterName());
        BlockingActionFilter blockingActionFilter = null;
        for (ActionFilter filter : actionFilters.filters()) {
            if (filter instanceof BlockingActionFilter) {
                blockingActionFilter = (BlockingActionFilter) filter;
                break;
            }
        }

        assertNotNull("failed to find BlockingActionFilter", blockingActionFilter);
        blockingActionFilter.blockActions(blockedActions.toArray(Strings.EMPTY_ARRAY));
        info = infoService.refresh();
        assertNotNull("info should not be null", info);
        assertThat(info.getNodeLeastAvailableDiskUsages().size(), equalTo(0));
        assertThat(info.getNodeMostAvailableDiskUsages().size(), equalTo(0));
        assertThat(info.shardSizes.size(), equalTo(0));
        assertThat(info.reservedSpace.size(), equalTo(0));

        // check we recover
        blockingActionFilter.blockActions();
        setClusterInfoTimeout("15s");
        info = infoService.refresh();
        assertNotNull("info should not be null", info);
        assertThat(info.getNodeLeastAvailableDiskUsages().size(), equalTo(2));
        assertThat(info.getNodeMostAvailableDiskUsages().size(), equalTo(2));
        assertThat(info.shardSizes.size(), greaterThan(0));

        RoutingTable routingTable = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState().routingTable();
        for (ShardRouting shard : routingTable.allShards()) {
            assertTrue(info.getReservedSpace(shard.currentNodeId(), info.getDataPath(shard)).containsShardId(shard.shardId()));
        }

    }
}
