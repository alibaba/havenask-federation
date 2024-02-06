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

package org.havenask.engine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.havenask.Version;
import org.havenask.cluster.ClusterModule;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.EmptyClusterInfoService;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.havenask.cluster.routing.allocation.decider.AllocationDecider;
import org.havenask.cluster.routing.allocation.decider.AllocationDeciders;
import org.havenask.cluster.routing.allocation.decider.HavenaskShardsLimitAllocationDecider;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.discovery.DiscoveryModule;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;
import org.havenask.engine.util.RangeUtil;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.env.TestEnvironment;
import org.havenask.test.gateway.TestGatewayAllocator;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.havenask.cluster.HavenaskAllocationTestCase.SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES;
import static org.havenask.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.havenask.cluster.node.DiscoveryNodeRole.INGEST_ROLE;
import static org.havenask.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.havenask.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.havenask.engine.util.RangeUtil.MAX_PARTITION_RANGE;
import static org.havenask.engine.util.RangeUtil.splitRange;

public class MetaDataSyncerTests extends HavenaskAllocationTestCase {
    private MetaDataSyncer metaDataSyncer;
    private ThreadPool threadPool;
    private ClusterState clusterState;
    private Path defaultRuntimeDataPath;

    private static final int TARGET_VERSION = 1651870394;
    private static final int DEFAULT_PART_COUNT = 1;
    private static final int DEFAULT_PART_ID = 0;
    private static final int DEFAULT_SEARCHER_TCP_PORT = 39300;
    private static final int DEFAUlT_SEARCHER_GRPC_PORT = 39400;
    private static final String GENETATION_PATH = "generation_0";
    private static final String[] indexNames = { "test1", "in0" };
    private static final String[] havenaskIndexNames = { "test1" };
    private static final String[] cm2ConfigBizNames = { "general.default_sql" };

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        // generate clusterService
        threadPool = new TestThreadPool(getTestName());
        Path workDir = createTempDir();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), workDir.toString())
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .build();
        Set<Setting<?>> buildInSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        buildInSettings.add(NativeProcessControlService.HAVENASK_COMMAND_TIMEOUT_SETTING);
        ClusterService clusterService = new ClusterService(settings, new ClusterSettings(settings, buildInSettings), threadPool);

        // generate clusterState
        Set<Setting<?>> clusterSetSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSetSettings.add(HavenaskShardsLimitAllocationDecider.CLUSTER_TOTAL_HAVENASK_SHARDS_PER_NODE_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSetSettings);
        List<AllocationDecider> deciders = new ArrayList<>(ClusterModule.createAllocationDeciders(settings, clusterSettings, emptyList()));
        Collections.shuffle(deciders, random());
        AllocationService strategy = new HavenaskAllocationTestCase.MockAllocationService(
            new AllocationDeciders(deciders),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );

        final DiscoveryNode localNode = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DATA_ROLE, INGEST_ROLE, MASTER_ROLE),
            Version.CURRENT
        );

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        settings(Version.CURRENT).put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 7)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test1")).build();

        ClusterState state = ClusterState.builder(new ClusterName(MetaDataSyncerTests.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .metadata(metadata)
            .routingTable(routingTable)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        state = strategy.reroute(state, "reroute");
        state = startInitializingShardsAndReroute(strategy, state);

        state = strategy.reroute(state, "reroute");
        // generate HavenaskEngineEnvironment
        Environment environment = TestEnvironment.newEnvironment(settings);
        HavenaskEngineEnvironment havenaskEngineEnvironment = new HavenaskEngineEnvironment(environment, settings);

        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment)) {
            // generate NativeProcessControlService
            NativeProcessControlService nativeProcessControlService = new MockNativeProcessControlService(
                null,
                clusterService,
                threadPool,
                environment,
                nodeEnvironment,
                new HavenaskEngineEnvironment(environment, settings)
            );

            // generate metaDataSyncer
            metaDataSyncer = new MetaDataSyncer(clusterService, null, havenaskEngineEnvironment, nativeProcessControlService, null, null);
            this.clusterState = state;

            // get defaultRuntimeDataPath
            defaultRuntimeDataPath = havenaskEngineEnvironment.getRuntimedataPath();
        }
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        threadPool = null;
        metaDataSyncer.close();
    }

    public void testCreateQrsUpdateHeartbeatTargetRequest() throws Exception {
        UpdateHeartbeatTargetRequest qrsTargetRequest = metaDataSyncer.createQrsUpdateHeartbeatTargetRequest(clusterState);
        TargetInfo.ServiceInfo serviceInfo = qrsTargetRequest.getServiceInfo();

        assertEquals(TARGET_VERSION, qrsTargetRequest.getTargetVersion());
        assertEquals(false, qrsTargetRequest.getCleanDisk());
        assertEquals("qrs", serviceInfo.zone_name);
        assertEquals(DEFAULT_PART_COUNT, (int) serviceInfo.part_count);
        assertEquals(DEFAULT_PART_ID, (int) serviceInfo.part_id);

        assert (serviceInfo.cm2_config.containsKey("local"));
        List<TargetInfo.ServiceInfo.Cm2Config> cm2ConfigList = serviceInfo.cm2_config.get("local");
        assertEquals(cm2ConfigBizNames.length, cm2ConfigList.size());
        for (int i = 0; i < cm2ConfigBizNames.length; i++) {
            TargetInfo.ServiceInfo.Cm2Config curCm2Config = cm2ConfigList.get(i);
            assertEquals(cm2ConfigBizNames[i], curCm2Config.biz_name);
            assertEquals(DEFAULT_PART_COUNT, (int) curCm2Config.part_count);
            assertEquals(DEFAULT_PART_ID, (int) curCm2Config.part_id);
            assertEquals(DEFAULT_SEARCHER_TCP_PORT, (int) curCm2Config.tcp_port);
            assertEquals(DEFAUlT_SEARCHER_GRPC_PORT, (int) curCm2Config.grpc_port);
            assertEquals(true, curCm2Config.support_heartbeat);
        }
    }

    public void testCreateSearcherUpdateHeartbeatTargetRequest() throws Exception {
        for (String indexName : havenaskIndexNames) {
            int shardCount = clusterState.metadata().index(indexName).getNumberOfShards();
            List<RangeUtil.PartitionRange> vec = splitRange(0, MAX_PARTITION_RANGE, shardCount);
            for (int i = 0; i < shardCount; i++) {
                String partition = String.format(Locale.ROOT, "partition_%d_%d", vec.get(i).first, vec.get(i).second);
                Path versionPath = defaultRuntimeDataPath.resolve(indexName).resolve(GENETATION_PATH).resolve(partition);
                Path FilePath = versionPath.resolve("version." + randomIntBetween(0, 8));
                Files.createDirectories(versionPath);
                Files.createFile(FilePath);
            }
        }
        UpdateHeartbeatTargetRequest searcherTargetRequest = metaDataSyncer.createSearcherUpdateHeartbeatTargetRequest(clusterState);

        TargetInfo.ServiceInfo serviceInfo = searcherTargetRequest.getServiceInfo();
        Map<String, Map<String, TargetInfo.TableInfo>> tableInfos = searcherTargetRequest.getTableInfo();

        TargetInfo.TableGroup havenaskTableGroup = searcherTargetRequest.getTargetInfo().table_groups.get("general.table_group.test1");
        assertEquals(false, havenaskTableGroup.broadcast);
        assertEquals(1, havenaskTableGroup.table_names.size());
        assertEquals("test1", havenaskTableGroup.table_names.get(0));
        assertEquals(3, havenaskTableGroup.unpublish_part_ids.size());

        assertEquals(TARGET_VERSION, searcherTargetRequest.getTargetVersion());
        assertEquals(false, searcherTargetRequest.getCleanDisk());
        assertEquals("general", serviceInfo.zone_name);
        assertEquals(DEFAULT_PART_COUNT, (int) serviceInfo.part_count);
        assertEquals(DEFAULT_PART_ID, (int) serviceInfo.part_id);
        assertEquals(0, (int) serviceInfo.version);

        assertEquals(indexNames.length, tableInfos.size());
        for (int i = 0; i < indexNames.length; i++) {
            assert (tableInfos.containsKey(indexNames[i]));
            Map<String, TargetInfo.TableInfo> curTableInfoMap = tableInfos.get(indexNames[i]);
            assert (curTableInfoMap.containsKey("0"));
            TargetInfo.TableInfo curTableInfo = curTableInfoMap.get("0");
            assertEquals("in0" != indexNames[i] ? 1 : 0, (int) curTableInfo.table_mode);
            assertEquals("in0" != indexNames[i] ? 2 : 3, (int) curTableInfo.table_type);
        }
    }
}
