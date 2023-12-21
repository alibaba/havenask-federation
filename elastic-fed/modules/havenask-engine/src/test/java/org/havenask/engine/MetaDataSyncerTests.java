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

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.havenask.cluster.HavenaskAllocationTestCase.createAllocationService;
import static org.havenask.cluster.HavenaskAllocationTestCase.startInitializingShardsAndReroute;
import junit.framework.TestCase;
import org.havenask.Version;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.discovery.DiscoveryModule;
import org.havenask.engine.index.config.ZoneBiz;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;
import org.havenask.engine.util.Utils;
import org.havenask.env.Environment;
import org.havenask.env.TestEnvironment;
import org.havenask.index.shard.ShardId;
import org.havenask.test.HavenaskTestCase;
import org.junit.After;
import org.junit.Before;

import static org.havenask.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.BIZ_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DEFAULT_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DEFAULT_BIZ_CONFIG;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.TABLE_DIR;
import static org.havenask.engine.index.engine.EngineSettings.ENGINE_HAVENASK;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataSyncerTests extends HavenaskTestCase {
    private MetaDataSyncer metaDataSyncer;

    private ClusterState clusterState;
    private Path defaultRuntimeDataPath;

    private static final int TARGET_VERSION = 1651870394;
    private static final int DEFAULT_PART_COUNT = 1;
    private static final int DEFAULT_PART_ID = 0;
    private static final String INDEX_SUB_PATH = "generation_0/partition_0_65535";
    private static final String[] subDirNames = { "test", "in0" };
    private static final String[] cm2ConfigBizNames = { "general.default_sql", "general.test.write" };

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        String[] nodeIds = { "node1", "node2" };

        // generate ClusterState
        AllocationService strategy = createAllocationService(
            Settings.builder().put(SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey(), true).build()
        );

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT).put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        clusterState = ClusterState.builder(clusterState).nodes(createDiscoveryNodesBuilder(nodeIds)).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // generate clusterService
        ClusterService clusterService = mock(ClusterService.class);

        // generate HavenaskEngineEnvironment
        ShardId shardId = new ShardId("test", "test", 0);
        String tableName = Utils.getHavenaskTableName(shardId);
        Path workDir = createTempDir();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), workDir.toString())
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), ENGINE_HAVENASK)
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .build();
        Path indexFile = workDir.resolve(HavenaskEngineEnvironment.DEFAULT_DATA_PATH)
            .resolve(HavenaskEngineEnvironment.HAVENASK_RUNTIMEDATA_PATH)
            .resolve(tableName);
        Files.createDirectories(indexFile);
        TestCase.assertTrue(Files.exists(indexFile));

        Path configPath = workDir.resolve(HavenaskEngineEnvironment.DEFAULT_DATA_PATH)
            .resolve(HavenaskEngineEnvironment.HAVENASK_CONFIG_PATH);
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0"));
        Files.createDirectories(configPath.resolve(BIZ_DIR).resolve(DEFAULT_DIR).resolve("0"));
        Files.createDirectories(configPath.resolve(BIZ_DIR).resolve(DEFAULT_DIR).resolve("0").resolve("zones").resolve("general"));
        ZoneBiz zoneBiz = new ZoneBiz();
        Files.write(
            configPath.resolve(BIZ_DIR).resolve(DEFAULT_DIR).resolve("0").resolve(DEFAULT_BIZ_CONFIG),
            zoneBiz.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE
        );
        Environment environment = TestEnvironment.newEnvironment(settings);
        HavenaskEngineEnvironment havenaskEngineEnvironment = new HavenaskEngineEnvironment(environment, settings);

        // generate NativeProcessControlService
        NativeProcessControlService nativeProcessControlService = mock(NativeProcessControlService.class);

        when(nativeProcessControlService.getSearcherTcpPort()).thenReturn(39300);
        when(nativeProcessControlService.getSearcherGrpcPort()).thenReturn(39400);
        when(nativeProcessControlService.getQrsTcpPort()).thenReturn(49300);

        when(clusterService.getSettings()).thenReturn(settings);

        // generate metaDataSyncer
        metaDataSyncer = new MetaDataSyncer(clusterService, null, havenaskEngineEnvironment, nativeProcessControlService, null, null);
        this.clusterState = clusterState;
        // get defaultRuntimeDataPath
        defaultRuntimeDataPath = havenaskEngineEnvironment.getRuntimedataPath();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        metaDataSyncer.close();
    }

    public void testCreateQrsUpdateHeartbeatTargetRequest() throws Exception {
        Map<String, TargetInfo.ServiceInfo.Cm2Config> expectedCm2ConfigMap = generateExpectedCm2ConfigMap();

        UpdateHeartbeatTargetRequest qrsTargetRequest = metaDataSyncer.createQrsUpdateHeartbeatTargetRequest(clusterState);
        TargetInfo.ServiceInfo serviceInfo = qrsTargetRequest.getServiceInfo();

        assertEquals(TARGET_VERSION, qrsTargetRequest.getTargetVersion());
        assertEquals(false, qrsTargetRequest.getCleanDisk());
        assertEquals("qrs", serviceInfo.zone_name);
        assertEquals(DEFAULT_PART_COUNT, (int) serviceInfo.part_count);
        assertEquals(DEFAULT_PART_ID, (int) serviceInfo.part_id);

        assert (serviceInfo.cm2_config.containsKey("local"));
        List<TargetInfo.ServiceInfo.Cm2Config> cm2ConfigList = serviceInfo.cm2_config.get("local");
        assertEquals(cm2ConfigList.size(), expectedCm2ConfigMap.size());
        for (int i = 0; i < cm2ConfigBizNames.length; i++) {
            TargetInfo.ServiceInfo.Cm2Config curCm2Config = cm2ConfigList.get(i);
            TargetInfo.ServiceInfo.Cm2Config expectedCm2Config = expectedCm2ConfigMap.get(curCm2Config.biz_name + curCm2Config.ip);
            assertEquals(expectedCm2Config.biz_name, curCm2Config.biz_name);
            assertEquals((int) expectedCm2Config.part_count, (int) curCm2Config.part_count);
            assertEquals((int) expectedCm2Config.part_id, (int) curCm2Config.part_id);
            assertEquals((int) expectedCm2Config.tcp_port, (int) curCm2Config.tcp_port);
            assertEquals((int) expectedCm2Config.grpc_port, (int) curCm2Config.grpc_port);
            assertEquals(true, curCm2Config.support_heartbeat);
        }
    }

    public void testCreateSearcherUpdateHeartbeatTargetRequest() throws Exception {
        for (String subDir : subDirNames) {
            Path versionPath = defaultRuntimeDataPath.resolve(subDir).resolve(INDEX_SUB_PATH);
            Path FilePath = versionPath.resolve("version." + randomIntBetween(0, 8));
            Files.createDirectories(versionPath);
            Files.createFile(FilePath);
        }
        UpdateHeartbeatTargetRequest searcherTargetRequest = metaDataSyncer.createSearcherUpdateHeartbeatTargetRequest(clusterState);

        TargetInfo.ServiceInfo serviceInfo = searcherTargetRequest.getServiceInfo();
        Map<String, Map<String, TargetInfo.TableInfo>> tableInfos = searcherTargetRequest.getTableInfo();

        assertEquals(TARGET_VERSION, searcherTargetRequest.getTargetVersion());
        assertEquals(false, searcherTargetRequest.getCleanDisk());
        assertEquals("general", serviceInfo.zone_name);
        assertEquals(DEFAULT_PART_COUNT, (int) serviceInfo.part_count);
        assertEquals(DEFAULT_PART_ID, (int) serviceInfo.part_id);
        assertEquals(0, (int) serviceInfo.version);

        assertEquals(subDirNames.length, tableInfos.size());
        for (int i = 0; i < subDirNames.length; i++) {
            assert (tableInfos.containsKey(subDirNames[i]));
            Map<String, TargetInfo.TableInfo> curTableInfoMap = tableInfos.get(subDirNames[i]);
            assert (curTableInfoMap.containsKey("0"));
            TargetInfo.TableInfo curTableInfo = curTableInfoMap.get("0");
            assertEquals("in0" != subDirNames[i] ? 1 : 0, (int) curTableInfo.table_mode);
            assertEquals("in0" != subDirNames[i] ? 2 : 3, (int) curTableInfo.table_type);
            assertEquals("in0" == subDirNames[i] ? 2 : 1, (int) curTableInfo.total_partition_count);
        }
    }

    private DiscoveryNodes.Builder createDiscoveryNodesBuilder(String[] nodeIds) throws Exception {
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        String[] hosts = { "127.0.0.1", "127.0.0.2" };
        List<Map<String, String>> attributesList = new ArrayList<>();
        List<Set<DiscoveryNodeRole>> rolesList = new ArrayList<>();

        for (int i = 0; i < nodeIds.length; i++) {
            attributesList.add(
                Map.of(
                    "havenask.searcher.http.port",
                    String.valueOf(39200 + i),
                    "havenask.searcher.tcp.port",
                    String.valueOf(39300 + i),
                    "havenask.searcher.grpc.port",
                    String.valueOf(39400 + i)
                )
            );
        }
        rolesList.add(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE));
        rolesList.add(Set.of(DiscoveryNodeRole.DATA_ROLE));
        rolesList.add(Set.of(DiscoveryNodeRole.DATA_ROLE));

        for (int i = 0; i < nodeIds.length; i++) {
            TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(hosts[i]), 9300);
            DiscoveryNode discoveryNode = new DiscoveryNode(nodeIds[i], transportAddress, attributesList.get(i), rolesList.get(i), null);
            nb.add(discoveryNode);
        }
        nb.localNodeId("node1");
        nb.masterNodeId("node1");
        return nb;
    }

    private Map<String, TargetInfo.ServiceInfo.Cm2Config> generateExpectedCm2ConfigMap() {
        Map<String, TargetInfo.ServiceInfo.Cm2Config> expectedCm2ConfigMap = new HashMap<>();
        String[] bizNames = { "general.default_sql", "general.default_sql", "general.test.write", "general.test.write" };
        String[] ips = { "127.0.0.2", "127.0.0.1", "127.0.0.2", "127.0.0.1" };
        int[] grpcPorts = { 39401, 39400, 39401, 39400 };
        int[] tcpPorts = { 39301, 39300, 39301, 39300 };
        int[] partCounts = { 2, 2, 1, 1 };
        int[] partIds = { 0, 1, 0, 0 };
        for (int i = 0; i < bizNames.length; i++) {
            TargetInfo.ServiceInfo.Cm2Config curCm2Config = new TargetInfo.ServiceInfo.Cm2Config();
            curCm2Config.biz_name = bizNames[i];
            curCm2Config.ip = ips[i];
            curCm2Config.grpc_port = grpcPorts[i];
            curCm2Config.tcp_port = tcpPorts[i];
            curCm2Config.part_count = partCounts[i];
            curCm2Config.part_id = partIds[i];
            curCm2Config.support_heartbeat = true;

            expectedCm2ConfigMap.put(bizNames[i] + ips[i], curCm2Config);
        }
        return expectedCm2ConfigMap;
    }
}
