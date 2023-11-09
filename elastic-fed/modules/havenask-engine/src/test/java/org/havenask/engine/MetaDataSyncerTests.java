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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.RoutingNodes;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.Settings;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataSyncerTests extends HavenaskTestCase {
    private MetaDataSyncer metaDataSyncer;

    private ClusterState clusterState;
    private Path defaultRuntimeDataPath;

    private static final int TARGET_VERSION = 1651870394;
    private static final int DEFAULT_PART_COUNT = 1;
    private static final int DEFAULT_PART_ID = 0;
    private static final int DEFAULT_SEARCHER_TCP_PORT = 39300;
    private static final int DEFAUlT_SEARCHER_GRPC_PORT = 39400;
    private static final String INDEX_SUB_PATH = "generation_0/partition_0_65535";
    private static final String[] subDirNames = { "test1", "test2", "test3", "test4", "in0" };
    private static final String[] cm2ConfigBizNames = {
        "general.para_search_2",
        "general.para_search_2.search",
        "general.default_sql",
        "general.para_search_4",
        "general.default.search",
        "general.default_agg",
        "general.default_agg.search",
        "general.default",
        "general.para_search_4.search" };

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        String[] indexNames = { "test1", "test2", "test3", "test4", "in0" };
        int indexCount = 5;

        // generate clusterService
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        RoutingNodes routingNodes = mock(RoutingNodes.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        RoutingNode routingNode = mock(RoutingNode.class);

        when(state.getRoutingNodes()).thenReturn(routingNodes);
        when(state.nodes()).thenReturn(nodes);
        when(nodes.getLocalNodeId()).thenReturn("localNodeId");
        when(routingNodes.node("localNodeId")).thenReturn(routingNode);

        ShardId[] shardIds = new ShardId[indexCount];
        for (int i = 0; i < shardIds.length; i++) {
            shardIds[i] = mock(ShardId.class);
            when(shardIds[i].getIndexName()).thenReturn(indexNames[i]);
        }

        StreamInput in = mock(StreamInput.class);
        when(in.readByte()).thenReturn((byte) 1);
        ShardRouting[] shardRoutings = new ShardRouting[indexCount];
        for (int i = 0; i < shardRoutings.length; i++) {
            shardRoutings[i] = new ShardRouting(shardIds[i], in);
        }

        List<ShardRouting> listShardRoutings = new ArrayList<>();
        for (int i = 0; i < shardRoutings.length; i++) {
            listShardRoutings.add(shardRoutings[i]);
        }
        when(routingNode.iterator()).thenReturn(listShardRoutings.iterator());

        // generate HavenaskEngineEnvironment
        ShardId shardId = new ShardId("indexFile", "indexFile", 0);
        String tableName = Utils.getHavenaskTableName(shardId);
        Path workDir = createTempDir();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), workDir.toString())
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
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
        this.clusterState = state;
        // get defaultRuntimeDataPath
        defaultRuntimeDataPath = havenaskEngineEnvironment.getRuntimedataPath();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        metaDataSyncer.close();
    }

    @AwaitsFix(bugUrl = "https://github.com/alibaba/havenask-federation/issues/256")
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

    @AwaitsFix(bugUrl = "https://github.com/alibaba/havenask-federation/issues/256")
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
            assertEquals(1, (int) curTableInfo.total_partition_count);
        }
    }
}
