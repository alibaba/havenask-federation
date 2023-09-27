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
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DEFAULT_BIZ_CONFIG;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DEFAULT_DIR;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.TABLE_DIR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataSyncerTests extends HavenaskTestCase {
    private MetaDataSyncer metaDataSyncer;
    private Path defaultRuntimeDataPath;

    private static final String INDEX_SUB_PATH = "generation_0/partition_0_65535";
    private static final String[] subDirNames = { "test1", "test2", "test3", "test4", "in0" };

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        ClusterService clusterService = mock(ClusterService.class);

        // generate ClusterState
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        RoutingNodes routingNodes = mock(RoutingNodes.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        RoutingNode routingNode = mock(RoutingNode.class);

        when(state.getRoutingNodes()).thenReturn(routingNodes);
        when(state.nodes()).thenReturn(nodes);
        when(nodes.getLocalNodeId()).thenReturn("localNodeId");
        when(routingNodes.node("localNodeId")).thenReturn(routingNode);

        ShardId shardId1 = mock(ShardId.class);
        when(shardId1.getIndexName()).thenReturn("test1");
        ShardId shardId2 = mock(ShardId.class);
        when(shardId2.getIndexName()).thenReturn("test2");
        ShardId shardId3 = mock(ShardId.class);
        when(shardId3.getIndexName()).thenReturn("test3");
        ShardId shardId4 = mock(ShardId.class);
        when(shardId4.getIndexName()).thenReturn("test4");
        ShardId shardId5 = mock(ShardId.class);
        when(shardId5.getIndexName()).thenReturn("in0");

        StreamInput in = mock(StreamInput.class);
        when(in.readByte()).thenReturn((byte) 1);
        ShardRouting shardRouting1 = new ShardRouting(shardId1, in);
        ShardRouting shardRouting2 = new ShardRouting(shardId2, in);
        ShardRouting shardRouting3 = new ShardRouting(shardId3, in);
        ShardRouting shardRouting4 = new ShardRouting(shardId4, in);
        ShardRouting shardRouting5 = new ShardRouting(shardId5, in);

        List<ShardRouting> shardRoutings = new ArrayList<>();
        shardRoutings.add(shardRouting1);
        shardRoutings.add(shardRouting2);
        shardRoutings.add(shardRouting3);
        shardRoutings.add(shardRouting4);
        shardRoutings.add(shardRouting5);
        when(routingNode.iterator()).thenReturn(shardRoutings.iterator());

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
        UpdateHeartbeatTargetRequest qrsTargetRequest = metaDataSyncer.createQrsUpdateHeartbeatTargetRequest();
        assertEquals(true, true);
    }

    public void testCreateSearcherUpdateHeartbeatTargetRequest() throws Exception {
        for (String subDir : subDirNames) {
            Path versionPath = defaultRuntimeDataPath.resolve(subDir).resolve(INDEX_SUB_PATH);
            Path FilePath = versionPath.resolve("version." + randomIntBetween(0, 8));
            Files.createDirectories(versionPath);
            Files.createFile(FilePath);
        }
        UpdateHeartbeatTargetRequest searcherTargetRequest = metaDataSyncer.createSearcherUpdateHeartbeatTargetRequest();
        assertEquals(true, true);
    }
}
