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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.HavenaskExecutors;
import org.havenask.discovery.DiscoveryModule;
import org.havenask.engine.index.config.ZoneBiz;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.util.Utils;
import org.havenask.env.Environment;
import org.havenask.env.ShardLock;
import org.havenask.env.TestEnvironment;
import org.havenask.index.IndexSettings;
import org.havenask.index.shard.ShardId;
import org.havenask.test.DummyShardLock;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.ThreadPool;

import static org.havenask.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.BIZ_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DEFAULT_BIZ_CONFIG;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DEFAULT_DIR;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.TABLE_DIR;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HavenaskEngineEnvironmentTests extends HavenaskTestCase {
    // test testDeleteIndexDirectoryUnderLock
    public void testDeleteIndexDirectoryUnderLock() throws IOException {
        ShardId shardId = new ShardId("indexFile", "indexFile", 0);
        String tableName = Utils.getHavenaskTableName(shardId);
        Path workDir = createTempDir();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), workDir.toString())
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .build();
        Path indexFile = workDir.resolve("data")
            .resolve(HavenaskEngineEnvironment.DEFAULT_DATA_PATH)
            .resolve(HavenaskEngineEnvironment.HAVENASK_RUNTIMEDATA_PATH)
            .resolve(tableName);
        Files.createDirectories(indexFile);
        TestCase.assertTrue(Files.exists(indexFile));

        Path configPath = workDir.resolve("data")
            .resolve(HavenaskEngineEnvironment.DEFAULT_DATA_PATH)
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
        IndexMetadata build = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(settings))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = HavenaskExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);

        MetaDataSyncer metaDataSyncer = mock(MetaDataSyncer.class);
        when(metaDataSyncer.getThreadPool()).thenReturn(threadPool);
        TargetInfo targetInfo = new TargetInfo();
        targetInfo.table_info = new HashMap<>();
        when(metaDataSyncer.getSearcherTargetInfo()).thenReturn(targetInfo);
        when(metaDataSyncer.getIndexLock(tableName)).thenReturn(new ReentrantLock());

        havenaskEngineEnvironment.setMetaDataSyncer(metaDataSyncer);

        ShardLock shardLock = new DummyShardLock(shardId);
        havenaskEngineEnvironment.deleteIndexDirectoryUnderLock(
            shardLock.getShardId().getIndex(),
            new IndexSettings(build, Settings.EMPTY)
        );

        TestCase.assertFalse(Files.exists(indexFile));
    }
}
