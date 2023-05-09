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
import java.nio.file.Files;
import java.nio.file.Path;

import junit.framework.TestCase;
import org.havenask.common.settings.Settings;
import org.havenask.discovery.DiscoveryModule;
import org.havenask.env.Environment;
import org.havenask.env.TestEnvironment;
import org.havenask.index.Index;
import org.havenask.index.shard.ShardId;
import org.havenask.test.DummyShardLock;
import org.havenask.test.HavenaskTestCase;

import static org.havenask.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;

public class HavenaskEngineEnvironmentTests extends HavenaskTestCase {
    // test deleteIndexDirectoryUnderLock
    public void testDeleteIndexDirectoryUnderLock() throws IOException {
        Path workDir = createTempDir();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), workDir.toString())
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .build();
        Path indexFile = workDir.resolve(HavenaskEngineEnvironment.DEFAULT_DATA_PATH)
            .resolve(HavenaskEngineEnvironment.HAVENASK_RUNTIMEDATA_PATH)
            .resolve("indexFile");
        Files.createDirectories(indexFile);
        TestCase.assertTrue(Files.exists(indexFile));
        Environment environment = TestEnvironment.newEnvironment(settings);
        HavenaskEngineEnvironment havenaskEngineEnvironment = new HavenaskEngineEnvironment(environment, settings);
        havenaskEngineEnvironment.deleteIndexDirectoryUnderLock(new Index("indexFile", "indexFile"), null);
        TestCase.assertFalse(Files.exists(indexFile));
    }

    // test deleteShardDirectoryUnderLock
    public void testDeleteShardDirectoryUnderLock() throws IOException {
        Path workDir = createTempDir();
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), workDir.toString())
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .build();
        Path indexFile = workDir.resolve(HavenaskEngineEnvironment.DEFAULT_DATA_PATH)
            .resolve(HavenaskEngineEnvironment.HAVENASK_RUNTIMEDATA_PATH)
            .resolve("indexFile");
        Files.createDirectories(indexFile);
        TestCase.assertTrue(Files.exists(indexFile));
        Environment environment = TestEnvironment.newEnvironment(settings);
        HavenaskEngineEnvironment havenaskEngineEnvironment = new HavenaskEngineEnvironment(environment, settings);
        havenaskEngineEnvironment.deleteShardDirectoryUnderLock(new DummyShardLock(new ShardId("indexFile", "indexFile", 0)), null);
        TestCase.assertFalse(Files.exists(indexFile));
    }

}
