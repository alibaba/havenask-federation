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

package org.havenask.engine.index.config.generator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import org.havenask.index.Index;
import org.havenask.index.engine.EngineConfig;
import org.havenask.index.shard.ShardId;
import org.havenask.test.HavenaskTestCase;

import static org.havenask.engine.index.config.generator.TableConfigGenerator.CLUSTER_DIR;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.CLUSTER_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.TABLE_DIR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableConfigGeneratorTests extends HavenaskTestCase {
    public void testBasic() throws IOException {
        String indexName = randomAlphaOfLength(5);
        EngineConfig engineConfig = mock(EngineConfig.class);
        when(engineConfig.getShardId()).thenReturn(new ShardId(new Index(indexName, randomAlphaOfLength(5)), 0));
        Path configPath = createTempDir();
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(CLUSTER_DIR));
        TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(engineConfig, configPath);
        tableConfigGenerator.generate();

        Path clusterConfigPath = configPath.resolve(TABLE_DIR).resolve("0").resolve(CLUSTER_DIR).resolve(indexName + CLUSTER_FILE_SUFFIX);
        assertTrue(Files.exists(clusterConfigPath));
        String content = Files.readString(clusterConfigPath);
        String expect = String.format(
            Locale.ROOT,
            "{\n"
                + "\t\"build_option_config\":{\n"
                + "\t\t\"async_build\":true,\n"
                + "\t\t\"max_recover_time\":3\n"
                + "\t},\n"
                + "\t\"cluster_config\":{\n"
                + "\t\t\"build_in_mem\":false,\n"
                + "\t\t\"builder_rule_config\":{\n"
                + "\t\t\t\"partition_count\":1\n"
                + "\t\t},\n"
                + "\t\t\"cluster_name\":\"%s\",\n"
                + "\t\t\"hash_mode\":{\n"
                + "\t\t\t\"hash_field\":\"id\",\n"
                + "\t\t\t\"hash_function\":\"HASH\"\n"
                + "\t\t},\n"
                + "\t\t\"table_name\":\"%s\"\n"
                + "\t},\n"
                + "\t\"offline_index_config\":{\n"
                + "\t\t\n"
                + "\t},\n"
                + "\t\"online_index_config\":{\n"
                + "\t\t\"build_config\":{\n"
                + "\t\t\t\"build_total_memory\":5120,\n"
                + "\t\t\t\"dump_thread_count\":8,\n"
                + "\t\t\t\"max_doc_count\":0\n"
                + "\t\t},\n"
                + "\t\t\"enable_async_dump_segment\":false,\n"
                + "\t\t\"load_remain_flush_realtime_index\":false,\n"
                + "\t\t\"max_realtime_memory_use\":800,\n"
                + "\t\t\"on_disk_flush_realtime_index\":false\n"
                + "\t}\n"
                + "}",
            indexName,
            indexName
        );
        assertEquals(expect, content);
    }
}
