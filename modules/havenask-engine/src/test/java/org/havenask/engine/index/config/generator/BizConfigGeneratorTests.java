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

import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.index.IndexSettings;
import org.havenask.index.codec.CodecService;
import org.havenask.index.engine.EngineConfig;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperServiceTestCase;
import org.havenask.index.shard.ShardId;

import static org.havenask.engine.index.config.generator.BizConfigGenerator.BIZ_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.CLUSTER_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.CLUSTER_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DATA_TABLES_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DATA_TABLES_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.SCHEMAS_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.SCHEMAS_FILE_SUFFIX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BizConfigGeneratorTests extends MapperServiceTestCase {
    public void testBasic() throws IOException {
        String indexName = randomAlphaOfLength(5);
        IndexMetadata build = IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings settings = new IndexSettings(build, Settings.EMPTY);
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        EngineConfig engineConfig = mock(EngineConfig.class);
        CodecService codecService = mock(CodecService.class);

        when(codecService.getMapperService()).thenReturn(mapperService);
        when(engineConfig.getShardId()).thenReturn(new ShardId(new Index(indexName, randomAlphaOfLength(5)), 0));
        when(engineConfig.getCodecService()).thenReturn(codecService);
        when(engineConfig.getIndexSettings()).thenReturn(settings);
        Path configPath = createTempDir();
        Files.createDirectories(configPath.resolve(BIZ_DIR).resolve("0").resolve(CLUSTER_DIR));
        Files.createDirectories(configPath.resolve(BIZ_DIR).resolve("0").resolve(SCHEMAS_DIR));
        Files.createDirectories(configPath.resolve(BIZ_DIR).resolve("0").resolve(DATA_TABLES_DIR));
        BizConfigGenerator bizConfigGenerator = new BizConfigGenerator(engineConfig, configPath);
        bizConfigGenerator.generate();

        {
            Path clusterConfigPath = configPath.resolve(BIZ_DIR).resolve("0").resolve(CLUSTER_DIR).resolve(indexName + CLUSTER_FILE_SUFFIX);
            assertTrue(Files.exists(clusterConfigPath));
            String content = Files.readString(clusterConfigPath);
            String expect = String.format(
                Locale.ROOT,
                "{\n"
                    + "\t\"build_option_config\":{\n"
                    + "\t\t\"async_build\":true,\n"
                    + "\t\t\"async_queue_size\":1000,\n"
                    + "\t\t\"document_filter\":true,\n"
                    + "\t\t\"max_recover_time\":30,\n"
                    + "\t\t\"sort_build\":false,\n"
                    + "\t\t\"sort_descriptions\":[],\n"
                    + "\t\t\"sort_queue_mem\":4096,\n"
                    + "\t\t\"sort_queue_size\":10000000\n"
                    + "\t},\n"
                    + "\t\"cluster_config\":{\n"
                    + "\t\t\"builder_rule_config\":{\n"
                    + "\t\t\t\"batch_mode\":false,\n"
                    + "\t\t\t\"build_parallel_num\":1,\n"
                    + "\t\t\t\"merge_parallel_num\":1,\n"
                    + "\t\t\t\"partition_count\":1\n"
                    + "\t\t},\n"
                    + "\t\t\"cluster_name\":\"%s\",\n"
                    + "\t\t\"hash_mode\":{\n"
                    + "\t\t\t\"hash_field\":\"_id\",\n"
                    + "\t\t\t\"hash_function\":\"HASH\"\n"
                    + "\t\t},\n"
                    + "\t\t\"table_name\":\"%s\"\n"
                    + "\t},\n"
                    + "\t\"offline_index_config\":{\n"
                    + "\t\t\"build_config\":{\n"
                    + "\t\t\t\"build_total_memory\":5120,\n"
                    + "\t\t\t\"keep_version_count\":40\n"
                    + "\t\t}\n"
                    + "\t}\n"
                    + "}",
                indexName,
                indexName
            );

            assertEquals(expect, content);
        }

        {
            Path schemaConfigPath = configPath.resolve(BIZ_DIR).resolve("0").resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);
            assertTrue(Files.exists(schemaConfigPath));
            String content = Files.readString(schemaConfigPath);
            String expect = String.format(
                Locale.ROOT,
                "{\n"
                    + "\t\"attributes\":[\"_seq_no\",\"field\",\"_id\",\"_version\",\"_primary_term\"],\n"
                    + "\t\"fields\":[{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_routing\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_seq_no\",\n"
                    + "\t\t\"field_type\":\"INT64\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"field\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_source\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_id\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_version\",\n"
                    + "\t\t\"field_type\":\"INT64\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_primary_term\",\n"
                    + "\t\t\"field_type\":\"INT64\"\n"
                    + "\t}],\n"
                    + "\t\"indexs\":[{\n"
                    + "\t\t\"index_fields\":\"_routing\",\n"
                    + "\t\t\"index_name\":\"_routing\",\n"
                    + "\t\t\"index_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"index_fields\":\"_seq_no\",\n"
                    + "\t\t\"index_name\":\"_seq_no\",\n"
                    + "\t\t\"index_type\":\"NUMBER\"\n"
                    + "\t},{\n"
                    + "\t\t\"index_fields\":\"field\",\n"
                    + "\t\t\"index_name\":\"field\",\n"
                    + "\t\t\"index_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"has_primary_key_attribute\":true,\n"
                    + "\t\t\"index_fields\":\"_id\",\n"
                    + "\t\t\"index_name\":\"_id\",\n"
                    + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                    + "\t\t\"is_primary_key_sorted\":false\n"
                    + "\t}],\n"
                    + "\t\"summarys\":{\n"
                    + "\t\t\"summary_fields\":[\"_routing\",\"_source\",\"_id\"]\n"
                    + "\t},\n"
                    + "\t\"table_name\":\"%s\",\n"
                    + "\t\"table_type\":\"normal\"\n"
                    + "}",
                indexName
            );
            assertEquals(expect, content);
        }

        {
            Path dataTablesPath = configPath.resolve(BIZ_DIR)
                .resolve("0")
                .resolve(DATA_TABLES_DIR)
                .resolve(indexName + DATA_TABLES_FILE_SUFFIX);
            assertTrue(Files.exists(dataTablesPath));
            String content = Files.readString(dataTablesPath);
            String expect = String.format(
                Locale.ROOT,
                "{\n"
                    + "\t\"data_descriptions\":[],\n"
                    + "\t\"processor_chain_config\":[\n"
                    + "\t\t{\n"
                    + "\t\t\t\"clusters\":[\n"
                    + "\t\t\t\t\"%s\"\n"
                    + "\t\t\t],\n"
                    + "\t\t\t\"document_processor_chain\":[\n"
                    + "\t\t\t\t{\n"
                    + "\t\t\t\t\t\"class_name\":\"TokenizeDocumentProcessor\",\n"
                    + "\t\t\t\t\t\"module_name\":\"\",\n"
                    + "\t\t\t\t\t\"parameters\":{}\n"
                    + "\t\t\t\t}\n"
                    + "\t\t\t],\n"
                    + "\t\t\t\"modules\":[]\n"
                    + "\t\t}\n"
                    + "\t],\n"
                    + "\t\"processor_config\":{\n"
                    + "\t\t\"processor_queue_size\":2000,\n"
                    + "\t\t\"processor_thread_num\":30\n"
                    + "\t},\n"
                    + "\t\"processor_rule_config\":{\n"
                    + "\t\t\"parallel_num\":1,\n"
                    + "\t\t\"partition_count\":1\n"
                    + "\t}\n"
                    + "}",
                indexName
            );
            assertEquals(expect, content);
        }
    }
}
