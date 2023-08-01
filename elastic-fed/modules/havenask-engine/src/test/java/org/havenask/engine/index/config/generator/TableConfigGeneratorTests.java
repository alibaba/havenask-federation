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
import java.util.Collection;
import java.util.Locale;

import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperServiceTestCase;
import org.havenask.plugins.Plugin;

import static java.util.Collections.singletonList;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DATA_TABLES_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DATA_TABLES_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.SCHEMAS_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.SCHEMAS_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.CLUSTER_DIR;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.CLUSTER_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.TABLE_DIR;

public class TableConfigGeneratorTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new HavenaskEnginePlugin(Settings.EMPTY));
    }

    public void testBasic() throws IOException {
        String indexName = randomAlphaOfLength(5);
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        Path configPath = createTempDir();
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(CLUSTER_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(SCHEMAS_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(DATA_TABLES_DIR));
        TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(indexName, Settings.EMPTY, mapperService, configPath);
        tableConfigGenerator.generate();

        {
            Path clusterConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(CLUSTER_DIR)
                .resolve(indexName + CLUSTER_FILE_SUFFIX);
            assertTrue(Files.exists(clusterConfigPath));
            String content = Files.readString(clusterConfigPath);
            String expect = String.format(
                Locale.ROOT,
                "{\n"
                    + "\t\"build_option_config\":{\n"
                    + "\t\t\"async_build\":true,\n"
                    + "\t\t\"async_queue_size\":50000,\n"
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
                    + "\t\"direct_write\":true,\n"
                    + "\t\"offline_index_config\":{\n"
                    + "\t\t\"build_config\":{\n"
                    + "\t\t\t\"build_total_memory\":128,\n"
                    + "\t\t\t\"max_doc_count\":100000\n"
                    + "\t\t},\n"
                    + "\t\t\"merge_config\":{\n"
                    + "\t\t\t\"merge_strategy\":\"combined\"\n"
                    + "\t\t}\n"
                    + "\t},\n"
                    + "\t\"online_index_config\":{\n"
                    + "\t\t\"build_config\":{\n"
                    + "\t\t\t\"build_total_memory\":128,\n"
                    + "\t\t\t\"max_doc_count\":100000\n"
                    + "\t\t},\n"
                    + "\t\t\"enable_async_dump_segment\":true,\n"
                    + "\t\t\"max_realtime_dump_interval\":60,\n"
                    + "\t\t\"on_disk_flush_realtime_index\":true\n"
                    + "\t},\n"
                    + "\t\"realtime\":true,\n"
                    + "\t\"wal_config\":{\n"
                    + "\t\t\"sink\":{\n"
                    + "\t\t\t\"queue_name\":\"%s\",\n"
                    + "\t\t\t\"queue_size\":\"500000\"\n"
                    + "\t\t},\n"
                    + "\t\t\"strategy\":\"queue\",\n"
                    + "\t\t\"timeout_ms\":10000\n"
                    + "\t}\n"
                    + "}",
                indexName,
                indexName,
                indexName
            );
            assertEquals(expect, content);
        }

        {
            Path schemaConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(SCHEMAS_DIR)
                .resolve(indexName + SCHEMAS_FILE_SUFFIX);
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
            Path dataTablesPath = configPath.resolve(TABLE_DIR)
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

        tableConfigGenerator.remove();
        Path clusterConfigPath = configPath.resolve(TABLE_DIR)
            .resolve("0")
            .resolve(BizConfigGenerator.CLUSTER_DIR)
            .resolve(indexName + BizConfigGenerator.CLUSTER_FILE_SUFFIX);
        assertFalse(Files.exists(clusterConfigPath));

        Path schemaConfigPath = configPath.resolve(TABLE_DIR).resolve("0").resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);
        assertFalse(Files.exists(schemaConfigPath));

        Path dataTablesPath = configPath.resolve(TABLE_DIR)
            .resolve("0")
            .resolve(DATA_TABLES_DIR)
            .resolve(indexName + DATA_TABLES_FILE_SUFFIX);
        assertFalse(Files.exists(dataTablesPath));
    }

    public void testDupFieldProcessor() throws IOException {
        String indexName = randomAlphaOfLength(5);
        MapperService mapperService = createMapperService(mapping(b -> {
            {
                b.startObject("field");
                {
                    b.field("type", "dense_vector");
                    b.field("dims", 128);
                }
                b.endObject();
                b.startObject("field2");
                {
                    b.field("type", "dense_vector");
                    b.field("dims", 128);
                }
                b.endObject();
            }
        }));
        Path configPath = createTempDir();
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(CLUSTER_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(SCHEMAS_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(DATA_TABLES_DIR));
        TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(indexName, Settings.EMPTY, mapperService, configPath);
        tableConfigGenerator.generate();

        {
            Path clusterConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(CLUSTER_DIR)
                .resolve(indexName + CLUSTER_FILE_SUFFIX);
            assertTrue(Files.exists(clusterConfigPath));
            String content = Files.readString(clusterConfigPath);
            String expect = String.format(
                Locale.ROOT,
                "{\n"
                    + "\t\"build_option_config\":{\n"
                    + "\t\t\"async_build\":true,\n"
                    + "\t\t\"async_queue_size\":50000,\n"
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
                    + "\t\"direct_write\":true,\n"
                    + "\t\"offline_index_config\":{\n"
                    + "\t\t\"build_config\":{\n"
                    + "\t\t\t\"build_total_memory\":128,\n"
                    + "\t\t\t\"max_doc_count\":100000\n"
                    + "\t\t},\n"
                    + "\t\t\"merge_config\":{\n"
                    + "\t\t\t\"merge_strategy\":\"combined\"\n"
                    + "\t\t}\n"
                    + "\t},\n"
                    + "\t\"online_index_config\":{\n"
                    + "\t\t\"build_config\":{\n"
                    + "\t\t\t\"build_total_memory\":128,\n"
                    + "\t\t\t\"max_doc_count\":100000\n"
                    + "\t\t},\n"
                    + "\t\t\"enable_async_dump_segment\":true,\n"
                    + "\t\t\"max_realtime_dump_interval\":60,\n"
                    + "\t\t\"on_disk_flush_realtime_index\":true\n"
                    + "\t},\n"
                    + "\t\"realtime\":true,\n"
                    + "\t\"wal_config\":{\n"
                    + "\t\t\"sink\":{\n"
                    + "\t\t\t\"queue_name\":\"%s\",\n"
                    + "\t\t\t\"queue_size\":\"500000\"\n"
                    + "\t\t},\n"
                    + "\t\t\"strategy\":\"queue\",\n"
                    + "\t\t\"timeout_ms\":10000\n"
                    + "\t}\n"
                    + "}",
                indexName,
                indexName,
                indexName
            );
            assertEquals(expect, content);
        }

        {
            Path schemaConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(SCHEMAS_DIR)
                .resolve(indexName + SCHEMAS_FILE_SUFFIX);
            assertTrue(Files.exists(schemaConfigPath));
            String content = Files.readString(schemaConfigPath);
            String expect = String.format(
                Locale.ROOT,
                "{\n"
                    + "\t\"attributes\":[\"_seq_no\",\"_id\",\"_version\",\"_primary_term\"],\n"
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
                    + "\t\t\"field_name\":\"field2\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"DUP_id\",\n"
                    + "\t\t\"field_type\":\"RAW\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"DUP_field\",\n"
                    + "\t\t\"field_type\":\"RAW\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"DUP_field2\",\n"
                    + "\t\t\"field_type\":\"RAW\"\n"
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
                    + "\t\t\"index_fields\":[\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"DUP_id\"\n"
                    + "\t\t\t},\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"DUP_field\"\n"
                    + "\t\t\t}\n"
                    + "\t\t],\n"
                    + "\t\t\"index_name\":\"field\",\n"
                    + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                    + "\t\t\"indexer\":\"aitheta_indexer\",\n"
                    + "\t\t\"parameters\":{\n"
                    + "\t\t\t\"dimension\":\"128\",\n"
                    + "\t\t\t\"build_metric_type\":\"l2\",\n"
                    + "\t\t\t\"search_metric_type\":\"l2\",\n"
                    + "\t\t\t\"index_type\":\"graph\",\n"
                    + "\t\t\t\"proxima.graph.common.graph_type\":\"hnsw\"\n"
                    + "\t\t}\n"
                    + "\t},{\n"
                    + "\t\t\"has_primary_key_attribute\":true,\n"
                    + "\t\t\"index_fields\":\"_id\",\n"
                    + "\t\t\"index_name\":\"_id\",\n"
                    + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                    + "\t\t\"is_primary_key_sorted\":false\n"
                    + "\t},{\n"
                    + "\t\t\"index_fields\":[\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"DUP_id\"\n"
                    + "\t\t\t},\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"DUP_field2\"\n"
                    + "\t\t\t}\n"
                    + "\t\t],\n"
                    + "\t\t\"index_name\":\"field2\",\n"
                    + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                    + "\t\t\"indexer\":\"aitheta_indexer\",\n"
                    + "\t\t\"parameters\":{\n"
                    + "\t\t\t\"dimension\":\"128\",\n"
                    + "\t\t\t\"build_metric_type\":\"l2\",\n"
                    + "\t\t\t\"search_metric_type\":\"l2\",\n"
                    + "\t\t\t\"index_type\":\"graph\",\n"
                    + "\t\t\t\"proxima.graph.common.graph_type\":\"hnsw\"\n"
                    + "\t\t}\n"
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
            Path dataTablesPath = configPath.resolve(TABLE_DIR)
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
                    + "\t\t\t\t},\n"
                    + "\t\t\t\t{\n"
                    + "\t\t\t\t\t\"class_name\":\"DupFieldProcessor\",\n"
                    + "\t\t\t\t\t\"module_name\":\"\",\n"
                    + "\t\t\t\t\t\"parameters\":{\n"
                    + "\t\t\t\t\t\t\"DUP_field\":\"field\",\n"
                    + "\t\t\t\t\t\t\"DUP_id\":\"_id\",\n"
                    + "\t\t\t\t\t\t\"DUP_field2\":\"field2\"\n"
                    + "\t\t\t\t\t}\n"
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

        tableConfigGenerator.remove();
        Path clusterConfigPath = configPath.resolve(TABLE_DIR)
            .resolve("0")
            .resolve(BizConfigGenerator.CLUSTER_DIR)
            .resolve(indexName + BizConfigGenerator.CLUSTER_FILE_SUFFIX);
        assertFalse(Files.exists(clusterConfigPath));

        Path schemaConfigPath = configPath.resolve(TABLE_DIR).resolve("0").resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);
        assertFalse(Files.exists(schemaConfigPath));

        Path dataTablesPath = configPath.resolve(TABLE_DIR)
            .resolve("0")
            .resolve(DATA_TABLES_DIR)
            .resolve(indexName + DATA_TABLES_FILE_SUFFIX);
        assertFalse(Files.exists(dataTablesPath));
    }

    public void testMaxDocConfig() throws IOException {
        String indexName = randomAlphaOfLength(5);
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        Path configPath = createTempDir();
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(CLUSTER_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(SCHEMAS_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(DATA_TABLES_DIR));

        try {
            Settings settings = Settings.builder().put("index.havenask.flush.max_doc_count", "0").build();
            TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(indexName, settings, mapperService, configPath);
            tableConfigGenerator.generate();
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("index.havenask.flush.max_doc_count must be a positive integer", e.getMessage());
        }

        try {
            Settings settings = Settings.builder().put("index.havenask.flush.max_doc_count", "-1").build();
            TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(indexName, settings, mapperService, configPath);
            tableConfigGenerator.generate();
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("index.havenask.flush.max_doc_count must be a positive integer", e.getMessage());
        }

        try {
            Settings settings = Settings.builder().put("index.havenask.flush.max_doc_count", "abc").build();
            TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(indexName, settings, mapperService, configPath);
            tableConfigGenerator.generate();
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [abc] for setting [index.havenask.flush.max_doc_count]", e.getMessage());
        }

        Settings settings = Settings.builder().put("index.havenask.flush.max_doc_count", "10").build();
        TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(indexName, settings, mapperService, configPath);
        tableConfigGenerator.generate();

        {
            Path clusterConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(CLUSTER_DIR)
                .resolve(indexName + CLUSTER_FILE_SUFFIX);
            assertTrue(Files.exists(clusterConfigPath));
            String content = Files.readString(clusterConfigPath);
            String expect = String.format(
                Locale.ROOT,
                "{\n"
                    + "\t\"build_option_config\":{\n"
                    + "\t\t\"async_build\":true,\n"
                    + "\t\t\"async_queue_size\":50000,\n"
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
                    + "\t\"direct_write\":true,\n"
                    + "\t\"offline_index_config\":{\n"
                    + "\t\t\"build_config\":{\n"
                    + "\t\t\t\"build_total_memory\":128,\n"
                    + "\t\t\t\"max_doc_count\":100000\n"
                    + "\t\t},\n"
                    + "\t\t\"merge_config\":{\n"
                    + "\t\t\t\"merge_strategy\":\"combined\"\n"
                    + "\t\t}\n"
                    + "\t},\n"
                    + "\t\"online_index_config\":{\n"
                    + "\t\t\"build_config\":{\n"
                    + "\t\t\t\"build_total_memory\":128,\n"
                    + "\t\t\t\"max_doc_count\":10\n"
                    + "\t\t},\n"
                    + "\t\t\"enable_async_dump_segment\":true,\n"
                    + "\t\t\"max_realtime_dump_interval\":60,\n"
                    + "\t\t\"on_disk_flush_realtime_index\":true\n"
                    + "\t},\n"
                    + "\t\"realtime\":true,\n"
                    + "\t\"wal_config\":{\n"
                    + "\t\t\"sink\":{\n"
                    + "\t\t\t\"queue_name\":\"%s\",\n"
                    + "\t\t\t\"queue_size\":\"500000\"\n"
                    + "\t\t},\n"
                    + "\t\t\"strategy\":\"queue\",\n"
                    + "\t\t\"timeout_ms\":10000\n"
                    + "\t}\n"
                    + "}",
                indexName,
                indexName,
                indexName
            );
            assertEquals(expect, content);
        }

        {
            Path schemaConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(SCHEMAS_DIR)
                .resolve(indexName + SCHEMAS_FILE_SUFFIX);
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
            Path dataTablesPath = configPath.resolve(TABLE_DIR)
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

        tableConfigGenerator.remove();
        Path clusterConfigPath = configPath.resolve(TABLE_DIR)
            .resolve("0")
            .resolve(BizConfigGenerator.CLUSTER_DIR)
            .resolve(indexName + BizConfigGenerator.CLUSTER_FILE_SUFFIX);
        assertFalse(Files.exists(clusterConfigPath));

        Path schemaConfigPath = configPath.resolve(TABLE_DIR).resolve("0").resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);
        assertFalse(Files.exists(schemaConfigPath));

        Path dataTablesPath = configPath.resolve(TABLE_DIR)
            .resolve("0")
            .resolve(DATA_TABLES_DIR)
            .resolve(indexName + DATA_TABLES_FILE_SUFFIX);
        assertFalse(Files.exists(dataTablesPath));
    }
}
