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

import static java.util.Collections.singletonList;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DATA_TABLES_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.DATA_TABLES_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.SCHEMAS_DIR;
import static org.havenask.engine.index.config.generator.BizConfigGenerator.SCHEMAS_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.BizConfigGeneratorTests.compareJsonObjects;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.CLUSTER_DIR;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.CLUSTER_FILE_SUFFIX;
import static org.havenask.engine.index.config.generator.TableConfigGenerator.TABLE_DIR;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Locale;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.index.config.BizConfig;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperServiceTestCase;
import org.havenask.plugins.Plugin;

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
            BizConfig bizConfig = new BizConfig();
            bizConfig.cluster_config.cluster_name = indexName;
            bizConfig.cluster_config.table_name = indexName;
            bizConfig.wal_config.sink.queue_name = indexName;
            String expect = bizConfig.toString();
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
                    + "\t\"attributes\":[\"_id\",\"_seq_no\",\"field\",\"_version\",\"_primary_term\"],\n"
                    + "\t\"fields\":[{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_id\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
                    + "\t},{\n"
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
                    + "\t\t\"field_name\":\"_version\",\n"
                    + "\t\t\"field_type\":\"INT64\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_primary_term\",\n"
                    + "\t\t\"field_type\":\"INT64\"\n"
                    + "\t}],\n"
                    + "\t\"indexs\":[{\n"
                    + "\t\t\"has_primary_key_attribute\":true,\n"
                    + "\t\t\"index_fields\":\"_id\",\n"
                    + "\t\t\"index_name\":\"_id\",\n"
                    + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                    + "\t\t\"is_primary_key_sorted\":false\n"
                    + "\t},{\n"
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
                    + "\t}],\n"
                    + "\t\"settings\":{\n"
                    + "\t\t\"enable_all_text_field_meta\":true\n"
                    + "\t},\n"
                    + "\t\"summarys\":{\n"
                    + "\t\t\"compress\":true,\n"
                    + "\t\t\"summary_fields\":[\"_id\",\"_routing\",\"_source\"]\n"
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
                b.startObject("category_example");
                {
                    b.field("type", "keyword");
                }
                b.endObject();
                b.startObject("field");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 128);
                    b.field("category", "category_example");
                }
                b.endObject();
                b.startObject("field2");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
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
            BizConfig bizConfig = new BizConfig();
            bizConfig.cluster_config.cluster_name = indexName;
            bizConfig.cluster_config.table_name = indexName;
            bizConfig.wal_config.sink.queue_name = indexName;
            String expect = bizConfig.toString();
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
                    + "\t\"attributes\":[\"_id\",\"category_example\",\"_seq_no\",\"_version\",\"_primary_term\"],\n"
                    + "\t\"fields\":[{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_id\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"_routing\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"category_example\",\n"
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
                    + "\t\t\"field_name\":\"_version\",\n"
                    + "\t\t\"field_type\":\"INT64\"\n"
                    + "\t},{\n"
                    + "\t\t\"binary_field\":false,\n"
                    + "\t\t\"field_name\":\"field2\",\n"
                    + "\t\t\"field_type\":\"STRING\"\n"
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
                    + "\t\t\"has_primary_key_attribute\":true,\n"
                    + "\t\t\"index_fields\":\"_id\",\n"
                    + "\t\t\"index_name\":\"_id\",\n"
                    + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                    + "\t\t\"is_primary_key_sorted\":false\n"
                    + "\t},{\n"
                    + "\t\t\"index_fields\":\"_routing\",\n"
                    + "\t\t\"index_name\":\"_routing\",\n"
                    + "\t\t\"index_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"index_fields\":\"category_example\",\n"
                    + "\t\t\"index_name\":\"category_example\",\n"
                    + "\t\t\"index_type\":\"STRING\"\n"
                    + "\t},{\n"
                    + "\t\t\"index_fields\":\"_seq_no\",\n"
                    + "\t\t\"index_name\":\"_seq_no\",\n"
                    + "\t\t\"index_type\":\"NUMBER\"\n"
                    + "\t},{\n"
                    + "\t\t\"index_fields\":[\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"_id\"\n"
                    + "\t\t\t},\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"category_example\"\n"
                    + "\t\t\t},\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"DUP_field\"\n"
                    + "\t\t\t}\n"
                    + "\t\t],\n"
                    + "\t\t\"index_name\":\"field\",\n"
                    + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                    + "\t\t\"indexer\":\"aitheta2_indexer\",\n"
                    + "\t\t\"parameters\":{\n"
                    + "\t\t\t\"dimension\":\"128\",\n"
                    + "\t\t\t\"enable_rt_build\":\"true\",\n"
                    + "\t\t\t\"distance_type\":\"SquaredEuclidean\",\n"
                    + "\t\t\t\"ignore_invalid_doc\":\"true\",\n"
                    + "\t\t\t\"builder_name\":\"HnswBuilder\",\n"
                    + "\t\t\t\"searcher_name\":\"HnswSearcher\",\n"
                    + "\t\t\t\"build_index_params\":\"{\\\"proxima.hnsw.builder.thread_count\\\":%d}\"\n"
                    + "\t\t}\n"
                    + "\t},{\n"
                    + "\t\t\"index_fields\":[\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"_id\"\n"
                    + "\t\t\t},\n"
                    + "\t\t\t{\n"
                    + "\t\t\t\t\"boost\":1,\n"
                    + "\t\t\t\t\"field_name\":\"DUP_field2\"\n"
                    + "\t\t\t}\n"
                    + "\t\t],\n"
                    + "\t\t\"index_name\":\"field2\",\n"
                    + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                    + "\t\t\"indexer\":\"aitheta2_indexer\",\n"
                    + "\t\t\"parameters\":{\n"
                    + "\t\t\t\"dimension\":\"128\",\n"
                    + "\t\t\t\"enable_rt_build\":\"true\",\n"
                    + "\t\t\t\"distance_type\":\"SquaredEuclidean\",\n"
                    + "\t\t\t\"ignore_invalid_doc\":\"true\",\n"
                    + "\t\t\t\"builder_name\":\"HnswBuilder\",\n"
                    + "\t\t\t\"searcher_name\":\"HnswSearcher\",\n"
                    + "\t\t\t\"build_index_params\":\"{\\\"proxima.hnsw.builder.thread_count\\\":%d}\"\n"
                    + "\t\t}\n"
                    + "\t}],\n"
                    + "\t\"settings\":{\n"
                    + "\t\t\"enable_all_text_field_meta\":true\n"
                    + "\t},\n"
                    + "\t\"summarys\":{\n"
                    + "\t\t\"compress\":true,\n"
                    + "\t\t\"summary_fields\":[\"_id\",\"_routing\",\"_source\"]\n"
                    + "\t},\n"
                    + "\t\"table_name\":\"%s\",\n"
                    + "\t\"table_type\":\"normal\"\n"
                    + "}",
                DenseVectorFieldMapper.HnswIndexOptions.getBuilderMaxNeighborCnt(),
                DenseVectorFieldMapper.HnswIndexOptions.getBuilderMaxNeighborCnt(),
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

    public void testFullSettings() throws IOException {
        String indexName = randomAlphaOfLength(5);
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        Path configPath = createTempDir();
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(CLUSTER_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(SCHEMAS_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(DATA_TABLES_DIR));
        TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(EngineSettings.HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.getKey(), 10)
                .put(EngineSettings.HAVENASK_WAL_CONFIG_SINK_QUEUE_SIZE.getKey(), 100)
                .put(EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD.getKey(), "test")
                .build(),
            mapperService,
            configPath
        );
        tableConfigGenerator.generate();

        {
            Path clusterConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(BizConfigGenerator.CLUSTER_DIR)
                .resolve(indexName + BizConfigGenerator.CLUSTER_FILE_SUFFIX);
            assertTrue(Files.exists(clusterConfigPath));
            String content = Files.readString(clusterConfigPath);
            BizConfig bizConfig = new BizConfig();
            bizConfig.cluster_config.cluster_name = indexName;
            bizConfig.cluster_config.table_name = indexName;
            bizConfig.wal_config.sink.queue_name = indexName;
            bizConfig.online_index_config.build_config.max_doc_count = 10;
            bizConfig.cluster_config.builder_rule_config.partition_count = 3;
            bizConfig.wal_config.sink.queue_size = "100";
            bizConfig.cluster_config.hash_mode.hash_field = "test";
            String expect = bizConfig.toString();

            assertEquals(expect, content);
        }
    }

    public void testGenerateByHavenaskParamsSettings() throws IOException {
        String indexName = randomAlphaOfLength(5);
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        Path configPath = createTempDir();
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(CLUSTER_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(SCHEMAS_DIR));
        Files.createDirectories(configPath.resolve(TABLE_DIR).resolve("0").resolve(DATA_TABLES_DIR));

        String clusterJson = "{\n"
            + "   \"cluster_config\":{\n"
            + "       \"builder_rule_config\":{\n"
            + "           \"partition_count\":1\n"
            + "       }\n"
            + "   },\n"
            + "    \"build_option_config\" : {\n"
            + "        \"async_build\" : true,\n"
            + "        \"async_queue_size\" : 1000,\n"
            + "        \"document_filter\" : true,\n"
            + "        \"max_recover_time\" : 30,\n"
            + "        \"sort_build\" : true,\n"
            + "        \"sort_descriptions\" : [\n"
            + "            {\n"
            + "                \"sort_field\" : \"hits\",\n"
            + "                \"sort_pattern\" : \"asc\"\n"
            + "            }\n"
            + "        ],\n"
            + "        \"sort_queue_mem\" : 4096,\n"
            + "        \"sort_queue_size\" : 10000000\n"
            + "    }"
            + "}";

        String schemaJson = "{\n"
            + "    \"attributes\":[\"_seq_no\",\"_id\",\"_version\",\"_primary_term\"],\n"
            + "    \"fields\":[{\n"
            + "        \"analyzer\":\"simple_analyzer\",\n"
            + "        \"binary_field\":false,\n"
            + "        \"field_name\":\"foo\",\n"
            + "        \"field_type\":\"TEXT\"\n"
            + "    }],\n"
            + "    \"indexs\":[{\n"
            + "        \"doc_payload_flag\":1,\n"
            + "        \"index_fields\":\"foo\",\n"
            + "        \"index_name\":\"foo\",\n"
            + "        \"index_type\":\"TEXT\",\n"
            + "        \"position_list_flag\":1,\n"
            + "        \"position_payload_flag\":1,\n"
            + "        \"term_frequency_flag\":1\n"
            + "    }]\n"
            + "}";

        String dataTableJson = String.format(
            Locale.ROOT,
            "{\n"
                + "    \"processor_chain_config\" : [\n"
                + "        {\n"
                + "            \"clusters\" : [\n"
                + "                \"%s\"\n"
                + "            ],\n"
                + "            \"document_processor_chain\" : [\n"
                + "                {\n"
                + "                    \"class_name\" : \"AnyClassName\",\n"
                + "                    \"module_name\" : \"example_module\",\n"
                + "                    \"parameters\" : {\n"
                + "                        \"param1\":\"value1\"\n"
                + "                    }\n"
                + "                }\n"
                + "            ],\n"
                + "            \"modules\" : [\n"
                + "                 {\n"
                + "                    \"module_name\" : \"example_module\",\n"
                + "                    \"module_path\" : \"libExample.so\",\n"
                + "                    \"parameters\" : {\n"
                + "                    }\n"
                + "                 }\n"
                + "            ]\n"
                + "        }\n"
                + "    ]\n"
                + "}",
            indexName
        );

        TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(EngineSettings.HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.getKey(), 10)
                .put(EngineSettings.HAVENASK_WAL_CONFIG_SINK_QUEUE_SIZE.getKey(), 100)
                .put(EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD.getKey(), "test")
                .put(EngineSettings.HAVENASK_CLUSTER_JSON.getKey(), clusterJson)
                .put(EngineSettings.HAVENASK_SCHEMA_JSON.getKey(), schemaJson)
                .put(EngineSettings.HAVENASK_DATA_TABLE_JSON.getKey(), dataTableJson)
                .build(),
            mapperService,
            configPath
        );
        tableConfigGenerator.generate();

        {
            String expectedClusterJsonStr = String.format(
                Locale.ROOT,
                "{\n"
                    + "    \"cluster_config\":{\n"
                    + "        \"cluster_name\":\"%s\",\n"
                    + "        \"builder_rule_config\":{\n"
                    + "            \"partition_count\":1\n"
                    + "        },\n"
                    + "        \"hash_mode\":{\n"
                    + "            \"hash_field\":\"test\",\n"
                    + "            \"hash_function\":\"HASH\"\n"
                    + "        },\n"
                    + "        \"table_name\":\"%s\"\n"
                    + "    },\n"
                    + "    \"direct_write\":true,\n"
                    + "    \"online_index_config\":{\n"
                    + "        \"build_config\":{\n"
                    + "            \"max_doc_count\":10\n"
                    + "        }\n"
                    + "    },\n"
                    + "    \"build_option_config\":{\n"
                    + "        \"sort_queue_mem\":4096,\n"
                    + "        \"async_queue_size\":1000,\n"
                    + "        \"document_filter\":true,\n"
                    + "        \"max_recover_time\":30,\n"
                    + "        \"sort_descriptions\":[\n"
                    + "            {\n"
                    + "                \"sort_field\":\"hits\",\n"
                    + "                \"sort_pattern\":\"asc\"\n"
                    + "            }\n"
                    + "        ],\n"
                    + "        \"sort_build\":true,\n"
                    + "        \"async_build\":true,\n"
                    + "        \"sort_queue_size\":10000000\n"
                    + "    },\n"
                    + "    \"wal_config\":{\n"
                    + "        \"sink\":{\n"
                    + "            \"queue_name\":\"%s\",\n"
                    + "            \"queue_size\":\"100\"\n"
                    + "        },\n"
                    + "        \"strategy\":\"queue\"\n"
                    + "    }\n"
                    + "}",
                indexName,
                indexName,
                indexName
            );

            Path clusterConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(CLUSTER_DIR)
                .resolve(indexName + CLUSTER_FILE_SUFFIX);
            assertTrue(Files.exists(clusterConfigPath));
            String content = Files.readString(clusterConfigPath);

            assertTrue(
                JSONObject.parseObject(expectedClusterJsonStr, Feature.OrderedField)
                    .equals(JSONObject.parseObject(content, Feature.OrderedField))
            );
        }

        {
            String expectedSchemaJsonStr = String.format(
                Locale.ROOT,
                "{\n"
                    + "    \"attributes\":[\"_seq_no\",\"_id\",\"_version\",\"_primary_term\"],\n"
                    + "    \"fields\":[{\n"
                    + "        \"binary_field\":false,\n"
                    + "        \"field_name\":\"_routing\",\n"
                    + "        \"field_type\":\"STRING\"\n"
                    + "    },{\n"
                    + "        \"binary_field\":false,\n"
                    + "        \"field_name\":\"_seq_no\",\n"
                    + "        \"field_type\":\"INT64\"\n"
                    + "    },{\n"
                    + "        \"analyzer\":\"simple_analyzer\",\n"
                    + "        \"binary_field\":false,\n"
                    + "        \"field_name\":\"foo\",\n"
                    + "        \"field_type\":\"TEXT\"\n"
                    + "    },{\n"
                    + "        \"binary_field\":false,\n"
                    + "        \"field_name\":\"_source\",\n"
                    + "        \"field_type\":\"STRING\"\n"
                    + "    },{\n"
                    + "        \"binary_field\":false,\n"
                    + "        \"field_name\":\"_id\",\n"
                    + "        \"field_type\":\"STRING\"\n"
                    + "    },{\n"
                    + "        \"binary_field\":false,\n"
                    + "        \"field_name\":\"_version\",\n"
                    + "        \"field_type\":\"INT64\"\n"
                    + "    },{\n"
                    + "        \"binary_field\":false,\n"
                    + "        \"field_name\":\"_primary_term\",\n"
                    + "        \"field_type\":\"INT64\"\n"
                    + "    }],\n"
                    + "    \"indexs\":[{\n"
                    + "        \"has_primary_key_attribute\":true,\n"
                    + "        \"index_fields\":\"_id\",\n"
                    + "        \"index_name\":\"_id\",\n"
                    + "        \"index_type\":\"PRIMARYKEY64\",\n"
                    + "        \"is_primary_key_sorted\":false\n"
                    + "    },{\n"
                    + "        \"index_fields\":\"_routing\",\n"
                    + "        \"index_name\":\"_routing\",\n"
                    + "        \"index_type\":\"STRING\"\n"
                    + "    },{\n"
                    + "        \"index_fields\":\"_seq_no\",\n"
                    + "        \"index_name\":\"_seq_no\",\n"
                    + "        \"index_type\":\"NUMBER\"\n"
                    + "    },{\n"
                    + "        \"doc_payload_flag\":1,\n"
                    + "        \"index_fields\":\"foo\",\n"
                    + "        \"index_name\":\"foo\",\n"
                    + "        \"index_type\":\"TEXT\",\n"
                    + "        \"position_list_flag\":1,\n"
                    + "        \"position_payload_flag\":1,\n"
                    + "        \"term_frequency_flag\":1\n"
                    + "    }],\n"
                    + "    \"settings\":{\n"
                    + "        \"enable_all_text_field_meta\":true\n"
                    + "    },\n"
                    + "    \"summarys\":{\n"
                    + "        \"compress\":true,\n"
                    + "        \"summary_fields\":[\"_routing\",\"_source\",\"_id\"]\n"
                    + "    },\n"
                    + "    \"table_name\":\"%s\",\n"
                    + "    \"table_type\":\"normal\"\n"
                    + "}",
                indexName
            );
            Path schemaConfigPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(SCHEMAS_DIR)
                .resolve(indexName + SCHEMAS_FILE_SUFFIX);
            assertTrue(Files.exists(schemaConfigPath));
            String content = Files.readString(schemaConfigPath);

            assertTrue(
                compareJsonObjects(
                    JSONObject.parseObject(expectedSchemaJsonStr, Feature.OrderedField),
                    JSONObject.parseObject(content, Feature.OrderedField)
                )
            );
        }

        {
            String expectedDataTableJson = String.format(
                Locale.ROOT,
                "{\n"
                    + "    \"data_descriptions\":[],\n"
                    + "    \"processor_chain_config\" : [\n"
                    + "        {\n"
                    + "            \"clusters\" : [\n"
                    + "                \"%s\"\n"
                    + "            ],\n"
                    + "            \"document_processor_chain\" : [\n"
                    + "                {\n"
                    + "                    \"class_name\" : \"AnyClassName\",\n"
                    + "                    \"module_name\" : \"example_module\",\n"
                    + "                    \"parameters\" : {\n"
                    + "                        \"param1\":\"value1\"\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ],\n"
                    + "            \"modules\" : [\n"
                    + "                 {\n"
                    + "                    \"module_name\" : \"example_module\",\n"
                    + "                    \"module_path\" : \"libExample.so\",\n"
                    + "                    \"parameters\" : {\n"
                    + "                    }\n"
                    + "                 }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    ],\n"
                    + "    \"processor_config\" : {\n"
                    + "        \"processor_queue_size\" : 2000,\n"
                    + "        \"processor_thread_num\" : 30\n"
                    + "    },\n"
                    + "    \"processor_rule_config\" : {\n"
                    + "        \"parallel_num\" : 1,\n"
                    + "        \"partition_count\" : 1\n"
                    + "    }\n"
                    + "}",
                indexName
            );

            Path dataTablesPath = configPath.resolve(TABLE_DIR)
                .resolve("0")
                .resolve(DATA_TABLES_DIR)
                .resolve(indexName + DATA_TABLES_FILE_SUFFIX);
            assertTrue(Files.exists(dataTablesPath));
            String content = Files.readString(dataTablesPath);

            assertTrue(
                compareJsonObjects(
                    JSONObject.parseObject(expectedDataTableJson, Feature.OrderedField),
                    JSONObject.parseObject(content, Feature.OrderedField)
                )
            );
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
