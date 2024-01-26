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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;

import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Nullable;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.BizConfig;
import org.havenask.engine.index.config.DataTable;
import org.havenask.engine.index.config.Processor.ProcessChain;
import org.havenask.engine.index.config.Processor.ProcessorChainConfig;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.util.VersionUtils;
import org.havenask.index.mapper.MapperService;

public class TableConfigGenerator {
    public static final String TABLE_DIR = "table";
    public static final String CLUSTER_DIR = "clusters";
    public static final String CLUSTER_FILE_SUFFIX = "_cluster.json";
    public static final String SCHEMAS_DIR = "schemas";
    public static final String SCHEMAS_FILE_SUFFIX = "_schema.json";
    public static final String DATA_TABLES_DIR = "data_tables";
    public static final String DATA_TABLES_FILE_SUFFIX = "_table.json";
    private final Path configPath;
    private final String indexName;
    private final Settings indexSettings;
    private final MapperService mapperService;

    public TableConfigGenerator(String indexName, Settings indexSettings, @Nullable MapperService mapperService, Path configPath) {
        this.indexName = indexName;
        this.indexSettings = indexSettings;
        this.mapperService = mapperService;
        this.configPath = configPath.resolve(TABLE_DIR);
    }

    public static void generateTable(String indexName, Settings indexSettings, MapperService mapperService, Path configPath)
        throws IOException {
        TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(indexName, indexSettings, mapperService, configPath);
        tableConfigGenerator.generate();
    }

    public static void removeTable(String indexName, Path configPath) throws IOException {
        TableConfigGenerator tableConfigGenerator = new TableConfigGenerator(indexName, null, null, configPath);
        tableConfigGenerator.remove();
    }

    public void generate() throws IOException {
        long lastVersion = VersionUtils.getMaxVersion(configPath, 0);
        String strVersion = String.valueOf(lastVersion);
        generateClusterConfig(strVersion);
        Schema schema = generateSchema(strVersion);
        generateDataTable(schema, strVersion);
    }

    public void remove() throws IOException {
        long lastVersion = VersionUtils.getMaxVersion(configPath, 0);
        String strVersion = String.valueOf(lastVersion);
        Path clusterConfigPath = configPath.resolve(strVersion).resolve(CLUSTER_DIR).resolve(indexName + CLUSTER_FILE_SUFFIX);
        Files.deleteIfExists(clusterConfigPath);

        Path schemaPath = configPath.resolve(strVersion).resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);
        Files.deleteIfExists(schemaPath);

        Path dataTablePath = configPath.resolve(strVersion).resolve(DATA_TABLES_DIR).resolve(indexName + DATA_TABLES_FILE_SUFFIX);
        Files.deleteIfExists(dataTablePath);
    }

    private void generateClusterConfig(String version) throws IOException {
        String clusterJson = EngineSettings.HAVENASK_CLUSTER_JSON.get(indexSettings);
        Path clusterConfigPath = configPath.resolve(version).resolve(CLUSTER_DIR).resolve(indexName + CLUSTER_FILE_SUFFIX);
        if (clusterJson != null && !clusterJson.equals("")) {
            Files.write(
                clusterConfigPath,
                clusterJson.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        } else {
            BizConfig bizConfig = new BizConfig();
            bizConfig.cluster_config.builder_rule_config.partition_count = indexSettings.getAsInt(
                IndexMetadata.SETTING_NUMBER_OF_SHARDS,
                1
            );
            bizConfig.online_index_config.build_config.max_doc_count = EngineSettings.HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.get(
                indexSettings
            );
            bizConfig.cluster_config.cluster_name = indexName;
            bizConfig.cluster_config.table_name = indexName;
            bizConfig.wal_config.sink.queue_name = indexName;
            bizConfig.wal_config.sink.queue_size = String.valueOf(EngineSettings.HAVENASK_WAL_CONFIG_SINK_QUEUE_SIZE.get(indexSettings));
            if (EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD.exists(indexSettings)) {
                bizConfig.cluster_config.hash_mode.hash_field = EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD.get(indexSettings);
            }
            // bizConfig.realtime = true;//EngineSettings.HAVENASK_REALTIME_ENABLE.get(indexSettings);

            Files.write(
                clusterConfigPath,
                bizConfig.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        }
    }

    private Schema generateSchema(String version) throws IOException {
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema schema = schemaGenerator.getSchema(indexName, indexSettings, mapperService);
        Path schemaPath = configPath.resolve(version).resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);

        String schemaJson = EngineSettings.HAVENASK_SCHEMA_JSON.get(indexSettings);
        if (schemaJson != null && !schemaJson.equals("")) {
            Files.write(
                schemaPath,
                schemaJson.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        } else {
            Files.write(
                schemaPath,
                schema.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        }

        return schema;
    }

    private void generateDataTable(Schema schema, String version) throws IOException {
        String dataTableJson = EngineSettings.HAVENASK_DATA_TABLE_JSON.get(indexSettings);
        Path dataTablePath = configPath.resolve(version).resolve(DATA_TABLES_DIR).resolve(indexName + DATA_TABLES_FILE_SUFFIX);
        if (dataTableJson != null && !dataTableJson.equals("")) {
            Files.write(
                dataTablePath,
                dataTableJson.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        } else {
            DataTable dataTable = new DataTable();
            ProcessorChainConfig processorChainConfig = new ProcessorChainConfig();
            processorChainConfig.clusters = List.of(indexName);
            if (schema != null && schema.getDupFields().size() > 0) {
                ProcessChain processChain = new ProcessChain();
                processChain.class_name = "DupFieldProcessor";
                processChain.parameters = new HashMap<>();
                schema.getDupFields().forEach((field) -> { processChain.parameters.put(SchemaGenerator.DUP_PREFIX + field, field); });
                processorChainConfig.document_processor_chain.add(processChain);
            }
            dataTable.processor_chain_config = List.of(processorChainConfig);

            Files.write(
                dataTablePath,
                dataTable.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        }
    }
}
