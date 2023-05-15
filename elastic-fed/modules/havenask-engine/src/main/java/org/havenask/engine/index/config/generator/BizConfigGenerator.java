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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.havenask.common.Nullable;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.BizConfig;
import org.havenask.engine.index.config.DataTable;
import org.havenask.engine.index.config.Processor.ProcessorChainConfig;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.index.config.ZoneBiz;
import org.havenask.engine.util.VersionUtils;
import org.havenask.index.mapper.MapperService;

public class BizConfigGenerator {

    public static final String BIZ_DIR = "bizs";
    public static final String CLUSTER_DIR = "clusters";
    public static final String CLUSTER_FILE_SUFFIX = "_cluster.json";
    private static final String PLUGINS_DIR = "plugins";
    public static final String SCHEMAS_DIR = "schemas";
    public static final String SCHEMAS_FILE_SUFFIX = "_schema.json";
    public static final String DATA_TABLES_DIR = "data_tables";
    public static final String DATA_TABLES_FILE_SUFFIX = "_table.json";
    public static final String DEFAULT_BIZ_CONFIG = "zones/general/default_biz.json";
    private final Path configPath;
    private final String indexName;
    private final Settings indexSettings;
    private final MapperService mapperService;

    public BizConfigGenerator(String indexName, Settings indexSettings, @Nullable MapperService mapperService, Path configPath) {
        this.indexName = indexName;
        this.indexSettings = indexSettings;
        this.mapperService = mapperService;
        this.configPath = configPath.resolve(BIZ_DIR);
    }

    public static void generateBiz(String indexName, Settings indexSettings, MapperService mapperService, Path configPath)
        throws IOException {
        BizConfigGenerator bizConfigGenerator = new BizConfigGenerator(indexName, indexSettings, mapperService, configPath);
        bizConfigGenerator.generate();
    }

    public static void removeBiz(String indexName, Path configPath) throws IOException {
        BizConfigGenerator bizConfigGenerator = new BizConfigGenerator(indexName, null, null, configPath);
        bizConfigGenerator.remove();
    }

    public void generate() throws IOException {
        long lastVersion = VersionUtils.getMaxVersion(configPath, 0);
        String strVersion = String.valueOf(lastVersion);
        generateClusterConfig(strVersion);
        generateSchema(strVersion);
        generateDefaultBizConfig(strVersion);
        generateDataTable(strVersion);
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

        removeDefaultBizConfig(strVersion);
    }

    private synchronized void generateDefaultBizConfig(String version) throws IOException {
        Path defaultBizConfigPath = configPath.resolve(version).resolve(DEFAULT_BIZ_CONFIG);

        String strZone = Files.readString(defaultBizConfigPath, StandardCharsets.UTF_8);
        ZoneBiz zoneBiz = ZoneBiz.parse(strZone);
        Set<String> indices = new HashSet<>(zoneBiz.turing_options_config.dependency_table);
        indices.add(indexName);
        zoneBiz.turing_options_config.dependency_table = indices;
        Files.write(
            defaultBizConfigPath,
            zoneBiz.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private synchronized void removeDefaultBizConfig(String version) throws IOException {
        Path defaultBizConfigPath = configPath.resolve(version).resolve(DEFAULT_BIZ_CONFIG);

        String strZone = Files.readString(defaultBizConfigPath, StandardCharsets.UTF_8);
        ZoneBiz zoneBiz = ZoneBiz.parse(strZone);
        Set<String> indices = new HashSet<>(zoneBiz.turing_options_config.dependency_table);
        indices.remove(indexName);
        zoneBiz.turing_options_config.dependency_table = indices;
        Files.write(
            defaultBizConfigPath,
            zoneBiz.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private void generateClusterConfig(String version) throws IOException {
        BizConfig bizConfig = new BizConfig();
        bizConfig.cluster_config.cluster_name = indexName;
        bizConfig.cluster_config.table_name = indexName;
        Path clusterConfigPath = configPath.resolve(version).resolve(CLUSTER_DIR).resolve(indexName + CLUSTER_FILE_SUFFIX);
        Files.write(
            clusterConfigPath,
            bizConfig.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private void generateSchema(String version) throws IOException {
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema schema = schemaGenerator.getSchema(indexName, indexSettings, mapperService);
        Path schemaPath = configPath.resolve(version).resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);
        Files.write(
            schemaPath,
            schema.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private void generateDataTable(String version) throws IOException {
        DataTable dataTable = new DataTable();
        ProcessorChainConfig processorChainConfig = new ProcessorChainConfig();
        processorChainConfig.clusters = List.of(indexName);
        dataTable.processor_chain_config = List.of(processorChainConfig);
        Path dataTablePath = configPath.resolve(version).resolve(DATA_TABLES_DIR).resolve(indexName + DATA_TABLES_FILE_SUFFIX);
        Files.write(
            dataTablePath,
            dataTable.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }
}
