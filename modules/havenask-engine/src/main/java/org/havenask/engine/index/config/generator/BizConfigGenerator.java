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

import org.havenask.engine.index.config.BizConfig;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.index.engine.SchemaGenerate;
import org.havenask.engine.util.VersionUtils;
import org.havenask.index.engine.EngineConfig;

public class BizConfigGenerator {

    public static final String CLUSTER_DIR = "cluster";
    public static final String CLUSTER_FILE_SUFFIX = "_cluster.json";
    private static final String PLUGINS_DIR = "plugins";
    public static final String SCHEMAS_DIR = "schemas";
    public static final String SCHEMAS_FILE_SUFFIX = "_schemas.json";
    private static final String ZONES_DIR = "zones";
    private final Path configPath;
    private final EngineConfig engineConfig;
    private final String indexName;

    public BizConfigGenerator(EngineConfig engineConfig, Path configPath) {
        this.engineConfig = engineConfig;
        this.indexName = engineConfig.getShardId().getIndexName();
        this.configPath = configPath;
    }

    public static void generateBiz(EngineConfig engineConfig, Path configPath) throws IOException {
        BizConfigGenerator bizConfigGenerator = new BizConfigGenerator(engineConfig, configPath);
        bizConfigGenerator.generate();
    }

    public static void removeBiz(EngineConfig engineConfig, Path configPath) throws IOException {
        BizConfigGenerator bizConfigGenerator = new BizConfigGenerator(engineConfig, configPath);
        bizConfigGenerator.remove();
    }

    public void generate() throws IOException {
        long lastVersion = VersionUtils.getMaxVersion(configPath, 0);
        String strVersion = String.valueOf(lastVersion);
        generateClusterConfig(strVersion);
        generateSchema(strVersion);
        generateDefaultBizConfig(strVersion);
    }

    public void remove() throws IOException {
        long lastVersion = VersionUtils.getMaxVersion(configPath, 0);
        String strVersion = String.valueOf(lastVersion);
        Path clusterConfigPath = configPath.resolve(strVersion).resolve(CLUSTER_DIR).resolve(indexName + CLUSTER_FILE_SUFFIX);
        Files.deleteIfExists(clusterConfigPath);

        Path schemaPath = configPath.resolve(strVersion).resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);
        Files.deleteIfExists(schemaPath);
    }

    // TODO 实现具体功能
    private void generateDefaultBizConfig(String version) {

    }

    private void generateClusterConfig(String version) throws IOException {
        BizConfig bizConfig = new BizConfig();
        bizConfig.cluster_config.cluster_name = indexName;
        bizConfig.cluster_config.table_name = indexName;
        Path clusterConfigPath = configPath.resolve(version).resolve(CLUSTER_DIR).resolve(indexName + CLUSTER_FILE_SUFFIX);
        Files.write(clusterConfigPath, bizConfig.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
    }

    private void generateSchema(String version) throws IOException {
        SchemaGenerate schemaGenerate = new SchemaGenerate();
        Schema schema = schemaGenerate.getSchema(engineConfig);
        Path schemaPath = configPath.resolve(version).resolve(SCHEMAS_DIR).resolve(indexName + SCHEMAS_FILE_SUFFIX);
        Files.write(schemaPath, schema.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
    }
}
