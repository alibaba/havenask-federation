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
import org.havenask.engine.util.VersionUtils;
import org.havenask.index.engine.EngineConfig;

public class TableConfigGenerator {
    public static final String CLUSTER_DIR = "cluster";
    public static final String CLUSTER_FILE_SUFFIX = "_cluster.json";
    private final Path configPath;
    private final EngineConfig engineConfig;
    private final String indexName;

    public TableConfigGenerator(EngineConfig engineConfig, Path configPath) {
        this.engineConfig = engineConfig;
        this.indexName = engineConfig.getShardId().getIndexName();
        this.configPath = configPath;
    }

    public void generate() throws IOException {
        long lastVersion = VersionUtils.getMaxVersion(configPath, 0);
        String strVersion = String.valueOf(lastVersion);
        generateClusterConfig(strVersion);
    }

    private void generateClusterConfig(String version) throws IOException {
        BizConfig bizConfig = new BizConfig();
        bizConfig.cluster_config.cluster_name = indexName;
        bizConfig.cluster_config.table_name = indexName;
        Path clusterConfigPath = configPath.resolve(version).resolve(CLUSTER_DIR).resolve(indexName + CLUSTER_FILE_SUFFIX);
        Files.write(clusterConfigPath, bizConfig.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
    }
}
