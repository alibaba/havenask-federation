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
import java.util.function.Function;

import org.havenask.HavenaskException;
import org.havenask.common.io.PathUtils;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.engine.index.config.generator.BizConfigGenerator;
import org.havenask.engine.index.config.generator.TableConfigGenerator;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.util.Utils;
import org.havenask.env.Environment;
import org.havenask.env.ShardLock;
import org.havenask.index.Index;
import org.havenask.index.IndexSettings;
import org.havenask.index.shard.ShardId;
import org.havenask.plugins.NodeEnvironmentPlugin.CustomEnvironment;

import static org.havenask.env.Environment.PATH_HOME_SETTING;

public class HavenaskEngineEnvironment implements CustomEnvironment {
    public static final String DEFAULT_DATA_PATH = "havenask";
    public static final String HAVENASK_CONFIG_PATH = "config";
    public static final String HAVENASK_RUNTIMEDATA_PATH = "runtimedata";
    public static final String HAVENASK_TABLE_CONFIG_PATH = "table";
    public static final String HAVENASK_BIZS_CONFIG_PATH = "bizs";
    public static final String HAVENASK_BS_WORK_PATH = "bs";
    public static final Setting<String> HAVENASK_PATH_DATA_SETTING = new Setting<>(
        "havenask.path.data",
        DEFAULT_DATA_PATH,
        Function.identity(),
        Property.NodeScope
    );

    private final Environment environment;
    private final Path dataPath;
    private final Path configPath;
    private final Path runtimedataPath;
    private final Path bsWorkPath;
    private final Path tablePath;
    private final Path bizsPath;

    private MetaDataSyncer metaDataSyncer;

    public HavenaskEngineEnvironment(final Environment environment, final Settings settings) {
        this.environment = environment;
        if (HAVENASK_PATH_DATA_SETTING.exists(settings)) {
            dataPath = PathUtils.get(HAVENASK_PATH_DATA_SETTING.get(settings)).normalize();
        } else if (this.environment.dataFiles().length >= 1) {
            dataPath = this.environment.dataFiles()[0].resolve(DEFAULT_DATA_PATH);
        } else {
            Path homeFile = PathUtils.get(PATH_HOME_SETTING.get(settings)).toAbsolutePath().normalize();
            dataPath = homeFile.resolve(DEFAULT_DATA_PATH);
        }

        try {
            if (Files.exists(dataPath) == false) {
                Files.createDirectories(dataPath);
            }
        } catch (IOException e) {
            throw new HavenaskException("havenask init engine environment error", e);
        }

        configPath = dataPath.resolve(HAVENASK_CONFIG_PATH);
        runtimedataPath = dataPath.resolve(HAVENASK_RUNTIMEDATA_PATH);
        bsWorkPath = dataPath.resolve(HAVENASK_BS_WORK_PATH);
        tablePath = configPath.resolve(HAVENASK_TABLE_CONFIG_PATH);
        bizsPath = configPath.resolve(HAVENASK_BIZS_CONFIG_PATH);
    }

    /**
     * get havenask data path
     *
     * @return dataPath
     */
    public Path getDataPath() {
        return dataPath;
    }

    /**
     * get config path
     *
     * @return configPath
     */
    public Path getConfigPath() {
        return configPath;
    }

    /**
     * get table path
     *
     * @return tablePath
     */
    public Path getTablePath() {
        return tablePath;
    }

    /**
     * get bizs path
     *
     * @return bizsPath
     */
    public Path getBizsPath() {
        return bizsPath;
    }

    /**
     * get runtime data path
     *
     * @return runtimedataPath
     */
    public Path getRuntimedataPath() {
        return runtimedataPath;
    }

    /**
     * get table config path
     *
     * @return bsWorkPath
     */
    public Path getBsWorkPath() {
        return bsWorkPath;
    }

    /**
     * get table config path
     * @param shardId shardId
     * @return tablePath
     */
    public Path getShardPath(ShardId shardId) {
        String tableName = Utils.getHavenaskTableName(shardId);
        return runtimedataPath.resolve(tableName);
    }

    public void setMetaDataSyncer(MetaDataSyncer metaDataSyncer) {
        this.metaDataSyncer = metaDataSyncer;
    }

    @Override
    public void deleteIndexDirectoryUnderLock(Index index, IndexSettings indexSettings) throws IOException {
        // do nothing
    }

    @Override
    public void deleteShardDirectoryUnderLock(ShardLock lock, IndexSettings indexSettings) throws IOException {
        if (EngineSettings.isHavenaskEngine(indexSettings.getSettings()) == false) {
            return;
        }
        String tableName = Utils.getHavenaskTableName(lock.getShardId());
        BizConfigGenerator.removeBiz(tableName, configPath);
        TableConfigGenerator.removeTable(tableName, configPath);
        Path indexDir = runtimedataPath.resolve(tableName);
        IOUtils.rm(indexDir);
        if (metaDataSyncer != null) {
            metaDataSyncer.setPendingSync();
        }
    }
}
