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

import java.nio.file.Path;
import java.util.function.Function;

import org.havenask.common.io.PathUtils;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;

import static org.havenask.env.Environment.PATH_HOME_SETTING;

public class HavenaskEngineEnvironment {
    public static final String DEFAULT_DATA_PATH = "data_havenask";
    public static final String HAVENASK_CONFIG_PATH = "config";
    public static final String HAVENASK_TABLE_CONFIG_PATH = "table";
    public static final String HAVENASK_BIZS_CONFIG_PATH = "bizs";
    public static final Setting<String> HAVENASK_PATH_DATA_SETTING =
        new Setting<>("havenask.path.data", DEFAULT_DATA_PATH, Function.identity(), Property.NodeScope);

    private final Path dataPath;
    private final Path configPath;
    private final Path tablePath;
    private final Path bizsPath;

    public HavenaskEngineEnvironment(final Settings settings) {
        final Path homeFile = PathUtils.get(PATH_HOME_SETTING.get(settings)).normalize();
        if (HAVENASK_PATH_DATA_SETTING.exists(settings)) {
            dataPath = PathUtils.get(HAVENASK_PATH_DATA_SETTING.get(settings)).normalize();
        } else {
            dataPath = homeFile.resolve(DEFAULT_DATA_PATH);
        }

        configPath = dataPath.resolve(HAVENASK_CONFIG_PATH);
        tablePath = dataPath.resolve(HAVENASK_TABLE_CONFIG_PATH);
        bizsPath = dataPath.resolve(HAVENASK_BIZS_CONFIG_PATH);
    }

    public Path getDataPath() {
        return dataPath;
    }

}
