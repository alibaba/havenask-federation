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

import org.havenask.common.settings.Settings;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.index.shard.IndexSettingProvider;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;

public class HavenaskIndexSettingProvider implements IndexSettingProvider {
    public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
        if (EngineSettings.isHavenaskEngine(templateAndRequestSettings)) {
            int replica = templateAndRequestSettings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 0);
            if (replica != 0) {
                throw new IllegalArgumentException("havenask engine only support 0 replica");
            }
            return Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 0).build();
        } else {
            return Settings.EMPTY;
        }
    }
}
