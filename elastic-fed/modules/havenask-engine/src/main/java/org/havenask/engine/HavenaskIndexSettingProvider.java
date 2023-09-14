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
import org.havenask.common.unit.TimeValue;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.index.shard.IndexSettingProvider;

import static org.havenask.engine.HavenaskEnginePlugin.HAVENASK_SET_DEFAULT_ENGINE_SETTING;
import static org.havenask.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;

public class HavenaskIndexSettingProvider implements IndexSettingProvider {
    private static final String DEFAULT_REFRESH_INTERVAL = "5s";
    private static final TimeValue MAX_REFRESH_INTERVAL = TimeValue.timeValueMinutes(5);
    private final boolean defaultHavenaskEngine;

    public HavenaskIndexSettingProvider(Settings clusterSettings) {
        this.defaultHavenaskEngine = HAVENASK_SET_DEFAULT_ENGINE_SETTING.get(clusterSettings);
    }

    public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
        Settings.Builder builder = Settings.builder();
        if (defaultHavenaskEngine) {
            builder.put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK);
        }

        if (defaultHavenaskEngine || EngineSettings.isHavenaskEngine(templateAndRequestSettings)) {
            if (false == templateAndRequestSettings.hasValue(INDEX_REFRESH_INTERVAL_SETTING.getKey())) {
                builder.put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), DEFAULT_REFRESH_INTERVAL);
            } else {
                TimeValue refresh = templateAndRequestSettings.getAsTime(INDEX_REFRESH_INTERVAL_SETTING.getKey(), null);
                if (refresh.millis() > MAX_REFRESH_INTERVAL.millis()) {
                    throw new IllegalArgumentException("havenask engine only support refresh interval less than " + MAX_REFRESH_INTERVAL);
                }
            }

            // 如果开启了realtime模式,则默认消费kafka的最新数据
            boolean realTime = EngineSettings.HAVENASK_REALTIME_ENABLE.get(templateAndRequestSettings);
            if (realTime && false == templateAndRequestSettings.hasValue(EngineSettings.HAVENASK_REALTIME_KAFKA_START_TIMESTAMP.getKey())) {
                builder.put(EngineSettings.HAVENASK_REALTIME_KAFKA_START_TIMESTAMP.getKey(), System.currentTimeMillis() * 1000);
            }
            return builder.build();
        } else {
            return Settings.EMPTY;
        }
    }
}
