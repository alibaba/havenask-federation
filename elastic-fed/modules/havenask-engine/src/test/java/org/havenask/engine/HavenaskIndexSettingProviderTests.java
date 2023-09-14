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
import org.havenask.test.HavenaskTestCase;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.engine.HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING;
import static org.havenask.engine.HavenaskEnginePlugin.HAVENASK_SET_DEFAULT_ENGINE_SETTING;

public class HavenaskIndexSettingProviderTests extends HavenaskTestCase {

    public void testGetAdditionalIndexSettings() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings("test", false, Settings.builder().put("index.engine", "havenask").build());
        TimeValue refresh = settings.getAsTime("index.refresh_interval", null);
        assertEquals(TimeValue.timeValueSeconds(5), refresh);
    }

    // test not havenask engine
    public void testGetAdditionalIndexSettingsWithNotHavenaskEngine() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings("test", false, Settings.builder().build());
        int replicas = settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 2);
        assertEquals(2, replicas);
    }

    // test havenask engine
    public void testGetAdditionalIndexSettingsWithHavenaskEngine() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings("test", false, Settings.builder().put("index.engine", "havenask").build());
        int replicas = settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 2);
        assertEquals(2, replicas);
    }

    // test HAVENASK_REALTIME_KAFKA_START_TIMESTAMP
    public void testGetAdditionalIndexSettingsWithRealTime() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings(
            "test",
            false,
            Settings.builder()
                .put("index.engine", "havenask")
                .put("index.havenask.realtime.enable", true)
                .put("index.havenask.realtime.topic_name", randomAlphaOfLength(5))
                .put("index.havenask.realtime.bootstrap.servers", randomAlphaOfLength(5))
                .build()
        );
        long timestamp = settings.getAsLong("index.havenask.realtime.kafka_start_timestamp_us", 0L);
        assertTrue(timestamp > 0);
    }

    // test HAVENASK_REALTIME_KAFKA_START_TIMESTAMP with timestamp settings exists
    public void testGetAdditionalIndexSettingsWithRealTimeAndTimestamp() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        long timestamp = System.currentTimeMillis() * 1000;
        Settings settings = provider.getAdditionalIndexSettings(
            "test",
            false,
            Settings.builder()
                .put("index.engine", "havenask")
                .put("index.havenask.realtime.enable", true)
                .put("index.havenask.realtime.topic_name", randomAlphaOfLength(5))
                .put("index.havenask.realtime.bootstrap.servers", randomAlphaOfLength(5))
                .put("index.havenask.realtime.kafka_start_timestamp_us", timestamp)
                .build()
        );
        long timestamp2 = settings.getAsLong("index.havenask.realtime.kafka_start_timestamp_us", 0L);
        assertEquals(0L, timestamp2);
    }

    // test refresh interval
    public void testGetAdditionalIndexSettingsWithRefreshInterval() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings(
            "test",
            false,
            Settings.builder().put("index.engine", "havenask").put("index.refresh_interval", "5m").build()
        );
        TimeValue refresh = settings.getAsTime("index.refresh_interval", null);
        assertEquals(null, refresh);
    }

    // test refresh interval with wrong value
    public void testGetAdditionalIndexSettingsWithRefreshInterval2() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        try {
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder().put("index.engine", "havenask").put("index.refresh_interval", "60m").build()
            );
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine only support refresh interval less than 5m", e.getMessage());
        }
    }

    // test default havenask engine
    public void testGetAdditionalIndexSettingsWithDefaultHavenaskEngine() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(
            Settings.builder()
                .put(HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
                .put(HAVENASK_SET_DEFAULT_ENGINE_SETTING.getKey(), true)
                .build()
        );
        Settings settings = provider.getAdditionalIndexSettings("test", false, Settings.builder().build());
        String engine = settings.get(EngineSettings.ENGINE_TYPE_SETTING.getKey());
        assertEquals(EngineSettings.ENGINE_HAVENASK, engine);
        TimeValue refresh = settings.getAsTime("index.refresh_interval", null);
        assertEquals(TimeValue.timeValueSeconds(5), refresh);
    }
}
