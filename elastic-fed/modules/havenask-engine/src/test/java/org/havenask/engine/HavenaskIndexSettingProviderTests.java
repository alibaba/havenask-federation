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

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.engine.HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING;
import static org.havenask.engine.HavenaskEnginePlugin.HAVENASK_SET_DEFAULT_ENGINE_SETTING;

import java.util.List;

import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.index.IndexModule;
import org.havenask.index.IndexSettings;
import org.havenask.index.mapper.RoutingFieldMapper;
import org.havenask.test.HavenaskTestCase;

public class HavenaskIndexSettingProviderTests extends HavenaskTestCase {

    public void testGetAdditionalIndexSettings() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings("test", false, Settings.builder().put("index.engine", "havenask").build());
        TimeValue refresh = settings.getAsTime("index.refresh_interval", null);
        boolean softDelete = settings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
        assertEquals(softDelete, false);
        assertEquals(TimeValue.timeValueSeconds(5), refresh);
        String indexStoreType = settings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK);
        assertEquals(indexStoreType, EngineSettings.ENGINE_HAVENASK);
        assertEquals(RoutingFieldMapper.NAME, settings.get(EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD.getKey()));
    }

    // test for havenask engine not support soft delete
    public void testGetAdditionalIndexSettingsWithSoftDelete() {
        try {
            HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder().put("index.engine", "havenask").put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build()
            );
            fail("havenask engine not support soft delete");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine not support soft delete", e.getMessage());
        }
    }

    // test for havenask engine only support index.store.type: havenask
    public void testGetAdditionalIndexSettingsWithIndexStoreType() {
        try {
            HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder().put("index.engine", "havenask").put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "fs").build()
            );
            fail("havenask engine only support index.store.type: havenask");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine only support index.store.type: havenask", e.getMessage());
        }
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
        String indexStoreType = settings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK);
        assertEquals(indexStoreType, EngineSettings.ENGINE_HAVENASK);
    }

    // test invalid index.routing_partition_size
    public void testGetAdditionalIndexSettingsWithRoutingPartitionSize() {
        try {
            HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder().put("index.engine", "havenask").put("index.routing_partition_size", 2).build()
            );
            fail("havenask engine not support routing.partition.size > 1");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine not support routing.partition.size > 1", e.getMessage());
        }
    }

    // test invalid index.routing_path
    public void testGetAdditionalIndexSettingsWithRoutingPath() {
        try {
            HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder().put("index.engine", "havenask").put("index.routing_path", "test").build()
            );
            fail("havenask engine not support custom routing.path");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine not support custom routing.path without hash_field", e.getMessage());
        }
    }

    // test index.havenask.hash_mode.hash_field
    public void testGetAdditionalIndexSettingsWithHashField() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings(
            "test",
            false,
            Settings.builder().put("index.engine", "havenask").put("index.havenask.hash_mode.hash_field", "test").build()
        );
        List<String> routings = settings.getAsList(IndexMetadata.INDEX_ROUTING_PATH.getKey());
        assertEquals(1, routings.size());
        assertEquals("test", routings.get(0));
    }

    public void testGetAdditionalIndexSettingsWithHashFieldAndRoutingPath() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings(
            "test",
            false,
            Settings.builder()
                .put("index.engine", "havenask")
                .put("index.havenask.hash_mode.hash_field", "test")
                .put("index.routing_path", "test")
                .build()
        );
        List<String> routings = settings.getAsList(IndexMetadata.INDEX_ROUTING_PATH.getKey());
        assertEquals(1, routings.size());
        assertEquals("test", routings.get(0));
    }

    public void testGetAdditionalIndexSettingsWithHashFieldAndValidRoutingPath() {
        try {
            HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder()
                    .put("index.engine", "havenask")
                    .put("index.havenask.hash_mode.hash_field", "test")
                    .put("index.routing_path", "test2")
                    .build()
            );
            fail("havenask engine not support custom routing.path");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine not support custom routing.path with different hash_field", e.getMessage());
        }
    }

    // hash_field equals RoutingFieldMapper.NAME
    public void testGetAdditionalIndexSettingsWithHashFieldRouting() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
        Settings settings = provider.getAdditionalIndexSettings(
            "test",
            false,
            Settings.builder().put("index.engine", "havenask").put("index.havenask.hash_mode.hash_field", RoutingFieldMapper.NAME).build()
        );
        assertEquals(RoutingFieldMapper.NAME, settings.get(EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD.getKey()));
        List<String> routings = settings.getAsList(IndexMetadata.INDEX_ROUTING_PATH.getKey());
        assertEquals(0, routings.size());
    }

    // hash_field equals RoutingFieldMapper.NAME but routing_path exists
    public void testGetAdditionalIndexSettingsWithHashFieldAndInvaildRoutingPath() {
        try {
            HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider(Settings.EMPTY);
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder()
                    .put("index.engine", "havenask")
                    .put("index.havenask.hash_mode.hash_field", RoutingFieldMapper.NAME)
                    .put("index.routing_path", RoutingFieldMapper.NAME)
                    .build()
            );
            fail("havenask engine not support custom routing.path");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine not support custom routing.path without hash_field", e.getMessage());
        }
    }
}
