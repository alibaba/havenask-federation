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
import org.havenask.test.HavenaskTestCase;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;

public class HavenaskIndexSettingProviderTests extends HavenaskTestCase {

    public void testGetAdditionalIndexSettings() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider();
        Settings settings = provider.getAdditionalIndexSettings("test", false, Settings.builder().put("index.engine", "havenask").build());
        int replicas = settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 2);
        assertEquals(0, replicas);
        // assert mappingDynamic
        boolean mappingDynamic = settings.getAsBoolean("index.mapper.dynamic", true);
        assertEquals(false, mappingDynamic);
    }

    // test for havenask engine only support 0 replica
    public void testGetAdditionalIndexSettingsWithReplica() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider();
        try {
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder().put("index.engine", "havenask").put(SETTING_NUMBER_OF_REPLICAS, 1).build()
            );
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine only support 0 replica", e.getMessage());
        }
    }

    // test for havenask engine only support 0 replica
    public void testGetAdditionalIndexSettingsWithReplica2() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider();
        try {
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder().put("index.engine", "havenask").put(SETTING_NUMBER_OF_REPLICAS, 2).build()
            );
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine only support 0 replica", e.getMessage());
        }
    }

    // test not havenask engine
    public void testGetAdditionalIndexSettingsWithNotHavenaskEngine() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider();
        Settings settings = provider.getAdditionalIndexSettings("test", false, Settings.builder().build());
        int replicas = settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 2);
        assertEquals(2, replicas);
    }

    // test invalid mappingDynamic
    public void testGetAdditionalIndexSettingsWithInvalidMappingDynamic() {
        HavenaskIndexSettingProvider provider = new HavenaskIndexSettingProvider();
        try {
            provider.getAdditionalIndexSettings(
                "test",
                false,
                Settings.builder().put("index.engine", "havenask").put("index.mapper.dynamic", true).build()
            );
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine only support mapping dynamic false", e.getMessage());
        }
    }
}
