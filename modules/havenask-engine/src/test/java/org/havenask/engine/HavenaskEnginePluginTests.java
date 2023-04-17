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

import junit.framework.TestCase;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;

/**
 * public static final Setting<Boolean> HAVENASK_ENGINE_ENABLED_SETTING = Setting.boolSetting(
 *         "havenask.engine.enabled",
 *         false,
 *         new Setting.Validator<Boolean>() {
 *             @Override
 *             public void validate(Boolean value) {}
 *
 *             @Override
 *             public void validate(Boolean value, Map<Setting<?>, Object> settings) {
 *                 // DISCOVERY_TYPE_SETTING must be single-node when havenask engine is enabled
 *                 if (value) {
 *                     String discoveryType = (String) settings.get(DISCOVERY_TYPE_SETTING);
 *                     if (false == SINGLE_NODE_DISCOVERY_TYPE.equals(discoveryType)) {
 *                         throw new IllegalArgumentException("havenask engine can only be enabled when discovery
 *                         type is single-node");
 *                     }
 *                 }
 *             }
 *
 *             @Override
 *             public Iterator<Setting<?>> settings() {
 *                 List<Setting<?>> settings = List.of(DISCOVERY_TYPE_SETTING);
 *                 return settings.iterator();
 *             }
 *         },
 *         Property.NodeScope,
 *         Setting.Property.Final
 *     );
 */

public class HavenaskEnginePluginTests extends HavenaskTestCase {
    // test HAVENASK_ENGINE_ENABLED_SETTING
    public void testHavenaskEngineEnabledSetting() {
        // havenask engine can only be enabled when discovery type is single-node
        Settings settings = Settings.builder().put("havenask.engine.enabled", true).put("discovery.type", "zen").build();
        try {
            HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings);
            TestCase.fail("havenask engine can only be enabled when discovery type is single-node");
        } catch (IllegalArgumentException e) {
            TestCase.assertEquals("havenask engine can only be enabled when discovery type is single-node", e.getMessage());
        }

        // havenask engine can only be enabled when discovery type is single-node
        settings = Settings.builder().put("havenask.engine.enabled", true).put("discovery.type", "single-node").build();
        assertTrue(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings));

        // discovery.type is not set
        settings = Settings.builder().put("havenask.engine.enabled", true).build();
        try {
            HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings);
            fail("havenask engine can only be enabled when discovery type is single-node");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine can only be enabled when discovery type is single-node", e.getMessage());
        }

        // havenask.engine.enabled is false
        settings = Settings.builder().put("havenask.engine.enabled", false).build();
        assertFalse(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings));

        // havenask.engine.enabled is not set
        settings = Settings.builder().build();
        assertFalse(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings));
    }
}
