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
