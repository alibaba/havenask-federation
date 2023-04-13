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

package org.havenask.engine.index.engine;

import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;

public class EngineSettingsTests extends HavenaskTestCase {
    // test isHavenaskEngine
    public void testIsHavenaskEngine() {
        Settings settings = Settings.builder().put("index.engine", "havenask").build();
        assertTrue(EngineSettings.isHavenaskEngine(settings));
    }

    // test isHavenaskEngine false
    public void testIsHavenaskEngineFalse() {
        Settings settings = Settings.builder().put("index.engine", "lucene").build();
        assertFalse(EngineSettings.isHavenaskEngine(settings));
    }

    // test isHavenaskEngine null
    public void testIsHavenaskEngineNull() {
        Settings settings = Settings.builder().build();
        assertFalse(EngineSettings.isHavenaskEngine(settings));
    }

    // test ENGINE_TYPE_SETTING illegal
    public void testEngineTypeSettingIllegal() {
        Settings settings = Settings.builder().put("index.engine", "test").build();
        try {
            EngineSettings.ENGINE_TYPE_SETTING.get(settings);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid engine type [test], must be [lucene] or [havenask]", e.getMessage());
        }
    }

    // test ENGINE_TYPE_SETTING shard
    public void testEngineTypeSettingShardAndReplica() {
        Settings settings = Settings.builder().put("index.engine", "havenask").put("index.number_of_shards", 2).build();
        try {
            EngineSettings.ENGINE_TYPE_SETTING.get(settings);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("havenask engine only support 1 shard", e.getMessage());
        }
    }

    // test ENGINE_TYPE_SETTING right shard
    public void testEngineTypeSettingRightShard() {
        Settings settings = Settings.builder().put("index.engine", "havenask").put("index.number_of_shards", 1).build();
        assertEquals("havenask", EngineSettings.ENGINE_TYPE_SETTING.get(settings));
    }

}
