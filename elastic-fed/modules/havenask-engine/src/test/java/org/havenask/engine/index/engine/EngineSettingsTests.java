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

    // test index.havenask.realtime.enable
    public void testHavenaskRealtimeEnable() {
        Settings settings = Settings.builder().put("index.havenask.realtime.enable", true).build();
        try {
            EngineSettings.HAVENASK_REALTIME_ENABLE.get(settings);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("index.havenask.realtime.topic_name must be set", e.getMessage());
        }

        settings = Settings.builder().put("index.havenask.realtime.enable", true).put("index.havenask.realtime.topic_name", "test").build();
        try {
            EngineSettings.HAVENASK_REALTIME_ENABLE.get(settings);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("index.havenask.realtime.bootstrap.servers must be set", e.getMessage());
        }

        settings = Settings.builder()
            .put("index.havenask.realtime.enable", true)
            .put("index.havenask.realtime.topic_name", "test")
            .put("index.havenask.realtime.bootstrap.servers", "localhost:9092")
            .build();
        assertTrue(EngineSettings.HAVENASK_REALTIME_ENABLE.get(settings));

        settings = Settings.builder().put("index.havenask.realtime.enable", false).build();
        assertFalse(EngineSettings.HAVENASK_REALTIME_ENABLE.get(settings));
    }

    public void testHavenaskMaxDocCount() {
        Settings settings = Settings.builder().put("index.havenask.flush.max_doc_count", "100").build();
        assertEquals(100, (int) EngineSettings.HAVENASK_FLUSH_MAX_DOC_COUNT.get(settings));

        settings = Settings.builder().put("index.havenask.flush.max_doc_count", "100000").build();
        assertEquals(100000, (int) EngineSettings.HAVENASK_FLUSH_MAX_DOC_COUNT.get(settings));

        try {
            settings = Settings.builder().put("index.havenask.flush.max_doc_count", "0").build();
            EngineSettings.HAVENASK_FLUSH_MAX_DOC_COUNT.get(settings);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("index.havenask.flush.max_doc_count must be a positive integer", e.getMessage());
        }

        try {
            settings = Settings.builder().put("index.havenask.flush.max_doc_count", "-1").build();
            EngineSettings.HAVENASK_FLUSH_MAX_DOC_COUNT.get(settings);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("index.havenask.flush.max_doc_count must be a positive integer", e.getMessage());
        }

        try {
            settings = Settings.builder().put("index.havenask.flush.max_doc_count", "abc").build();
            EngineSettings.HAVENASK_FLUSH_MAX_DOC_COUNT.get(settings);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [abc] for setting [index.havenask.flush.max_doc_count]", e.getMessage());
        }
    }
}
