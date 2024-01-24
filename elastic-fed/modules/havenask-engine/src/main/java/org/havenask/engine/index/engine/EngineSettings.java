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

import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EngineSettings {
    public static final String ENGINE_HAVENASK = "havenask";
    public static final String ENGINE_LUCENE = "lucene";

    public static final Setting<String> ENGINE_TYPE_SETTING = new Setting<>("index.engine", "lucene", (s) -> s, new Setting.Validator<>() {
        @Override
        public void validate(String value) {}

        @Override
        public void validate(String value, Map<Setting<?>, Object> settings) {
            // value must be lucene or havenask
            if (!ENGINE_LUCENE.equals(value) && !ENGINE_HAVENASK.equals(value)) {
                throw new IllegalArgumentException("Invalid engine type [" + value + "], must be [lucene] or [havenask]");
            }
        }
    }, Setting.Property.IndexScope, Setting.Property.Final);

    // float/double number will *10^HA3_FLOAT_MUL_BY10 for index and search(using multi fields)
    public static final Setting<Integer> HA3_FLOAT_MUL_BY10 = new Setting<>(
        "index.havenask.float.mul.by10",
        "10",
        Integer::parseInt,
        Setting.Property.IndexScope,
        Property.Final
    );

    // index.havenask.realtime.enable
    public static final Setting<Boolean> HAVENASK_REALTIME_ENABLE = new Setting<>(
        "index.havenask.realtime.enable",
        "false",
        Boolean::parseBoolean,
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {}

            @Override
            public void validate(Boolean value, Map<Setting<?>, Object> settings) {
                if (value) {
                    // index.havenask.realtime.topic_name and index.havenask.realtime.bootstrap.servers must be set
                    String topicName = (String) settings.get(HAVENASK_REALTIME_TOPIC_NAME);
                    String bootstrapServers = (String) settings.get(HAVENASK_REALTIME_BOOTSTRAP_SERVERS);
                    if (topicName == null || topicName.isEmpty()) {
                        throw new IllegalArgumentException("index.havenask.realtime.topic_name must be set");
                    }
                    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                        throw new IllegalArgumentException("index.havenask.realtime.bootstrap.servers must be set");
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> settings = List.of(HAVENASK_REALTIME_TOPIC_NAME, HAVENASK_REALTIME_BOOTSTRAP_SERVERS);
                return settings.iterator();
            }
        },
        Setting.Property.IndexScope,
        Property.Final
    );

    // index.havenask.realtime.topic_name
    public static final Setting<String> HAVENASK_REALTIME_TOPIC_NAME = new Setting<>(
        "index.havenask.realtime.topic_name",
        "",
        (s) -> s,
        Setting.Property.IndexScope,
        Property.Final
    );

    // index.havenask.realtime.bootstrap.servers
    public static final Setting<String> HAVENASK_REALTIME_BOOTSTRAP_SERVERS = new Setting<>(
        "index.havenask.realtime.bootstrap.servers",
        "",
        (s) -> s,
        Setting.Property.IndexScope,
        Property.Final
    );

    // index.havenask.realtime.kafka_start_timestamp_us
    public static final Setting<Long> HAVENASK_REALTIME_KAFKA_START_TIMESTAMP = Setting.longSetting(
        "index.havenask.realtime.kafka_start_timestamp_us",
        0L,
        0L,
        Setting.Property.IndexScope,
        Property.Final
    );

    public static final Setting<Integer> HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT = new Setting<>(
        "index.havenask.build_config.max_doc_count",
        "10000",
        Integer::parseInt,
        new Setting.Validator<>() {
            @Override
            public void validate(Integer value) {
                if (value <= 0) throw new IllegalArgumentException("index.havenask.build_config.max_doc_count must be a positive integer");
            }
        },
        Setting.Property.IndexScope,
        Property.Final
    );

    public static final Setting<Integer> HAVENASK_WAL_CONFIG_SINK_QUEUE_SIZE = new Setting<>(
        "index.havenask.wal_config.sink.queue_size",
        "5000",
        Integer::parseInt,
        new Setting.Validator<>() {
            @Override
            public void validate(Integer value) {
                if (value <= 0) throw new IllegalArgumentException("index.havenask.wal_config.sink.queue_size must be a positive integer");
            }
        },
        Setting.Property.IndexScope,
        Property.Final
    );

    public static final Setting<String> HAVENASK_HASH_MODE_HASH_FIELD = new Setting<>(
        "index.havenask.hash_mode.hash_field",
        "",
        (s) -> s,
        Setting.Property.IndexScope,
        Property.Final
    );

    public static final Setting<String> HAVENASK_CLUSTER_JSON = new Setting<>(
        "index.havenask.cluster.json",
        "",
        (s) -> s,
        Setting.Property.IndexScope,
        Property.Final
    );

    public static final Setting<String> HAVENASK_DATA_TABLE_JSON = new Setting<>(
        "index.havenask.data_table.json",
        "",
        (s) -> s,
        Setting.Property.IndexScope,
        Property.Final
    );

    public static final Setting<String> HAVENASK_SCHEMA_JSON = new Setting<>(
        "index.havenask.schema.json",
        "",
        (s) -> s,
        Setting.Property.IndexScope,
        Property.Final
    );

    public static boolean isHavenaskEngine(Settings indexSettings) {
        return ENGINE_HAVENASK.equals(ENGINE_TYPE_SETTING.get(indexSettings));
    }
}
