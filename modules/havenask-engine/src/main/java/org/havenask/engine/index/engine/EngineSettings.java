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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;

import static org.havenask.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;

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

            if (ENGINE_HAVENASK.equals(value)) {
                // havenask engine only support 1 shard
                Integer shards = (Integer) settings.get(INDEX_NUMBER_OF_SHARDS_SETTING);
                if (shards != null && shards != 1) {
                    throw new IllegalArgumentException("havenask engine only support 1 shard");
                }
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            List<Setting<?>> settings = List.of(INDEX_NUMBER_OF_SHARDS_SETTING);
            return settings.iterator();
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

    public static boolean isHavenaskEngine(Settings indexSettings) {
        return ENGINE_HAVENASK.equals(ENGINE_TYPE_SETTING.get(indexSettings));
    }
}
