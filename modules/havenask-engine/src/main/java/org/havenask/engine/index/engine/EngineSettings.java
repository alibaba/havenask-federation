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

public class EngineSettings {
    public static final String ENGINE_HAVENASK = "havenask";

    public static final Setting<String> ENGINE_TYPE_SETTING = new Setting<>(
        "index.engine",
        "lucene",
        (s) -> s,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

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
