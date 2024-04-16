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

package org.havenask.engine.repositories;

import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.fs.FsBlobStoreRepositoryIT;

import java.util.Arrays;
import java.util.Collection;

import static org.havenask.engine.HavenaskInternalClusterTestCase.havenaskNodeSettings;

public class HavenaskFsRepositoryIT extends FsBlobStoreRepositoryIT {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HavenaskEnginePlugin.HAVENASK_SET_DEFAULT_ENGINE_SETTING.getKey(), true)
            .build();
        return havenaskNodeSettings(settings, nodeOrdinal);
    }
}
