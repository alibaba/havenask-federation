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

package org.havenask;

import java.io.IOException;

import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.env.Environment;
import org.havenask.env.ShardLock;
import org.havenask.index.Index;
import org.havenask.index.IndexModule;
import org.havenask.index.IndexSettings;

public class TestHavenaskEnginePlugin extends HavenaskEnginePlugin {
    public TestHavenaskEnginePlugin(Settings settings) {
        super(settings);
    }

    @Override
    public CustomEnvironment newEnvironment(final Environment environment, final Settings settings) {
        super.newEnvironment(environment, settings);
        return new CustomEnvironment() {
            @Override
            public void deleteIndexDirectoryUnderLock(Index index, IndexSettings indexSettings) throws IOException {

            }

            @Override
            public void deleteShardDirectoryUnderLock(ShardLock lock, IndexSettings indexSettings) throws IOException {

            }
        };
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {

    }
}
