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

package org.havenask.engine.index;

import java.io.IOException;

import org.havenask.HavenaskException;
import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.index.config.generator.BizConfigGenerator;
import org.havenask.engine.index.config.generator.TableConfigGenerator;
import org.havenask.index.IndexService;
import org.havenask.index.shard.IndexEventListener;

public class HavenaskIndexEventListener implements IndexEventListener {

    private final HavenaskEngineEnvironment env;

    public HavenaskIndexEventListener(HavenaskEngineEnvironment env) {
        this.env = env;
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        try {
            BizConfigGenerator.generateBiz(
                indexService.index().getName(),
                indexService.getIndexSettings(),
                indexService.mapperService(),
                env.getConfigPath()
            );
            TableConfigGenerator.generateTable(
                indexService.index().getName(),
                indexService.getIndexSettings(),
                indexService.mapperService(),
                env.getConfigPath()
            );
        } catch (IOException e) {
            throw new HavenaskException("generate havenask config error", e);
        }
    }
}
