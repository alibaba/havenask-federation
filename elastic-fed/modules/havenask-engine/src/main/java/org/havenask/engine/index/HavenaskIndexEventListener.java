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

import org.havenask.HavenaskException;
import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.index.config.generator.BizConfigGenerator;
import org.havenask.engine.index.config.generator.RuntimeSegmentGenerator;
import org.havenask.engine.index.config.generator.TableConfigGenerator;
import org.havenask.index.IndexService;
import org.havenask.index.shard.IndexEventListener;
import org.havenask.index.shard.IndexShard;

import java.io.IOException;

public class HavenaskIndexEventListener implements IndexEventListener {

    private final HavenaskEngineEnvironment env;

    public HavenaskIndexEventListener(HavenaskEngineEnvironment env) {
        this.env = env;
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        String tableName = indexService.index().getName();
        try {
            BizConfigGenerator.generateBiz(
                tableName,
                indexService.getIndexSettings().getSettings(),
                indexService.mapperService(),
                env.getConfigPath()
            );
            TableConfigGenerator.generateTable(
                tableName,
                indexService.getIndexSettings().getSettings(),
                indexService.mapperService(),
                env.getConfigPath()
            );
        } catch (IOException e) {
            throw new HavenaskException("generate havenask config error", e);
        }
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        try {
            // 初始化segment信息
            RuntimeSegmentGenerator.generateRuntimeSegment(
                indexShard.shardId(),
                indexShard.indexSettings().getNumberOfShards(),
                indexShard.indexSettings().getSettings(),
                indexShard.mapperService(),
                env.getRuntimedataPath()
            );
        } catch (IOException e) {
            throw new HavenaskException("generate havenask config error", e);
        }
    }
}
