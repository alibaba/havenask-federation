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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.HavenaskException;
import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.MetaDataSyncer;
import org.havenask.engine.index.config.generator.BizConfigGenerator;
import org.havenask.engine.index.config.generator.RuntimeSegmentGenerator;
import org.havenask.engine.index.config.generator.TableConfigGenerator;
import org.havenask.index.IndexService;
import org.havenask.index.shard.IndexEventListener;
import org.havenask.index.shard.IndexShard;

public class HavenaskIndexEventListener implements IndexEventListener {
    private static final Logger LOGGER = LogManager.getLogger(HavenaskIndexEventListener.class);

    private final HavenaskEngineEnvironment env;
    private final MetaDataSyncer metaDataSyncer;

    public HavenaskIndexEventListener(HavenaskEngineEnvironment env, MetaDataSyncer metaDataSyncer) {
        this.env = env;
        this.metaDataSyncer = metaDataSyncer;
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
        } catch (Exception e) {
            throw new HavenaskException("generate havenask config error : ", e);
        }
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        checkIndexIsDeleted(indexShard);

        try {
            // 初始化segment信息
            RuntimeSegmentGenerator.generateRuntimeSegment(
                indexShard.shardId(),
                indexShard.indexSettings().getNumberOfShards(),
                indexShard.indexSettings().getSettings(),
                indexShard.mapperService(),
                env.getRuntimedataPath()
            );
        } catch (Exception e) {
            throw new HavenaskException("generate havenask config error", e);
        }
    }

    private void checkIndexIsDeleted(IndexShard indexShard) {
        int loopCount = 60;
        int sleepTime = 1000;

        String tableName = indexShard.shardId().getIndexName();
        Object indexLock = metaDataSyncer.getIndexLock(tableName);
        Object ShardLock = metaDataSyncer.getShardLock(indexShard.shardId());
        try {
            while (loopCount > 0) {
                if (indexLock == null && ShardLock == null) {
                    break;
                }
                Thread.sleep(sleepTime);
                indexLock = metaDataSyncer.getIndexLock(tableName);
                ShardLock = metaDataSyncer.getShardLock(indexShard.shardId());
                loopCount--;
            }
            if (loopCount == 0) {
                LOGGER.error("checkIndexIsDeleted out of time while create shard");
            }
        } catch (InterruptedException e) {
            LOGGER.error("checkIndexIsDeleted error while create shard", e);
        }
    }
}
