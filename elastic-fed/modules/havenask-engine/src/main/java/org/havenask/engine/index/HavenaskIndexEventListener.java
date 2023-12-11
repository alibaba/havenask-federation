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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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
        ReentrantLock indexLock = metaDataSyncer.getIndexLock(tableName);
        try {
            if (indexLock != null) {
                if (indexLock.tryLock(60, TimeUnit.SECONDS)) {
                    LOGGER.debug("get lock while creating index, table name :[{}]", tableName);
                } else {
                    LOGGER.debug("failed to get lock while creating index, out of time, table name :[{}]", tableName);
                }
            }
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
        } finally {
            if (indexLock != null) {
                try {
                    indexLock.unlock();
                    LOGGER.debug("release lock after creating index, table name :[{}]", tableName);
                } catch (IllegalMonitorStateException e) {
                    LOGGER.error("release lock error after creating index", e);
                }
            }
        }
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        String tableName = indexShard.shardId().getIndexName();
        String tableWithShardId = tableName + "_" + indexShard.shardId().getId();
        ReentrantLock indexLock = metaDataSyncer.getIndexLock(tableName);
        ReentrantLock indexShardLock = metaDataSyncer.getIndexLock(tableWithShardId);
        try {
            if (indexLock != null) {
                if (indexLock.tryLock(60, TimeUnit.SECONDS)) {
                    LOGGER.debug(
                            "get indexLock while creating shard, index: [{}], shardId :[{}]",
                            tableWithShardId,
                            indexShard.shardId().getId()
                    );
                    indexLock.unlock();
                } else {
                    LOGGER.debug(
                            "failed to get indexLock while creating shard, out of time, index: [{}], shardId :[{}]",
                            tableWithShardId,
                            indexShard.shardId().getId()
                    );
                }
                indexLock = null;
            }
            if (indexShardLock != null) {
                if (indexShardLock.tryLock(60, TimeUnit.SECONDS)) {
                    LOGGER.debug(
                        "get indexShardLock while creating shard, index: [{}], shardId :[{}]",
                        tableWithShardId,
                        indexShard.shardId().getId()
                    );
                } else {
                    LOGGER.debug(
                        "failed to get indexShardLock while creating shard, out of time, index: [{}], shardId :[{}]",
                        tableWithShardId,
                        indexShard.shardId().getId()
                    );
                }
            }
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
        } finally {
            if (indexLock != null) {
                try {
                    indexLock.unlock();
                    LOGGER.debug(
                            "release indexLock after creating shard, table name :[{}], shardId :[{}]",
                            tableName,
                            indexShard.shardId().getId()
                    );
                } catch (IllegalMonitorStateException e) {
                    LOGGER.error("release indexLock error after creating shard, table name :[{}], shardId :[{}]",
                            tableName,
                            indexShard.shardId().getId(),
                            e);
                }
            }
            if (indexShardLock != null) {
                try {
                    indexShardLock.unlock();
                    LOGGER.debug(
                        "release indexShardLock after creating shard, table name :[{}], shardId :[{}]",
                        tableName,
                        indexShard.shardId().getId()
                    );
                } catch (IllegalMonitorStateException e) {
                    LOGGER.error("release indexShardLock error after creating shard, table name :[{}], shardId :[{}]",
                            tableName,
                            indexShard.shardId().getId(),
                            e);
                }
            }
        }
    }
}
