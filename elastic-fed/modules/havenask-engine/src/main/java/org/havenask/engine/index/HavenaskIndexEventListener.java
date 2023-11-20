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
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
        ReentrantReadWriteLock indexLock = metaDataSyncer != null ? metaDataSyncer.getIndexLock(tableName) : null;
        if (indexLock != null) {
            // TODO:是否使用tryLock设置超时时间更好
            indexLock.writeLock().lock();
            LOGGER.debug("get lock while creating shard, table name :[{}]", tableName);
        }
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
        } finally {
            if (indexLock != null) {
                indexLock.writeLock().unlock();
                LOGGER.debug("release lock after creating shard, table name :[{}]", tableName);
            }
        }
    }
}
