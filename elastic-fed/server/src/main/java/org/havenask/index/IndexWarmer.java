/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.common.lucene.index.HavenaskDirectoryReader;
import org.havenask.common.unit.TimeValue;
import org.havenask.index.fielddata.IndexFieldData;
import org.havenask.index.fielddata.IndexFieldDataService;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.IndexShardState;
import org.havenask.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public final class IndexWarmer {

    private static final Logger logger = LogManager.getLogger(IndexWarmer.class);

    private final List<Listener> listeners;

    IndexWarmer(ThreadPool threadPool, IndexFieldDataService indexFieldDataService,
                Listener... listeners) {
        ArrayList<Listener> list = new ArrayList<>();
        final Executor executor = threadPool.executor(ThreadPool.Names.WARMER);
        list.add(new FieldDataWarmer(executor, indexFieldDataService));

        Collections.addAll(list, listeners);
        this.listeners = Collections.unmodifiableList(list);
    }

    void warm(HavenaskDirectoryReader reader, IndexShard shard, IndexSettings settings) {
        if (shard.state() == IndexShardState.CLOSED) {
            return;
        }
        if (settings.isWarmerEnabled() == false) {
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("{} top warming [{}]", shard.shardId(), reader);
        }
        shard.warmerService().onPreWarm();
        long time = System.nanoTime();
        final List<TerminationHandle> terminationHandles = new ArrayList<>();
        // get a handle on pending tasks
        for (final Listener listener : listeners) {
            terminationHandles.add(listener.warmReader(shard, reader));
        }
        // wait for termination
        for (TerminationHandle terminationHandle : terminationHandles) {
            try {
                terminationHandle.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("top warming has been interrupted", e);
                break;
            }
        }
        long took = System.nanoTime() - time;
        shard.warmerService().onPostWarm(took);
        if (shard.warmerService().logger().isTraceEnabled()) {
            shard.warmerService().logger().trace("top warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
        }
    }

    /** A handle on the execution of  warm-up action. */
    public interface TerminationHandle {

        TerminationHandle NO_WAIT = () -> {};

        /** Wait until execution of the warm-up action completes. */
        void awaitTermination() throws InterruptedException;
    }
    public interface Listener {
        /** Queue tasks to warm-up the given segments and return handles that allow to wait for termination of the
         *  execution of those tasks. */
        TerminationHandle warmReader(IndexShard indexShard, HavenaskDirectoryReader reader);
    }

    private static class FieldDataWarmer implements IndexWarmer.Listener {

        private final Executor executor;
        private final IndexFieldDataService indexFieldDataService;

        FieldDataWarmer(Executor executor, IndexFieldDataService indexFieldDataService) {
            this.executor = executor;
            this.indexFieldDataService = indexFieldDataService;
        }

        @Override
        public TerminationHandle warmReader(final IndexShard indexShard, final HavenaskDirectoryReader reader) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, MappedFieldType> warmUpGlobalOrdinals = new HashMap<>();
            for (MappedFieldType fieldType : mapperService.fieldTypes()) {
                final String indexName = fieldType.name();
                if (fieldType.eagerGlobalOrdinals() == false) {
                    continue;
                }
                warmUpGlobalOrdinals.put(indexName, fieldType);
            }
            final CountDownLatch latch = new CountDownLatch(warmUpGlobalOrdinals.size());
            for (final MappedFieldType fieldType : warmUpGlobalOrdinals.values()) {
                executor.execute(() -> {
                    try {
                        final long start = System.nanoTime();
                        IndexFieldData.Global<?> ifd = indexFieldDataService.getForField(fieldType,
                            indexFieldDataService.index().getName(),
                            () -> {
                                throw new UnsupportedOperationException("search lookup not available when warming an index");
                            });
                        IndexFieldData<?> global = ifd.loadGlobal(reader);
                        if (reader.leaves().isEmpty() == false) {
                            global.load(reader.leaves().get(0));
                        }

                        if (indexShard.warmerService().logger().isTraceEnabled()) {
                            indexShard.warmerService().logger().trace(
                                "warmed global ordinals for [{}], took [{}]",
                                fieldType.name(),
                                TimeValue.timeValueNanos(System.nanoTime() - start));
                        }
                    } catch (Exception e) {
                        indexShard
                            .warmerService()
                            .logger()
                            .warn(() -> new ParameterizedMessage("failed to warm-up global ordinals for [{}]", fieldType.name()), e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            return () -> latch.await();
        }
    }

}
