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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.havenask.common.lease.Releasable;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.util.concurrent.HavenaskRejectedExecutionException;
import org.havenask.index.stats.IndexingPressureStats;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IndexingPressure {

    public static final Setting<ByteSizeValue> MAX_INDEXING_BYTES =
        Setting.memorySizeSetting("indexing_pressure.memory.limit", "10%", Setting.Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(IndexingPressure.class);

    private final AtomicLong currentCombinedCoordinatingAndPrimaryBytes = new AtomicLong(0);
    private final AtomicLong currentCoordinatingBytes = new AtomicLong(0);
    private final AtomicLong currentPrimaryBytes = new AtomicLong(0);
    private final AtomicLong currentReplicaBytes = new AtomicLong(0);

    private final AtomicLong totalCombinedCoordinatingAndPrimaryBytes = new AtomicLong(0);
    private final AtomicLong totalCoordinatingBytes = new AtomicLong(0);
    private final AtomicLong totalPrimaryBytes = new AtomicLong(0);
    private final AtomicLong totalReplicaBytes = new AtomicLong(0);

    private final AtomicLong coordinatingRejections = new AtomicLong(0);
    private final AtomicLong primaryRejections = new AtomicLong(0);
    private final AtomicLong replicaRejections = new AtomicLong(0);

    private final long primaryAndCoordinatingLimits;
    private final long replicaLimits;

    public IndexingPressure(Settings settings) {
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.replicaLimits = (long) (this.primaryAndCoordinatingLimits * 1.5);
    }


    private static Releasable wrapReleasable(Releasable releasable) {
        final AtomicBoolean called = new AtomicBoolean();
        return () -> {
            if (called.compareAndSet(false, true)) {
                releasable.close();
            } else {
                logger.error("IndexingPressure memory is adjusted twice", new IllegalStateException("Releasable is called twice"));
                assert false : "IndexingPressure is adjusted twice";
            }
        };
    }

    public Releasable markCoordinatingOperationStarted(long bytes, boolean forceExecution) {
        long combinedBytes = this.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long replicaWriteBytes = this.currentReplicaBytes.get();
        long totalBytes = combinedBytes + replicaWriteBytes;
        if (forceExecution == false && totalBytes > primaryAndCoordinatingLimits) {
            long bytesWithoutOperation = combinedBytes - bytes;
            long totalBytesWithoutOperation = totalBytes - bytes;
            this.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.coordinatingRejections.getAndIncrement();
            throw new HavenaskRejectedExecutionException("rejected execution of coordinating operation [" +
                "coordinating_and_primary_bytes=" + bytesWithoutOperation + ", " +
                "replica_bytes=" + replicaWriteBytes + ", " +
                "all_bytes=" + totalBytesWithoutOperation + ", " +
                "coordinating_operation_bytes=" + bytes + ", " +
                "max_coordinating_and_primary_bytes=" + primaryAndCoordinatingLimits + "]", false);
        }
        currentCoordinatingBytes.getAndAdd(bytes);
        totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
        totalCoordinatingBytes.getAndAdd(bytes);
        return wrapReleasable(() -> {
            this.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.currentCoordinatingBytes.getAndAdd(-bytes);
        });
    }

    public Releasable markPrimaryOperationLocalToCoordinatingNodeStarted(long bytes) {
        currentPrimaryBytes.getAndAdd(bytes);
        totalPrimaryBytes.getAndAdd(bytes);
        return wrapReleasable(() -> this.currentPrimaryBytes.getAndAdd(-bytes));
    }

    public Releasable markPrimaryOperationStarted(long bytes, boolean forceExecution) {
        long combinedBytes = this.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long replicaWriteBytes = this.currentReplicaBytes.get();
        long totalBytes = combinedBytes + replicaWriteBytes;
        if (forceExecution == false && totalBytes > primaryAndCoordinatingLimits) {
            long bytesWithoutOperation = combinedBytes - bytes;
            long totalBytesWithoutOperation = totalBytes - bytes;
            this.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.primaryRejections.getAndIncrement();
            throw new HavenaskRejectedExecutionException("rejected execution of primary operation [" +
                "coordinating_and_primary_bytes=" + bytesWithoutOperation + ", " +
                "replica_bytes=" + replicaWriteBytes + ", " +
                "all_bytes=" + totalBytesWithoutOperation + ", " +
                "primary_operation_bytes=" + bytes + ", " +
                "max_coordinating_and_primary_bytes=" + primaryAndCoordinatingLimits + "]", false);
        }
        currentPrimaryBytes.getAndAdd(bytes);
        totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
        totalPrimaryBytes.getAndAdd(bytes);
        return wrapReleasable(() -> {
            this.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.currentPrimaryBytes.getAndAdd(-bytes);
        });
    }

    public Releasable markReplicaOperationStarted(long bytes, boolean forceExecution) {
        long replicaWriteBytes = this.currentReplicaBytes.addAndGet(bytes);
        if (forceExecution == false && replicaWriteBytes > replicaLimits) {
            long replicaBytesWithoutOperation = replicaWriteBytes - bytes;
            this.currentReplicaBytes.getAndAdd(-bytes);
            this.replicaRejections.getAndIncrement();
            throw new HavenaskRejectedExecutionException("rejected execution of replica operation [" +
                "replica_bytes=" + replicaBytesWithoutOperation + ", " +
                "replica_operation_bytes=" + bytes + ", " +
                "max_replica_bytes=" + replicaLimits + "]", false);
        }
        totalReplicaBytes.getAndAdd(bytes);
        return wrapReleasable(() -> this.currentReplicaBytes.getAndAdd(-bytes));
    }

    public long getCurrentCombinedCoordinatingAndPrimaryBytes() {
        return currentCombinedCoordinatingAndPrimaryBytes.get();
    }

    public long getCurrentCoordinatingBytes() {
        return currentCoordinatingBytes.get();
    }

    public long getCurrentPrimaryBytes() {
        return currentPrimaryBytes.get();
    }

    public long getCurrentReplicaBytes() {
        return currentReplicaBytes.get();
    }

    public IndexingPressureStats stats() {
        return new IndexingPressureStats(totalCombinedCoordinatingAndPrimaryBytes.get(), totalCoordinatingBytes.get(),
            totalPrimaryBytes.get(), totalReplicaBytes.get(), currentCombinedCoordinatingAndPrimaryBytes.get(),
            currentCoordinatingBytes.get(), currentPrimaryBytes.get(), currentReplicaBytes.get(), coordinatingRejections.get(),
            primaryRejections.get(), replicaRejections.get(), primaryAndCoordinatingLimits);
    }
}
