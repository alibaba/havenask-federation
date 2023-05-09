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

package org.havenask.indices.flush;

import org.apache.logging.log4j.Logger;
import org.havenask.ExceptionsHelper;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.index.shard.ShardId;
import org.havenask.test.InternalTestCluster;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.havenask.test.HavenaskTestCase.assertBusy;

/** Utils for SyncedFlush */
public class SyncedFlushUtil {

    private SyncedFlushUtil() {

    }

    /**
     * Blocking version of {@link SyncedFlushService#attemptSyncedFlush(ShardId, ActionListener)}
     */
    public static ShardsSyncedFlushResult attemptSyncedFlush(Logger logger, InternalTestCluster cluster, ShardId shardId) throws Exception {
        /*
         * When the last indexing operation is completed, we will fire a global checkpoint sync.
         * Since a global checkpoint sync request is a replication request, it will acquire an index
         * shard permit on the primary when executing. If this happens at the same time while we are
         * issuing the synced-flush, the synced-flush request will fail as it thinks there are
         * in-flight operations. We can avoid such situation by continuing issuing another synced-flush
         * if the synced-flush failed due to the ongoing operations on the primary.
         */
        SyncedFlushService service = cluster.getInstance(SyncedFlushService.class);
        AtomicReference<LatchedListener<ShardsSyncedFlushResult>> listenerHolder = new AtomicReference<>();
        assertBusy(() -> {
            LatchedListener<ShardsSyncedFlushResult> listener = new LatchedListener<>();
            listenerHolder.set(listener);
            service.attemptSyncedFlush(shardId, listener);
            listener.latch.await();
            if (listener.result != null && listener.result.failureReason() != null
                && listener.result.failureReason().contains("ongoing operations on primary")) {
                throw new AssertionError(listener.result.failureReason()); // cause the assert busy to retry
            }
        });
        if (listenerHolder.get().error != null) {
            throw ExceptionsHelper.convertToHavenaskException(listenerHolder.get().error);
        }
        return listenerHolder.get().result;
    }

    public static final class LatchedListener<T> implements ActionListener<T> {
        public volatile T result;
        public volatile Exception error;
        public final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onResponse(T result) {
            this.result = result;
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            error = e;
            latch.countDown();
        }
    }

    /**
     * Blocking version of {@link SyncedFlushService#sendPreSyncRequests(List, ClusterState, ShardId, ActionListener)}
     */
    public static Map<String, SyncedFlushService.PreSyncedFlushResponse> sendPreSyncRequests(SyncedFlushService service,
                                                                                             List<ShardRouting> activeShards,
                                                                                             ClusterState state,
                                                                                             ShardId shardId) {
        LatchedListener<Map<String, SyncedFlushService.PreSyncedFlushResponse>> listener = new LatchedListener<>();
        service.sendPreSyncRequests(activeShards, state, shardId, listener);
        try {
            listener.latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (listener.error != null) {
            throw ExceptionsHelper.convertToHavenaskException(listener.error);
        }
        return listener.result;
    }
}
