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

package org.havenask.action.support.replication;

import org.havenask.action.ActionListener;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.action.support.RetryableAction;
import org.havenask.common.UUIDs;
import org.havenask.common.unit.TimeValue;
import org.havenask.index.shard.IndexShardClosedException;
import org.havenask.index.shard.ShardId;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;

import java.util.Collections;

public class PendingReplicationActionsTests extends HavenaskTestCase {

    private TestThreadPool threadPool;
    private ShardId shardId;
    private PendingReplicationActions pendingReplication;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId("index", UUIDs.randomBase64UUID(), 0);
        threadPool = new TestThreadPool(getTestName());
        pendingReplication = new PendingReplicationActions(shardId, threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testAllocationIdActionCanBeRun() {
        String allocationId = UUIDs.randomBase64UUID();
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        pendingReplication.acceptNewTrackedAllocationIds(Collections.singleton(allocationId));
        TestAction action = new TestAction(future);
        pendingReplication.addPendingAction(allocationId, action);
        action.run();
        future.actionGet();
        assertTrue(future.isDone());
    }

    public void testMissingAllocationIdActionWillBeCancelled() {
        String allocationId = UUIDs.randomBase64UUID();
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        TestAction action = new TestAction(future);
        pendingReplication.addPendingAction(allocationId, action);
        expectThrows(IndexShardClosedException.class, future::actionGet);
    }

    public void testAllocationIdActionWillBeCancelledIfTrackedAllocationChanges() {
        String allocationId = UUIDs.randomBase64UUID();
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        pendingReplication.acceptNewTrackedAllocationIds(Collections.singleton(allocationId));
        TestAction action = new TestAction(future, false);
        pendingReplication.addPendingAction(allocationId, action);
        action.run();
        pendingReplication.acceptNewTrackedAllocationIds(Collections.emptySet());
        expectThrows(IndexShardClosedException.class, future::actionGet);
    }

    public void testAllocationIdActionWillBeCancelledOnClose() {
        String allocationId = UUIDs.randomBase64UUID();
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        pendingReplication.acceptNewTrackedAllocationIds(Collections.singleton(allocationId));
        TestAction action = new TestAction(future, false);
        pendingReplication.addPendingAction(allocationId, action);
        action.run();
        pendingReplication.close();
        expectThrows(IndexShardClosedException.class, future::actionGet);
    }

    private class TestAction extends RetryableAction<Void> {

        private final boolean succeed;
        private final Exception retryable = new Exception();

        private TestAction(ActionListener<Void> listener) {
            this(listener, true);
        }

        private TestAction(ActionListener<Void> listener, boolean succeed) {
            super(logger, threadPool, TimeValue.timeValueMillis(1), TimeValue.timeValueMinutes(1), listener);
            this.succeed = succeed;
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            if (succeed) {
                listener.onResponse(null);
            } else {
                listener.onFailure(retryable);
            }
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return retryable == e;
        }
    }
}
