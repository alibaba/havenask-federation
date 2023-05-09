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

package org.havenask.action.support;

import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.cluster.coordination.DeterministicTaskQueue;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.HavenaskRejectedExecutionException;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

import static org.havenask.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RetryableActionTests extends HavenaskTestCase {

    private DeterministicTaskQueue taskQueue;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        taskQueue = new DeterministicTaskQueue(settings, random());
    }

    public void testRetryableActionNoRetries() {
        final AtomicInteger executedCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(logger, taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(30), future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                listener.onResponse(true);
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return true;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertEquals(1, executedCount.get());
        assertTrue(future.actionGet());
    }

    public void testRetryableActionWillRetry() {
        int expectedRetryCount = randomIntBetween(1, 8);
        final AtomicInteger remainingFailedCount = new AtomicInteger(expectedRetryCount);
        final AtomicInteger retryCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(logger, taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(30), future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (remainingFailedCount.getAndDecrement() == 0) {
                    listener.onResponse(true);
                } else {
                    if (randomBoolean()) {
                        listener.onFailure(new HavenaskRejectedExecutionException());
                    } else {
                        throw new HavenaskRejectedExecutionException();
                    }
                }
            }

            @Override
            public boolean shouldRetry(Exception e) {
                retryCount.getAndIncrement();
                return e instanceof HavenaskRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        long previousDeferredTime = 0;
        for (int i = 0; i < expectedRetryCount; ++i) {
            assertTrue(taskQueue.hasDeferredTasks());
            final long deferredExecutionTime = taskQueue.getLatestDeferredExecutionTime();
            final long millisBound = 10 << i;
            assertThat(deferredExecutionTime, lessThanOrEqualTo(millisBound + previousDeferredTime));
            previousDeferredTime = deferredExecutionTime;
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }

        assertEquals(expectedRetryCount, retryCount.get());
        assertTrue(future.actionGet());
    }

    public void testRetryableActionTimeout() {
        final AtomicInteger retryCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(logger, taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(1), future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (randomBoolean()) {
                    listener.onFailure(new HavenaskRejectedExecutionException());
                } else {
                    throw new HavenaskRejectedExecutionException();
                }
            }

            @Override
            public boolean shouldRetry(Exception e) {
                retryCount.getAndIncrement();
                return e instanceof HavenaskRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        long previousDeferredTime = 0;
        while (previousDeferredTime < 1000) {
            assertTrue(taskQueue.hasDeferredTasks());
            previousDeferredTime = taskQueue.getLatestDeferredExecutionTime();
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }

        assertFalse(taskQueue.hasDeferredTasks());
        assertFalse(taskQueue.hasRunnableTasks());

        expectThrows(HavenaskRejectedExecutionException.class, future::actionGet);
    }

    public void testTimeoutOfZeroMeansNoRetry() {
        final AtomicInteger executedCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(logger, taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(0), future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                throw new HavenaskRejectedExecutionException();
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return e instanceof HavenaskRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertEquals(1, executedCount.get());
        expectThrows(HavenaskRejectedExecutionException.class, future::actionGet);
    }

    public void testFailedBecauseNotRetryable() {
        final AtomicInteger executedCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(logger, taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(30), future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                throw new IllegalStateException();
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return e instanceof HavenaskRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertEquals(1, executedCount.get());
        expectThrows(IllegalStateException.class, future::actionGet);
    }

    public void testRetryableActionCancelled() {
        final AtomicInteger executedCount = new AtomicInteger();
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(logger, taskQueue.getThreadPool(),
            TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(30), future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (executedCount.incrementAndGet() == 1) {
                    throw new HavenaskRejectedExecutionException();
                } else {
                    listener.onResponse(true);
                }
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return e instanceof HavenaskRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();

        retryableAction.cancel(new HavenaskException("Cancelled"));
        taskQueue.runAllRunnableTasks();

        // A second run will not occur because it is cancelled
        assertEquals(1, executedCount.get());
        expectThrows(HavenaskException.class, future::actionGet);
    }
}
