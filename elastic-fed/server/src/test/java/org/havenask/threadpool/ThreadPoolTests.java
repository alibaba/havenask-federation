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

package org.havenask.threadpool;

import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.HavenaskExecutors;
import org.havenask.common.util.concurrent.FutureUtils;
import org.havenask.test.HavenaskTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.havenask.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.havenask.threadpool.ThreadPool.assertCurrentMethodIsNotCalledRecursively;
import static org.hamcrest.CoreMatchers.equalTo;

public class ThreadPoolTests extends HavenaskTestCase {

    public void testBoundedByBelowMin() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(Integer.MIN_VALUE, min - 1);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(min));
    }

    public void testBoundedByAboveMax() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(max + 1, Integer.MAX_VALUE);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(max));
    }

    public void testBoundedByBetweenMinAndMax() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(min, max);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(value));
    }

    public void testAbsoluteTime() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            long currentTime = System.currentTimeMillis();
            long gotTime = threadPool.absoluteTimeInMillis();
            long delta = Math.abs(gotTime - currentTime);
            // the delta can be large, we just care it is the same order of magnitude
            assertTrue("thread pool cached absolute time " + gotTime + " is too far from real current time " + currentTime, delta < 10000);
        } finally {
            terminate(threadPool);
        }
    }

    public void testEstimatedTimeIntervalSettingAcceptsOnlyZeroAndPositiveTime() {
        Settings settings = Settings.builder().put("thread_pool.estimated_time_interval", -1).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> ESTIMATED_TIME_INTERVAL_SETTING.get(settings));
        assertEquals("failed to parse value [-1] for setting [thread_pool.estimated_time_interval], must be >= [0ms]", e.getMessage());
    }

    int factorial(int n) {
        assertCurrentMethodIsNotCalledRecursively();
        if (n <= 1) {
            return 1;
        } else {
            return n * factorial(n - 1);
        }
    }

    int factorialForked(int n, ExecutorService executor) {
        assertCurrentMethodIsNotCalledRecursively();
        if (n <= 1) {
            return 1;
        }
        return n * FutureUtils.get(executor.submit(() -> factorialForked(n - 1, executor)));
    }

    public void testAssertCurrentMethodIsNotCalledRecursively() {
        expectThrows(AssertionError.class, () -> factorial(between(2, 10)));
        assertThat(factorial(1), equalTo(1)); // is not called recursively
        assertThat(expectThrows(AssertionError.class, () -> factorial(between(2, 10))).getMessage(),
            equalTo("org.havenask.threadpool.ThreadPoolTests#factorial is called recursively"));
        TestThreadPool threadPool = new TestThreadPool("test");
        assertThat(factorialForked(1, threadPool.generic()), equalTo(1));
        assertThat(factorialForked(10, threadPool.generic()), equalTo(3628800));
        assertThat(expectThrows(AssertionError.class,
            () -> factorialForked(between(2, 10), HavenaskExecutors.newDirectExecutorService())).getMessage(),
            equalTo("org.havenask.threadpool.ThreadPoolTests#factorialForked is called recursively"));
        terminate(threadPool);
    }

    public void testInheritContextOnSchedule() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch executed = new CountDownLatch(1);

        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            threadPool.getThreadContext().putHeader("foo", "bar");
            final Integer one = Integer.valueOf(1);
            threadPool.getThreadContext().putTransient("foo", one);
            threadPool.schedule(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                assertEquals(threadPool.getThreadContext().getHeader("foo"), "bar");
                assertSame(threadPool.getThreadContext().getTransient("foo"), one);
                assertNull(threadPool.getThreadContext().getHeader("bar"));
                assertNull(threadPool.getThreadContext().getTransient("bar"));
                executed.countDown();
            }, TimeValue.timeValueMillis(randomInt(100)), randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC));
            threadPool.getThreadContext().putTransient("bar", "boom");
            threadPool.getThreadContext().putHeader("bar", "boom");
            latch.countDown();
            executed.await();
        } finally {
            latch.countDown();
            terminate(threadPool);
        }
    }
}
