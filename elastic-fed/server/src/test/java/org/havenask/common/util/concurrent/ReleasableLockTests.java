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

package org.havenask.common.util.concurrent;

import org.havenask.common.lease.Releasable;
import org.havenask.common.unit.TimeValue;
import org.havenask.test.HavenaskTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ReleasableLockTests extends HavenaskTestCase {

    /**
     * Test that accounting on whether or not a thread holds a releasable lock is correct. Previously we had a bug where on a re-entrant
     * lock that if a thread entered the lock twice we would declare that it does not hold the lock after it exits its first entrance but
     * not its second entrance.
     *
     * @throws BrokenBarrierException if awaiting on the synchronization barrier breaks
     * @throws InterruptedException   if awaiting on the synchronization barrier is interrupted
     */
    public void testIsHeldByCurrentThread() throws BrokenBarrierException, InterruptedException {
        final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        final ReleasableLock readLock = new ReleasableLock(readWriteLock.readLock());
        final ReleasableLock writeLock = new ReleasableLock(readWriteLock.writeLock());

        final int numberOfThreads = scaledRandomIntBetween(1, 32);
        final int iterations = scaledRandomIntBetween(1, 32);
        final CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (int j = 0; j < iterations; j++) {
                    if (randomBoolean()) {
                        acquire(readLock, writeLock);
                    } else {
                        acquire(writeLock, readLock);
                    }
                }
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(thread);
            thread.start();
        }

        barrier.await();
        barrier.await();
        for (final Thread thread : threads) {
            thread.join();
        }
    }

    private void acquire(final ReleasableLock lockToAcquire, final ReleasableLock otherLock) {
        try (@SuppressWarnings("unused") Releasable outer = randomAcquireMethod(lockToAcquire)) {
            assertTrue(lockToAcquire.isHeldByCurrentThread());
            assertFalse(otherLock.isHeldByCurrentThread());
            try (@SuppressWarnings("unused") Releasable inner = randomAcquireMethod(lockToAcquire)) {
                assertTrue(lockToAcquire.isHeldByCurrentThread());
                assertFalse(otherLock.isHeldByCurrentThread());
            }
            // previously there was a bug here and this would return false
            assertTrue(lockToAcquire.isHeldByCurrentThread());
            assertFalse(otherLock.isHeldByCurrentThread());
        }
        assertFalse(lockToAcquire.isHeldByCurrentThread());
        assertFalse(otherLock.isHeldByCurrentThread());
    }

    private ReleasableLock randomAcquireMethod(ReleasableLock lock) {
        if (randomBoolean()) {
            return lock.acquire();
        } else {
            try {
                ReleasableLock releasableLock = lock.tryAcquire(TimeValue.timeValueSeconds(30));
                assertThat(releasableLock, notNullValue());
                return releasableLock;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    public void testTryAcquire() throws Exception {
        ReleasableLock lock = new ReleasableLock(new ReentrantLock());
        int numberOfThreads = randomIntBetween(1, 10);
        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        AtomicInteger lockedCounter = new AtomicInteger();
        int timeout = randomFrom(0, 5, 10);
        List<Thread> threads =
            IntStream.range(0, numberOfThreads).mapToObj(i -> new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    try (ReleasableLock locked = lock.tryAcquire(TimeValue.timeValueMillis(timeout))) {
                        if (locked != null) {
                            lockedCounter.incrementAndGet();
                        }
                    }
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new AssertionError(e);
                }
            })).collect(Collectors.toList());
        threads.forEach(Thread::start);
        try (ReleasableLock locked = randomBoolean() ? lock.acquire() : null) {
            barrier.await(10, TimeUnit.SECONDS);
            for (Thread thread : threads) {
                thread.join(10000);
            }
            threads.forEach(t -> assertThat(t.isAlive(), is(false)));

            if (locked != null) {
                assertThat(lockedCounter.get(), equalTo(0));
            } else {
                assertThat(lockedCounter.get(), greaterThanOrEqualTo(1));
            }
        }

        try (ReleasableLock locked = lock.tryAcquire(TimeValue.ZERO)) {
            assertThat(locked, notNullValue());
        }
    }
}
