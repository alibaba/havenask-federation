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

package org.havenask.cluster.coordination;

import org.havenask.common.util.concurrent.HavenaskExecutors;
import org.havenask.common.util.concurrent.PrioritizedHavenaskThreadPoolExecutor;
import org.havenask.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * Mock single threaded {@link PrioritizedHavenaskThreadPoolExecutor} based on {@link DeterministicTaskQueue},
 * simulating the behaviour of an executor returned by {@link HavenaskExecutors#newSinglePrioritizing}.
 */
public class MockSinglePrioritizingExecutor extends PrioritizedHavenaskThreadPoolExecutor {

    public MockSinglePrioritizingExecutor(String name, DeterministicTaskQueue deterministicTaskQueue, ThreadPool threadPool) {
        super(name, 0, 1, 0L, TimeUnit.MILLISECONDS,
            r -> new Thread() {
                @Override
                public void start() {
                    deterministicTaskQueue.scheduleNow(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                r.run();
                            } catch (KillWorkerError kwe) {
                                // hacks everywhere
                            }
                        }

                        @Override
                        public String toString() {
                            return r.toString();
                        }
                    });
                }
            },
            threadPool.getThreadContext(), threadPool.scheduler());
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        // kill worker so that next one will be scheduled, using cached Error instance to not incur the cost of filling in the stack trace
        // on every task
        throw KillWorkerError.INSTANCE;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        // ensures we don't block
        return false;
    }

    private static final class KillWorkerError extends Error {
        private static final KillWorkerError INSTANCE = new KillWorkerError();
    }
}
