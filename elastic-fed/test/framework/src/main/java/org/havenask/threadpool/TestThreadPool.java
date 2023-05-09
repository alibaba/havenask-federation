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
import org.havenask.common.util.concurrent.HavenaskExecutors;
import org.havenask.node.Node;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

public class TestThreadPool extends ThreadPool {

    private final CountDownLatch blockingLatch = new CountDownLatch(1);
    private volatile boolean returnRejectingExecutor = false;
    private volatile ThreadPoolExecutor rejectingExecutor;

    public TestThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
        this(name, Settings.EMPTY, customBuilders);
    }

    public TestThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
        super(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).put(settings).build(), customBuilders);
    }

    @Override
    public ExecutorService executor(String name) {
        if (returnRejectingExecutor) {
            return rejectingExecutor;
        } else {
            return super.executor(name);
        }
    }

    public void startForcingRejections() {
        if (rejectingExecutor == null) {
            createRejectingExecutor();
        }
        returnRejectingExecutor = true;
    }

    public void stopForcingRejections() {
        returnRejectingExecutor = false;
    }

    @Override
    public void shutdown() {
        blockingLatch.countDown();
        if (rejectingExecutor != null) {
            rejectingExecutor.shutdown();
        }
        super.shutdown();
    }

    @Override
    public void shutdownNow() {
        blockingLatch.countDown();
        if (rejectingExecutor != null) {
            rejectingExecutor.shutdownNow();
        }
        super.shutdownNow();
    }

    private synchronized void createRejectingExecutor() {
        if (rejectingExecutor != null) {
            return;
        }
        ThreadFactory factory = HavenaskExecutors.daemonThreadFactory("reject_thread");
        rejectingExecutor = HavenaskExecutors.newFixed("rejecting", 1, 0, factory, getThreadContext());

        CountDownLatch startedLatch = new CountDownLatch(1);
        rejectingExecutor.execute(() -> {
            try {
                startedLatch.countDown();
                blockingLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            startedLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
