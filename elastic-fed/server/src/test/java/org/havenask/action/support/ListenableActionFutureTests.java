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

package org.havenask.action.support;

import org.havenask.action.ActionListener;
import org.havenask.common.util.concurrent.AbstractRunnable;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.Transports;
import org.havenask.action.support.PlainListenableActionFuture;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ListenableActionFutureTests extends HavenaskTestCase {

    public void testListenerIsCallableFromNetworkThreads() throws Throwable {
        ThreadPool threadPool = new TestThreadPool("testListenerIsCallableFromNetworkThreads");
        try {
            final PlainListenableActionFuture<Object> future = PlainListenableActionFuture.newListenableFuture();
            final CountDownLatch listenerCalled = new CountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final Object response = new Object();
            future.addListener(new ActionListener<Object>() {
                @Override
                public void onResponse(Object o) {
                    listenerCalled.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    error.set(e);
                    listenerCalled.countDown();
                }
            });
            Thread networkThread = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    error.set(e);
                    listenerCalled.countDown();
                }

                @Override
                protected void doRun() throws Exception {
                    future.onResponse(response);
                }
            }, Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX + "_testListenerIsCallableFromNetworkThread");
            networkThread.start();
            networkThread.join();
            listenerCalled.await();
            if (error.get() != null) {
                throw error.get();
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }


}
