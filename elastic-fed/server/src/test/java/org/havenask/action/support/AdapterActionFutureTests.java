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

import org.havenask.HavenaskException;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.UncategorizedExecutionException;
import org.havenask.test.HavenaskTestCase;
import org.havenask.transport.RemoteTransportException;
import org.havenask.action.support.AdapterActionFuture;

import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AdapterActionFutureTests extends HavenaskTestCase {

    public void testInterruption() throws Exception {
        final AdapterActionFuture<String, Integer> adapter =
                new AdapterActionFuture<String, Integer>() {
                    @Override
                    protected String convert(final Integer listenerResponse) {
                        return Objects.toString(listenerResponse);
                    }
                };

        // test all possible methods that can be interrupted
        final Runnable runnable = () -> {
            final int method = randomIntBetween(0, 4);
            switch (method) {
                case 0:
                    adapter.actionGet();
                    break;
                case 1:
                    adapter.actionGet("30s");
                    break;
                case 2:
                    adapter.actionGet(30000);
                    break;
                case 3:
                    adapter.actionGet(TimeValue.timeValueSeconds(30));
                    break;
                case 4:
                    adapter.actionGet(30, TimeUnit.SECONDS);
                    break;
                default:
                    throw new AssertionError(method);
            }
        };

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread main = Thread.currentThread();
        final Thread thread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            main.interrupt();
        });
        thread.start();

        final AtomicBoolean interrupted = new AtomicBoolean();

        barrier.await();

        try {
            runnable.run();
        } catch (final IllegalStateException e) {
            interrupted.set(Thread.interrupted());
        }
        // we check this here instead of in the catch block to ensure that the catch block executed
        assertTrue(interrupted.get());

        thread.join();
    }

    public void testUnwrapException() {
        checkUnwrap(new RemoteTransportException("test", new RuntimeException()), RuntimeException.class, RemoteTransportException.class);
        checkUnwrap(new RemoteTransportException("test", new Exception()),
            UncategorizedExecutionException.class, RemoteTransportException.class);
        checkUnwrap(new Exception(), UncategorizedExecutionException.class, Exception.class);
        checkUnwrap(new HavenaskException("test", new Exception()), HavenaskException.class, HavenaskException.class);
    }

    private void checkUnwrap(Exception exception, Class<? extends Exception> actionGetException,
                             Class<? extends Exception> getException) {
        final AdapterActionFuture<Void, Void> adapter = new AdapterActionFuture<Void, Void>() {
            @Override
            protected Void convert(Void listenerResponse) {
                fail();
                return null;
            }
        };

        adapter.onFailure(exception);
        assertEquals(actionGetException, expectThrows(RuntimeException.class, adapter::actionGet).getClass());
        assertEquals(actionGetException, expectThrows(RuntimeException.class, () -> adapter.actionGet(10, TimeUnit.SECONDS)).getClass());
        assertEquals(getException, expectThrows(ExecutionException.class, () -> adapter.get()).getCause().getClass());
        assertEquals(getException, expectThrows(ExecutionException.class, () -> adapter.get(10, TimeUnit.SECONDS)).getCause().getClass());
    }
}
