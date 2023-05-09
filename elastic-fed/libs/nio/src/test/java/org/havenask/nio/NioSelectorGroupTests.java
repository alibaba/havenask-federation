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

package org.havenask.nio;

import org.havenask.common.CheckedRunnable;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static org.havenask.common.util.concurrent.HavenaskExecutors.daemonThreadFactory;
import static org.mockito.Mockito.mock;

public class NioSelectorGroupTests extends HavenaskTestCase {

    private NioSelectorGroup nioGroup;

    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        nioGroup = new NioSelectorGroup(daemonThreadFactory(Settings.EMPTY, "acceptor"), 1,
            daemonThreadFactory(Settings.EMPTY, "selector"), 1, (s) -> new EventHandler(mock(Consumer.class), s));
    }

    @Override
    public void tearDown() throws Exception {
        nioGroup.close();
        super.tearDown();
    }

    public void testStartAndClose() throws IOException {
        // ctor starts threads. So we are testing that close() stops the threads. Our thread linger checks
        // will throw an exception is stop fails
        nioGroup.close();
    }

    @SuppressWarnings("unchecked")
    public void testCannotOperateAfterClose() throws IOException {
        nioGroup.close();

        IllegalStateException ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.bindServerChannel(mock(InetSocketAddress.class), mock(ChannelFactory.class)));
        assertEquals("NioGroup is closed.", ise.getMessage());
        ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.openChannel(mock(InetSocketAddress.class), mock(ChannelFactory.class)));
        assertEquals("NioGroup is closed.", ise.getMessage());
    }

    public void testCanCloseTwice() throws IOException {
        nioGroup.close();
        nioGroup.close();
    }

    @SuppressWarnings("unchecked")
    public void testExceptionAtStartIsHandled() throws IOException {
        RuntimeException ex = new RuntimeException();
        CheckedRunnable<IOException> ctor = () -> new NioSelectorGroup(r -> {throw ex;}, 1,
            daemonThreadFactory(Settings.EMPTY, "selector"),
            1, (s) -> new EventHandler(mock(Consumer.class), s));
        RuntimeException runtimeException = expectThrows(RuntimeException.class, ctor::run);
        assertSame(ex, runtimeException);
        // ctor starts threads. So we are testing that a failure to construct will stop threads. Our thread
        // linger checks will throw an exception is stop fails
    }
}
