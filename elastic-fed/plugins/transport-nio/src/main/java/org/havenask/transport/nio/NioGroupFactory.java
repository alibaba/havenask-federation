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

package org.havenask.transport.nio;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.AbstractRefCounted;
import org.havenask.http.HttpServerTransport;
import org.havenask.nio.ChannelFactory;
import org.havenask.nio.EventHandler;
import org.havenask.nio.NioGroup;
import org.havenask.nio.NioSelectorGroup;
import org.havenask.nio.NioServerSocketChannel;
import org.havenask.nio.NioSocketChannel;
import org.havenask.transport.TcpTransport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.havenask.common.util.concurrent.HavenaskExecutors.daemonThreadFactory;

/**
 * Creates and returns {@link NioSelectorGroup} instances. It will return a shared group for
 * both {@link #getHttpGroup()} and {@link #getTransportGroup()} if
 * {@link NioTransportPlugin#NIO_HTTP_WORKER_COUNT} is configured to be 0. If that setting is not 0, then it
 * will return a different group in the {@link #getHttpGroup()} call.
 */
public final class NioGroupFactory {

    private final Logger logger;
    private final Settings settings;
    private final int httpWorkerCount;

    private RefCountedNioGroup refCountedGroup;

    public NioGroupFactory(Settings settings, Logger logger) {
        this.logger = logger;
        this.settings = settings;
        this.httpWorkerCount = NioTransportPlugin.NIO_HTTP_WORKER_COUNT.get(settings);
    }

    public Settings getSettings() {
        return settings;
    }

    public synchronized NioGroup getTransportGroup() throws IOException {
        return getGenericGroup();
    }

    public synchronized NioGroup getHttpGroup() throws IOException {
        if (httpWorkerCount == 0) {
            return getGenericGroup();
        } else {
            return new NioSelectorGroup(daemonThreadFactory(this.settings, HttpServerTransport.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX),
                httpWorkerCount, (s) -> new EventHandler(this::onException, s));
        }
    }

    private NioGroup getGenericGroup() throws IOException {
        if (refCountedGroup == null) {
            ThreadFactory threadFactory = daemonThreadFactory(this.settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX);
            NioSelectorGroup nioGroup = new NioSelectorGroup(threadFactory, NioTransportPlugin.NIO_WORKER_COUNT.get(settings),
                (s) -> new EventHandler(this::onException, s));
            this.refCountedGroup = new RefCountedNioGroup(nioGroup);
            return new WrappedNioGroup(refCountedGroup);
        } else {
            refCountedGroup.incRef();
            return new WrappedNioGroup(refCountedGroup);
        }
    }

    private void onException(Exception exception) {
        logger.warn(new ParameterizedMessage("exception caught on transport layer [thread={}]", Thread.currentThread().getName()),
            exception);
    }

    private static class RefCountedNioGroup extends AbstractRefCounted implements NioGroup {

        public static final String NAME = "ref-counted-nio-group";
        private final NioSelectorGroup nioGroup;

        private RefCountedNioGroup(NioSelectorGroup nioGroup) {
            super(NAME);
            this.nioGroup = nioGroup;
        }

        @Override
        protected void closeInternal() {
            try {
                nioGroup.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory)
            throws IOException {
            return nioGroup.bindServerChannel(address, factory);
        }

        @Override
        public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
            return nioGroup.openChannel(address, factory);
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException("Should not close. Instead use decRef call.");
        }
    }

    /**
     * Wraps the {@link RefCountedNioGroup}. Calls {@link RefCountedNioGroup#decRef()} on close. After close,
     * this wrapped instance can no longer be used.
     */
    private static class WrappedNioGroup implements NioGroup {

        private final RefCountedNioGroup refCountedNioGroup;

        private final AtomicBoolean isOpen = new AtomicBoolean(true);

        private WrappedNioGroup(RefCountedNioGroup refCountedNioGroup) {
            this.refCountedNioGroup = refCountedNioGroup;
        }

        public <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory)
            throws IOException {
            ensureOpen();
            return refCountedNioGroup.bindServerChannel(address, factory);
        }

        public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
            ensureOpen();
            return refCountedNioGroup.openChannel(address, factory);
        }

        @Override
        public void close() {
            if (isOpen.compareAndSet(true, false)) {
                refCountedNioGroup.decRef();
            }
        }

        private void ensureOpen() {
            if (isOpen.get() == false) {
                throw new IllegalStateException("NioGroup is closed.");
            }
        }
    }
}
