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

package org.havenask.http.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.common.network.NetworkService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.util.BigArrays;
import org.havenask.common.util.PageCacheRecycler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.http.AbstractHttpServerTransport;
import org.havenask.http.HttpChannel;
import org.havenask.http.HttpServerChannel;
import org.havenask.nio.BytesChannelContext;
import org.havenask.nio.ChannelFactory;
import org.havenask.nio.Config;
import org.havenask.nio.InboundChannelBuffer;
import org.havenask.nio.NioGroup;
import org.havenask.nio.NioSelector;
import org.havenask.nio.NioSocketChannel;
import org.havenask.nio.ServerChannelContext;
import org.havenask.nio.SocketChannelContext;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.nio.NioGroupFactory;
import org.havenask.transport.nio.PageAllocator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_COUNT;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_IDLE;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_INTERVAL;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS;
import static org.havenask.http.HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE;
import static org.havenask.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;

public class NioHttpServerTransport extends AbstractHttpServerTransport {
    private static final Logger logger = LogManager.getLogger(NioHttpServerTransport.class);

    protected final PageAllocator pageAllocator;
    private final NioGroupFactory nioGroupFactory;

    protected final boolean tcpNoDelay;
    protected final boolean tcpKeepAlive;
    protected final int tcpKeepIdle;
    protected final int tcpKeepInterval;
    protected final int tcpKeepCount;
    protected final boolean reuseAddress;
    protected final int tcpSendBufferSize;
    protected final int tcpReceiveBufferSize;

    private volatile NioGroup nioGroup;
    private ChannelFactory<NioHttpServerChannel, NioHttpChannel> channelFactory;

    public NioHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays,
                                  PageCacheRecycler pageCacheRecycler, ThreadPool threadPool, NamedXContentRegistry xContentRegistry,
                                  Dispatcher dispatcher, NioGroupFactory nioGroupFactory, ClusterSettings clusterSettings) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher, clusterSettings);
        this.pageAllocator = new PageAllocator(pageCacheRecycler);
        this.nioGroupFactory = nioGroupFactory;

        ByteSizeValue maxChunkSize = SETTING_HTTP_MAX_CHUNK_SIZE.get(settings);
        ByteSizeValue maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        ByteSizeValue maxInitialLineLength = SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings);
        int pipeliningMaxEvents = SETTING_PIPELINING_MAX_EVENTS.get(settings);

        this.tcpNoDelay = SETTING_HTTP_TCP_NO_DELAY.get(settings);
        this.tcpKeepAlive = SETTING_HTTP_TCP_KEEP_ALIVE.get(settings);
        this.tcpKeepIdle = SETTING_HTTP_TCP_KEEP_IDLE.get(settings);
        this.tcpKeepInterval = SETTING_HTTP_TCP_KEEP_INTERVAL.get(settings);
        this.tcpKeepCount = SETTING_HTTP_TCP_KEEP_COUNT.get(settings);
        this.reuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
        this.tcpSendBufferSize = Math.toIntExact(SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings).getBytes());
        this.tcpReceiveBufferSize = Math.toIntExact(SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings).getBytes());


        logger.debug("using max_chunk_size[{}], max_header_size[{}], max_initial_line_length[{}], max_content_length[{}]," +
                " pipelining_max_events[{}]",
            maxChunkSize, maxHeaderSize, maxInitialLineLength, maxContentLength, pipeliningMaxEvents);
    }

    public Logger getLogger() {
        return logger;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            nioGroup = nioGroupFactory.getHttpGroup();
            channelFactory = channelFactory();
            bindServer();
            success = true;
        } catch (IOException e) {
            throw new HavenaskException(e);
        } finally {
            if (success == false) {
                doStop(); // otherwise we leak threads since we never moved to started
            }
        }
    }

    @Override
    protected void stopInternal() {
        try {
            nioGroup.close();
        } catch (Exception e) {
            logger.warn("unexpected exception while stopping nio group", e);
        }
    }

    @Override
    protected HttpServerChannel bind(InetSocketAddress socketAddress) throws IOException {
        NioHttpServerChannel httpServerChannel = nioGroup.bindServerChannel(socketAddress, channelFactory);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        httpServerChannel.addBindListener(ActionListener.toBiConsumer(future));
        future.actionGet();
        return httpServerChannel;
    }

    protected ChannelFactory<NioHttpServerChannel, NioHttpChannel> channelFactory() {
        return new HttpChannelFactory();
    }

    protected void acceptChannel(NioSocketChannel socketChannel) {
        super.serverAcceptedChannel((HttpChannel) socketChannel);
    }

    private class HttpChannelFactory extends ChannelFactory<NioHttpServerChannel, NioHttpChannel> {

        private HttpChannelFactory() {
            super(tcpNoDelay, tcpKeepAlive, tcpKeepIdle, tcpKeepInterval, tcpKeepCount, reuseAddress, tcpSendBufferSize,
                tcpReceiveBufferSize);
        }

        @Override
        public NioHttpChannel createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) {
            NioHttpChannel httpChannel = new NioHttpChannel(channel);
            HttpReadWriteHandler handler = new HttpReadWriteHandler(httpChannel,NioHttpServerTransport.this,
                handlingSettings, selector.getTaskScheduler(), threadPool::relativeTimeInMillis);
            Consumer<Exception> exceptionHandler = (e) -> onException(httpChannel, e);
            SocketChannelContext context = new BytesChannelContext(httpChannel, selector, socketConfig, exceptionHandler, handler,
                new InboundChannelBuffer(pageAllocator));
            httpChannel.setContext(context);
            return httpChannel;
        }

        @Override
        public NioHttpServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel,
                                                        Config.ServerSocket socketConfig) {
            NioHttpServerChannel httpServerChannel = new NioHttpServerChannel(channel);
            Consumer<Exception> exceptionHandler = (e) -> onServerException(httpServerChannel, e);
            Consumer<NioSocketChannel> acceptor = NioHttpServerTransport.this::acceptChannel;
            ServerChannelContext context = new ServerChannelContext(httpServerChannel, this, selector, socketConfig, acceptor,
                exceptionHandler);
            httpServerChannel.setContext(context);
            return httpServerChannel;
        }

    }
}
