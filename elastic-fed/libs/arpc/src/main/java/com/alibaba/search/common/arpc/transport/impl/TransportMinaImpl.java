/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.alibaba.search.common.arpc.transport.impl;

import java.net.InetSocketAddress;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.io.Connection;
import com.alibaba.search.common.arpc.io.IoComponent;
import com.alibaba.search.common.arpc.io.impl.IoComponentMinaImpl;
import com.alibaba.search.common.arpc.io.impl.MinaSessionHandler;
import com.alibaba.search.common.arpc.packet.PacketStreamer;
import com.alibaba.search.common.arpc.transport.Transport;
import com.alibaba.search.common.arpc.util.Constants;

public class TransportMinaImpl extends Transport {
    private final static Logger logger = LoggerFactory
            .getLogger(TransportMinaImpl.class);
    
    private IoConnector connector;

    public TransportMinaImpl(PacketStreamer packetStreamer) {
        super(packetStreamer);
        connector = new NioSocketConnector();
        connector.setHandler(new MinaSessionHandler());
    }
            
    @Override
    protected Connection doCreateConnection(String host, int port) {
        IoSession session;
        try {
            ConnectFuture future = connector.connect(new InetSocketAddress(
                    host, port));
            future.awaitUninterruptibly();
            session = future.getSession();
        } catch (Exception e) {
            logger.error("Failed to connect {}:{}", host, port);
            logger.error("Exception: ", e);
            return null;
        }
        IoComponent ioComponent = new IoComponentMinaImpl(session);
        session.setAttribute(Constants.IO_COMPONENT, ioComponent);
        Connection conn = new Connection(ioComponent, packetStreamer);
        ioComponent.setIoHandler(conn);
        return conn;
    }

    @Override
    public void dispose() {
        super.dispose();
        connector.dispose();    
    }  
    
}