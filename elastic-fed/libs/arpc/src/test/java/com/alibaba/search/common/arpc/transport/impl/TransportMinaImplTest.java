package com.alibaba.search.common.arpc.transport.impl;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.io.Connection;
import com.alibaba.search.common.arpc.io.MinaServer;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacketStreamer;
import com.alibaba.search.common.arpc.transport.Transport;

public class TransportMinaImplTest extends TestCase {
    private final static Logger logger = LoggerFactory
            .getLogger(TransportMinaImplTest.class);

    private static MinaServer server = new MinaServer();
    private Transport transport = null;
    
    protected void setUp() throws Exception {
        server.start();
        transport = new TransportMinaImpl(new DefaultPacketStreamer());
    }

    protected void tearDown() throws Exception {
        server.unbind();
        Thread.sleep(100);
    }

    public void testDispose() {
        Connection conn = transport.createConnection("127.0.0.1", server.getPort());
        assertTrue(conn.isConnected());
        transport.dispose();
        conn = transport.createConnection("127.0.0.1", server.getPort());
        assertNull(conn);
    }

    public void testCreateConnection() {
        Connection conn = transport.createConnection("127.0.0.1", server.getPort());
        assertTrue(conn.isConnected());
    }
}
