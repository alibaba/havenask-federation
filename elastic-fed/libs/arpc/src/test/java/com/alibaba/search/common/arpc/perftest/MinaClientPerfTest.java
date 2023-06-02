package com.alibaba.search.common.arpc.perftest;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.io.Connection;
import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHandler;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacket;
import com.alibaba.search.common.arpc.transport.Transport;
import com.alibaba.search.common.arpc.transport.impl.DefaultTransportFactory;

public class MinaClientPerfTest {
    private final static Logger logger = LoggerFactory
            .getLogger(MinaClientPerfTest.class);
    
    private static String host;
    private static int port;
    private static int packet_count;
    private static int packet_size;
    
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: host port packet_count packet_size");
            System.exit(-1);
        }
        host = args[0];
        port = Integer.valueOf(args[1]);
        packet_count = Integer.valueOf(args[2]);
        packet_size = Integer.valueOf(args[3]);
        Transport transport = new DefaultTransportFactory().createTransport();
        Connection conn = transport.createConnection(host, port);
        PacketHandler handler = new MinaClientPerfTest.SimpleHandler();
        
        ByteBuffer bb = ByteBuffer.allocate(packet_size);
        DefaultPacket packet = new DefaultPacket();

        for (int i = 1; i <= packet_count; ++i) {
            bb.clear();
            bb.putInt(i);
            bb.putInt(packet_size);
            packet.setBody(bb.array());
            
            if (!conn.postPacket(packet, handler, null, 1000, true)) {
                System.out.println("post failed");
            }
        }
    }
    
    static class SimpleHandler implements PacketHandler {

        private int expect = 1;
        private long begin = 0;
        private long end = 0;
        
        public void handlePacket(Packet packet, Object args) {
            if (packet.isRegularPacket()) {
                DefaultPacket reply = (DefaultPacket) packet;
                byte[] body = reply.getBody();
                ByteBuffer bb = ByteBuffer.wrap(body);
                int id = bb.getInt();
                int size = bb.getInt();

                if (id != expect) {
                    System.err.println("!!!id not expect, expect:" + expect
                            + " actual:" + id);
                }
                if (size != body.length) {
                    System.err.println("!!!length not expect, expect:" + size
                            + " actual:" + body.length);
                }
                if (id == 1) {
                    begin = System.currentTimeMillis();
                }
                if (id == packet_count) {
                    end = System.currentTimeMillis();
                    System.out.println("posted:" + packet_count);
                    System.out.println("packet size: " + packet_size + " bytes");
                    System.out.println("time: " + (end - begin) + "ms");
                    System.out.println("qps: " + 1000L * packet_count / (end - begin));
                    System.exit(0);
                }
                expect++;
            } else {
                System.out.println("error: receive control packet");
                System.exit(-1);
            }

        }
    }
}
