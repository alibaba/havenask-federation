package com.alibaba.search.common.arpc.packet.impl;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.exceptions.ArpcInvalidPacketHeaderException;
import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHeader;
import com.alibaba.search.common.arpc.packet.PacketStreamer;
import com.alibaba.search.common.arpc.packet.StreamerContext;
import com.alibaba.search.common.arpc.util.Constants;
import com.alibaba.search.common.arpc.util.DataBuffer;

public class DefaultPacketStreamerTest extends TestCase {
    private final static Logger logger = LoggerFactory
            .getLogger(DefaultPacketStreamerTest.class);
    
    private PacketStreamer packetStreamer;
    
    protected void setUp() throws Exception {
        super.setUp();
        packetStreamer = new DefaultPacketStreamer();
    }

    public void testGetPacketHeader() {
        DataBuffer input = new DataBuffer();
        PacketHeader packetHeader;
        try {
            packetHeader = packetStreamer.getPacketHeader(input);
            assertNull(packetHeader);
        } catch (ArpcInvalidPacketHeaderException e) {
            fail("unexpected to get exception");
        }
        
        input.writeInt(Constants.ANET_PACKET_FLAG);
        input.writeInt(1);
        input.writeInt(2);
        input.writeInt(3);
        try {
            packetHeader = packetStreamer.getPacketHeader(input);
            assertNotNull(packetHeader);
            assertEquals(1, packetHeader.getPacketId());
            assertEquals(2, packetHeader.getPcode());
            assertEquals(3, packetHeader.getBodyLen());
        } catch (ArpcInvalidPacketHeaderException e) {
            fail("unexpected to get exception");
        }
        
        input.clear();
        input.writeInt(Constants.ANET_PACKET_FLAG);
        input.writeInt(1);
        input.writeInt(2);
        input.writeInt(-1);
        try {
            packetHeader = packetStreamer.getPacketHeader(input);
            fail("get packet header should throw exception");
        } catch (ArpcInvalidPacketHeaderException e) {}
        
        input.clear();
        input.writeInt(Constants.ANET_PACKET_FLAG);
        input.writeInt(1);
        input.writeInt(2);
        input.writeInt(0x7fffffff);
        try {
            packetHeader = packetStreamer.getPacketHeader(input);
            fail("get packet header should throw exception");
        } catch (ArpcInvalidPacketHeaderException e) {}
        
        input.clear();
        input.writeInt(-1);
        input.writeInt(1);
        input.writeInt(2);
        input.writeInt(3);
        try {
            packetHeader = packetStreamer.getPacketHeader(input);
            fail("get packet header should throw exception");
        } catch (ArpcInvalidPacketHeaderException e) {}
    }
    
    public void testDecode() {
        DataBuffer input = new DataBuffer();
        StreamerContext context = new StreamerContext();
        
        try {
            packetStreamer.decode(input, context);
            assertFalse(context.isCompleted());
        } catch (ArpcInvalidPacketHeaderException e) {
            fail();
        }
        
        input.writeInt(Constants.ANET_PACKET_FLAG);
        input.writeInt(1); // pid
        input.writeInt(2); // pcode
        input.writeInt(10); // bodyLen
        byte[] bytes = new byte[5];
        for (int i = 1; i <= bytes.length; ++i) {
            bytes[i - 1] = (byte)i;
        }
        input.write(bytes);
        try {
            packetStreamer.decode(input, context);
            assertFalse(context.isCompleted());
            Packet packet = context.getPacket();
            PacketHeader header = packet.getHeader();
            assertEquals(1, header.getPacketId());
            assertEquals(2, header.getPcode());
            assertEquals(10, header.getBodyLen());
        } catch (ArpcInvalidPacketHeaderException e) {
            fail();
        }
        
        bytes = new byte[10];
        for (int i = 1; i <= bytes.length; ++i) {
            bytes[i - 1] = (byte)(i + 5);
        }
        input.write(bytes);
        try {
            packetStreamer.decode(input, context);
            assertTrue(context.isCompleted());
            Packet packet = context.getPacket();
            PacketHeader header = packet.getHeader();
            assertEquals(1, header.getPacketId());
            assertEquals(2, header.getPcode());
            assertEquals(10, header.getBodyLen());
            byte[] body = ((DefaultPacket)packet).getBody();
            assertEquals(10, body.length);
            for (int i = 1; i <= body.length; ++i) {
                assertEquals(i, body[i - 1]);
            }
        } catch (ArpcInvalidPacketHeaderException e) {
            fail();
        }
    }
    
    public void testEncode() {
        DefaultPacket packet = new DefaultPacket();
        packet.getHeader().setPacketId(1);
        packet.getHeader().setPcode(2);
        byte[] body = {100, 110};
        packet.setBody(body);
        byte[] buf = packetStreamer.encode(packet);
        ByteBuffer bb = ByteBuffer.wrap(buf);
        assertEquals(Constants.ANET_PACKET_FLAG, bb.getInt());
        assertEquals(1, bb.getInt());
        assertEquals(2, bb.getInt());
        assertEquals(2, bb.getInt());
        assertEquals(100, bb.get());
        assertEquals(110, bb.get());
        assertEquals(0, bb.remaining());
    }
}
