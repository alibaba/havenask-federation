package com.alibaba.search.common.arpc.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHandler;
import com.alibaba.search.common.arpc.packet.PacketManager;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket.CmdType;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacket;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacketStreamer;
import com.alibaba.search.common.arpc.transport.Transport;
import com.alibaba.search.common.arpc.transport.impl.TransportMinaImpl;
import com.alibaba.search.common.arpc.util.Constants;
import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;
import com.alibaba.search.common.arpc.util.SystemTimer;

public class ConnectionTest extends TestCase {
    private final static Logger logger = LoggerFactory
            .getLogger(ConnectionTest.class);
    
    private MinaServer server = new MinaServer();
    private Connection conn = null;

    @Override
    protected void setUp() throws Exception {
        server.start();
        Transport transport = new TransportMinaImpl(new DefaultPacketStreamer());
        conn = transport.createConnection("127.0.0.1", server.getPort());
    }

    @Override
    protected void tearDown() throws Exception {
        server.unbind();
        Thread.sleep(100);
    }
    
    public void testCreatConnection() {
        logger.debug("begin test");
        assertTrue(conn.isConnected());
    }

    public void testPostNullPacket() {
        logger.debug("begin test");
        assertFalse(conn.postPacket(null, null, null, 0, true));
    }
    
    public void testPostNormalPacket() {
        logger.debug("begin test");
        int timeout = 100;
        FakePacketHandler fakeHandler = new FakePacketHandler();
        
        DefaultPacket packet1 = new DefaultPacket();      
        assertTrue(conn.postPacket(packet1, fakeHandler, "string args", timeout, true));
        
        DefaultPacket packet2 = new DefaultPacket();
        packet2.setBody(new byte[10]);
        assertTrue(conn.postPacket(packet2, fakeHandler, null, timeout, true));
        
        assertTrue(fakeHandler.waitDone(2, timeout, true));
        assertTrue(fakeHandler.check(packet1, "string args"));
        assertTrue(fakeHandler.check(packet2, null));
    }

    public void testPostPacketAndNoResponse() {
        logger.debug("begin test");
        int timeout = 100;
        FakePacketHandler fakeHandler = new FakePacketHandler();
        
        // post a no response packet
        server.setSimulateError(ErrorCode.ARPC_ERROR_TIMEOUT);
        DefaultPacket packet1 = new DefaultPacket();
        packet1.setBody(new byte[1]);
        assertTrue(conn.postPacket(packet1, fakeHandler, "no response packet", timeout, true));
        assertFalse(fakeHandler.waitDone(1, timeout, true));
        
        // post a normal packet
        server.setSimulateError(ErrorCode.ARPC_ERROR_NONE);
        DefaultPacket packet2 = new DefaultPacket();
        packet2.setBody(new byte[10]);
        assertTrue(conn.postPacket(packet2, fakeHandler, "normal packet", timeout, true));
        assertTrue(fakeHandler.waitDone(1, timeout, true));
        assertTrue(fakeHandler.check(packet1, "normal packet"));
        fakeHandler.reset();
        
        server.unbind();     
        
        // the no response packet will received a connection closed control packet
        fakeHandler.waitDone(1, timeout, true);
        assertTrue(fakeHandler.check(new ControlPacket(null), "no response packet"));
        fakeHandler.reset();
        
        // post a normal packet after connection is closed
        DefaultPacket packet3 = new DefaultPacket();
        packet3.setBody(new byte[10]);
        assertFalse(conn.postPacket(packet3, fakeHandler, "normal packet after connection closed", 
                timeout, true));
        assertFalse(fakeHandler.waitDone(1, timeout, true));
    }
    
    public void testPostPacketAndReceiveBadPacket() {
        logger.debug("begin test");
        int timeout = 100;
        FakePacketHandler fakeHandler = new FakePacketHandler();
        
        // post a no response packet
        server.setSimulateError(ErrorCode.ARPC_ERROR_TIMEOUT);
        DefaultPacket packet1 = new DefaultPacket();
        packet1.setBody(new byte[1]);
        assertTrue(conn.postPacket(packet1, fakeHandler, "no response packet", timeout, true));
        assertFalse(fakeHandler.waitDone(1, timeout, true));
        fakeHandler.reset();
        
        // post a normal packet
        server.setSimulateError(ErrorCode.ARPC_ERROR_NONE);
        DefaultPacket packet2 = new DefaultPacket();
        packet2.setBody(new byte[10]);
        assertTrue(conn.postPacket(packet2, fakeHandler, "normal packet", timeout, true));
        assertTrue(fakeHandler.waitDone(1, timeout, true));
        assertTrue(fakeHandler.check(packet1, "normal packet"));
        fakeHandler.reset();
        
        // post a packet which will receive bad packet, and connection will be closed by client
        server.setSimulateError(ErrorCode.ARPC_ERROR_BAD_PACKET);
        DefaultPacket packet3 = new DefaultPacket();
        packet3.setBody(new byte[2]);
        assertTrue(conn.postPacket(packet3, fakeHandler, "response with bad packet", timeout, true));
        assertTrue(fakeHandler.waitDone(2, timeout, true));
        assertTrue(fakeHandler.check(new ControlPacket(null), "no response packet"));
        assertTrue(fakeHandler.check(new ControlPacket(null), "response with bad packet"));
        fakeHandler.reset();
        
        // post a normal packet after connection is closed
        DefaultPacket packet4 = new DefaultPacket();
        packet4.setBody(new byte[10]);
        assertFalse(conn.postPacket(packet4, fakeHandler, "normal packet after connection closed", 
                timeout, true));
        assertFalse(fakeHandler.waitDone(1, timeout, true));
    }
    
    public void testPostPacketNonBlocking() {
        logger.debug("begin test");
        int timeout = 100;
        int queueLimit = 20;
        FakePacketHandler fakeHandler = new FakePacketHandler();
        conn.setQueueLimit(queueLimit);
        
        // post queueLimit no response packets, output queue will be full
        server.setSimulateError(ErrorCode.ARPC_ERROR_TIMEOUT);
        for (int i = 0; i < queueLimit; ++i) {
            DefaultPacket packet = new DefaultPacket();
            assertTrue(conn.postPacket(packet, fakeHandler, null, timeout, false));
        }
        
        // non of these packet receive response
        assertFalse(fakeHandler.waitDone(1, timeout, false));
        
        DefaultPacket packet = new DefaultPacket();
        // can't post packet now if you choose to post non blocking
        assertFalse(conn.postPacket(packet, fakeHandler, null, timeout, false));
    }
    
    public void testPostPacketBlocking() {
        logger.debug("begin test");
        int timeout = 100;
        int queueLimit = 20;
        FakePacketHandler fakeHandler = new FakePacketHandler();
        conn.setQueueLimit(queueLimit);
        
        // post queueLimit+1 no response packets
        // packetId: 1~queueLimit will get into the output queue
        // packetId: queueLimit+1 will be blocked
        server.setSimulateError(ErrorCode.ARPC_ERROR_TIMEOUT);
        PostThread thread = new PostThread(fakeHandler, queueLimit + 1);
        thread.start();
        
        // non of these packet receive response
        assertFalse(fakeHandler.waitDone(1, timeout, false));
        
        // let packet queueLimit-1 get handled
        DefaultPacket packet = new DefaultPacket();
        packet.getHeader().setPacketId(queueLimit - 1);
        byte[] bytes = new DefaultPacketStreamer().encode(packet);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        conn.handleRead(buf);
        
        // check packet queueLimit-1 get handled
        assertTrue(fakeHandler.waitDone(1, timeout, true));
        ArrayList<Packet> packets = fakeHandler.getPackets();
        assertEquals(1, packets.size());
        assertEquals(queueLimit - 1, packets.get(0).getHeader().getPacketId());
        fakeHandler.reset();
        
        // packet queueLimit+1 will be put in output queue now but not handled
        assertFalse(fakeHandler.waitDone(1, timeout, true));
        
        // handle packet queueLimit+1
        packet = new DefaultPacket();
        packet.getHeader().setPacketId(queueLimit + 1);
        bytes = new DefaultPacketStreamer().encode(packet);
        buf = ByteBuffer.wrap(bytes);
        conn.handleRead(buf);
        
        assertTrue(fakeHandler.waitDone(1, timeout,true));
        packets = fakeHandler.getPackets();
        assertEquals(1, packets.size());
        assertEquals(queueLimit + 1, packets.get(0).getHeader().getPacketId());
    }
    
    public void testPostPacketBlockingAndConnectionClosed() {
        logger.debug("begin test");
        int timeout = 100;
        int queueLimit = 20;
        FakePacketHandler fakeHandler = new FakePacketHandler();
        conn.setQueueLimit(queueLimit);
        
        // post queueLimit+1 no response packets
        // packetId: 1~queueLimit will get into the output queue
        // packetId: queueLimit+1 will be blocked
        server.setSimulateError(ErrorCode.ARPC_ERROR_TIMEOUT);
        PostThread thread = new PostThread(fakeHandler, queueLimit + 1);
        thread.start();
        
        // non of these packet receive response
        assertFalse(fakeHandler.waitDone(1, timeout, false));
        
        conn.close();
        try {
            thread.join();
        } catch (InterruptedException e) {
            fail("unexpected exception");
        }
        
        // all packets in output queue will be handled with ControlPacket
        assertTrue(fakeHandler.waitDone(queueLimit, timeout, true));
        for (int i = 1; i <= queueLimit; ++i) {
            assertTrue(fakeHandler.check(new ControlPacket(null), i));
        }
        
        // packet queueLimit+1 wake up but the connection is closed
        // postpacket did not success
        ArrayList<Boolean> postedFlag = thread.getPostedFlag();
        for (int i = 0; i < queueLimit; ++i) {
            assertTrue(postedFlag.get(i));
        }
        assertFalse(postedFlag.get(queueLimit));
    }
    
    public void testPostPacketBlockingAndReceiveBadPacket() {
        logger.debug("begin test");
        int timeout = 100;
        int queueLimit = 20;
        FakePacketHandler fakeHandler = new FakePacketHandler();
        conn.setQueueLimit(queueLimit);
        
        // post queueLimit+1 no response packets
        // packetId: 1~queueLimit will get into the output queue
        // packetId: queueLimit+1 will be blocked
        server.setSimulateError(ErrorCode.ARPC_ERROR_TIMEOUT);
        PostThread thread = new PostThread(fakeHandler, queueLimit + 1);
        thread.start();
        
        // non of these packet receive response
        assertFalse(fakeHandler.waitDone(1, timeout, false));
        
        // connection receive bad response
        byte[] buf = new byte[20];
        conn.handleRead(ByteBuffer.wrap(buf));
        try {
            thread.join();
        } catch (InterruptedException e) {
            fail("unexpected exception");
        }
        
        // all packets in output queue will be handled with ControlPacket
        assertTrue(fakeHandler.waitDone(queueLimit, timeout, true));
        for (int i = 1; i <= queueLimit; ++i) {
            assertTrue(fakeHandler.check(new ControlPacket(null), i));
        }
        
        // packet queueLimit+1 wake up but the connection is closed
        // postpacket did not success
        ArrayList<Boolean> postedFlag = thread.getPostedFlag();
        for (int i = 0; i < queueLimit; ++i) {
            assertTrue(postedFlag.get(i));
        }
        assertFalse(postedFlag.get(queueLimit));
    }

    public void testCheckTimeout() {
        logger.debug("begin test");
        int timeout = 100;
        PacketManager packetManager = conn.getPacketManager();
        packetManager.setTimer(new SystemTimer() {
            public long getCurrentTime() {
                return 0;
            }
        });
        FakePacketHandler fakeHandler = new FakePacketHandler();

        DefaultPacket packet = new DefaultPacket();
        packetManager.addPacket(packet, fakeHandler, "packet1", 0);

        packet = new DefaultPacket();
        packetManager.addPacket(packet, fakeHandler, "packet2", 5);

        packet = new DefaultPacket();
        packetManager.addPacket(packet, fakeHandler, "packet3", 10);

        packet = new DefaultPacket();
        packetManager.addPacket(packet, fakeHandler, "packet4", 7);

        conn.checkTimeout(7);
        assertTrue(fakeHandler.waitDone(2, timeout, true));
        assertTrue(fakeHandler.check(new ControlPacket(null), "packet2"));
        assertTrue(fakeHandler.check(new ControlPacket(null), "packet4"));
        ArrayList<Packet> packets = fakeHandler.getPackets();
        for (Packet p : packets) {
            assertEquals(CmdType.CMD_TIMEOUT_PACKET, ((ControlPacket)p).getCmd());
        }
        assertEquals(2, packetManager.size());
        
        conn.checkTimeout(Constants.DEFAULT_PACKET_TIMEOUT);
        assertTrue(fakeHandler.waitDone(4, timeout, true));
        assertTrue(fakeHandler.check(new ControlPacket(null), "packet1"));
        assertTrue(fakeHandler.check(new ControlPacket(null), "packet3"));
        packets = fakeHandler.getPackets();
        for (Packet p : packets) {
            assertEquals(CmdType.CMD_TIMEOUT_PACKET, ((ControlPacket)p).getCmd());
        }
        assertEquals(0, packetManager.size());
    }

    public void testHandleRead() {
        logger.debug("begin test");
        int timeout = 100;
        
        PacketManager packetManager = conn.getPacketManager();
        FakePacketHandler fakeHandler = new FakePacketHandler();
        packetManager.clear();
        DefaultPacket packet = new DefaultPacket();
        packetManager.addPacket(packet, fakeHandler, "empty packet1", 0);
        
        packet = new DefaultPacket();
        packet.setBody(new byte[2]);
        packetManager.addPacket(packet, fakeHandler, "normal packet2", 0);
        
        packet = new DefaultPacket();
        packet.setBody(new byte[3]);
        packetManager.addPacket(packet, fakeHandler, "wrong packet3", 0);
        
        ByteBuffer buf = ByteBuffer.allocate(100);
        {
            // incomplete packet2 header
            buf.putInt(Constants.ANET_PACKET_FLAG);
            buf.putInt(2); // packetId
        }
        buf.flip();
        conn.handleRead(buf);

        // the databuffer is not a complete packet, so the handler was not called
        assertFalse(fakeHandler.waitDone(1, timeout, true));
        
        buf = ByteBuffer.allocate(100);
        {
            // complete packet2
            buf.putInt(0); // pcode
            buf.putInt(2); // body len
            buf.put((byte)0); // write body
            buf.put((byte)0); // write body
        }
        {
            // complete packet1
            buf.putInt(Constants.ANET_PACKET_FLAG);
            buf.putInt(1); // packetId
            buf.putInt(0); // pcode
            buf.putInt(0); // body len
        }
        {
            // incomplete packet3 header
            buf.putInt(Constants.ANET_PACKET_FLAG);
            buf.putInt(3); // packetId
            buf.putInt(0); // pcode
        }
        buf.flip();
        conn.handleRead(buf);
        
        // packet1 and packet2 are completed, and they will be handled
        assertTrue(fakeHandler.waitDone(2, timeout, true));
        assertTrue(fakeHandler.check(new DefaultPacket(), "empty packet1"));
        assertTrue(fakeHandler.check(new DefaultPacket(), "normal packet2"));
        
        buf = ByteBuffer.allocate(100);
        {
            // complete packet3 with wrong body len
            buf.putInt(-1); // wrong bodylen
            buf.put((byte)0); // write some body
            buf.put((byte)1); // write some body
            buf.put((byte)2); // write some body
        }
        buf.flip();
        conn.handleRead(buf);
        
        // all packets will be handled
        assertTrue(fakeHandler.waitDone(3, timeout, true));
        assertTrue(fakeHandler.check(new ControlPacket(null), "wrong packet3"));
        ArrayList<Packet> packets = fakeHandler.getPackets();
        assertEquals(3, packets.size());
        ControlPacket controlPacket = (ControlPacket)packets.get(2);
        assertEquals(CmdType.CMD_BAD_PACKET, controlPacket.getCmd());
    }
    
    public void testHandleClose() {
    	logger.debug("begin test");
    	
    	PacketManager packetManager = conn.getPacketManager();
        FakePacketHandler fakeHandler = new FakePacketHandler();
        
        packetManager.clear();
        packetManager.addPacket(new DefaultPacket(), fakeHandler, "packet1", 0);    	
    	packetManager.addPacket(new DefaultPacket(), fakeHandler, "packet2", 0);
    	
    	conn.handleClose();
    	
    	long timeout = 100;
    	assertTrue(fakeHandler.waitDone(2, timeout, true));
    	assertTrue(fakeHandler.check(new ControlPacket(null), "packet1"));
    	assertTrue(fakeHandler.check(new ControlPacket(null), "packet2"));
    	ArrayList<Packet> packets = fakeHandler.getPackets(); 
    	assertEquals(2, fakeHandler.getPackets().size());
    	for (Packet packet : packets) {	
    		assertEquals(CmdType.CMD_CONNECTION_CLOSED, ((ControlPacket)packet).getCmd());
    	}
    }
    
    public void testHandleException() {
    	logger.debug("begin test");
    	
    	PacketManager packetManager = conn.getPacketManager();
        FakePacketHandler fakeHandler = new FakePacketHandler();
        
        packetManager.clear();
        packetManager.addPacket(new DefaultPacket(), fakeHandler, "packet1", 0);    	
    	packetManager.addPacket(new DefaultPacket(), fakeHandler, "packet2", 0);
    	
    	conn.handleException(new Exception());
    	
    	long timeout = 100;
    	assertTrue(fakeHandler.waitDone(2, timeout, true));
    	assertTrue(fakeHandler.check(new ControlPacket(null), "packet1"));
    	assertTrue(fakeHandler.check(new ControlPacket(null), "packet2"));
    	ArrayList<Packet> packets = fakeHandler.getPackets(); 
    	assertEquals(2, fakeHandler.getPackets().size());
    	for (Packet packet : packets) {	
    		assertEquals(CmdType.CMD_CAUGHT_EXCEPTION, ((ControlPacket)packet).getCmd());
    	}
    }

    public void testHandlePacketMultitimes() {
        logger.debug("begin test");
        int timeout = 100;
        PacketManager packetManager = conn.getPacketManager();
        FakePacketHandler fakeHandler = new FakePacketHandler();
        
        DefaultPacket packet = new DefaultPacket();
        packetManager.addPacket(packet, fakeHandler, "normal packet1", 0);
        
        packet = new DefaultPacket();
        packet.setBody(new byte[2]);
        packetManager.addPacket(packet, null, "normal packet2", 0);
        
        ByteBuffer buf = ByteBuffer.allocate(100);
        {
            // packet1
            buf.putInt(Constants.ANET_PACKET_FLAG);
            buf.putInt(1); // packetid
            buf.putInt(0); // pcode
            buf.putInt(0); // body len
            
            // packet2
            buf.putInt(Constants.ANET_PACKET_FLAG);
            buf.putInt(2); // packetid
            buf.putInt(0); // pcode
            buf.putInt(2); // body len
            buf.put((byte)0); // write body
            buf.put((byte)0); // write body
            
            // packet1 again
            buf.putInt(Constants.ANET_PACKET_FLAG);
            buf.putInt(1); // packetid
            buf.putInt(0); // pcode
            buf.putInt(0); // body len
        }
        buf.flip();
        conn.handleRead(buf);
        assertTrue(fakeHandler.waitDone(1, timeout, true));
    }
    
    private class FakePacketHandler implements PacketHandler {
        private ArrayList<Packet> packets = new ArrayList<Packet>();
        private ArrayList<Object> args = new ArrayList<Object>();
        private Object waitObj = new Object();
        private volatile int handledCount = 0;
        
        public void reset() {
            packets.clear();
            args.clear();
            handledCount = 0;
        }
        
        public ArrayList<Packet> getPackets() {
            return packets;
        }

        public boolean check(Packet packet, Object arg) {           
            int size = packets.size();
            for (int i = 0; i < size; ++i) {
                if (packets.get(i).getClass() != packet.getClass()) {
                    continue;
                } 
                if (args.get(i) == null) {
                    if (arg != null) {
                        continue;
                    } else {
                        return true;
                    }
                } else {
                    if (args.get(i).equals(arg)) {
                        return true;
                    } else {
                        continue;
                    }
                }
            }
            return false;
        }
        
        public void handlePacket(Packet packet, Object args) {
            packets.add(packet);
            this.args.add(args);
            synchronized (waitObj) {
                handledCount++;
                waitObj.notify();
            }
        }
        
        public boolean waitDone(int count, long timeout, boolean exactly) {
            synchronized (waitObj) {
    			while (handledCount < count) {
    				try {
    				    long start = System.currentTimeMillis();
    					waitObj.wait(timeout);
    					long duration = System.currentTimeMillis() - start;
    					if (handledCount > count) {
    					    if (exactly) {
    					        logger.debug("waitDone fail, expect exactly:{} handledCount:{}", 
    					                count, handledCount);
    					        return false;
    					    } else {
    					        logger.debug("waitDone success, expect:{} handledCount:{}", 
    					                count, handledCount);
    					        return true;
    					    }
    					} else if (handledCount == count) {
    					    logger.debug("waitDone success, expect:{} handledCount:{}", 
    					                count, handledCount);
    					    return true;
    					} else {
    					    if (duration >= timeout) {
    					        logger.debug("waitDone fail, expect:{} handledCount:{}",
    					                count, handledCount);
    					        return false;
    					    }
    					    timeout -= duration;
    					}
    				} catch (Exception e) {
    				    e.printStackTrace();
    					return false;
    				}
    			}
            }
            return exactly ? handledCount == count : handledCount > count;
        }
    }
    
    private class PostThread extends Thread {
        private int postCount = 0;
        private PacketHandler packetHandler;
        private ArrayList<Boolean> postedFlag = new ArrayList<Boolean>();
        
        public PostThread(PacketHandler packetHandler, int postCount) {
            this.packetHandler = packetHandler;
            this.postCount = postCount;
        }
        
        public ArrayList<Boolean> getPostedFlag() {
            return postedFlag;
        }

        public void run() { 
            for (int i = 1; i <= postCount; ++i) {
                DefaultPacket packet = new DefaultPacket();
                postedFlag.add(conn.postPacket(packet, packetHandler, i, 0, true));
            }
        }
    }
}
