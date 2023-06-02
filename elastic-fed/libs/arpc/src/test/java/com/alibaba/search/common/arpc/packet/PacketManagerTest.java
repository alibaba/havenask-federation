package com.alibaba.search.common.arpc.packet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.PriorityQueue;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.packet.impl.DefaultPacket;
import com.alibaba.search.common.arpc.util.Constants;
import com.alibaba.search.common.arpc.util.SystemTimer;

public class PacketManagerTest extends TestCase {
    private final static Logger logger = LoggerFactory
            .getLogger(PacketManagerTest.class);
    
    public void testAddPacket() {
        PacketManager pm = new PacketManager();
        pm.setTimer(new SystemTimer() {
            public long getCurrentTime() {
                return 0;
            }
        });
        
        DefaultPacket packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 1);
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 0);    
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 50);    
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 100);
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 50);
        
        
        HashMap<Integer, PacketInfo> map = pm.getPostedPacketMap();
        PriorityQueue<PacketInfo> queue = pm.getWaitingQueue();
        
        assertEquals(5, map.size());
        assertEquals(5, queue.size());
        
        long expectedExpireTime[] = {1, 50, 50, 100, Constants.DEFAULT_PACKET_TIMEOUT};
        int expectedPacketId[] = {1, 3, 5, 4, 2};
        int idx = 0;
        while (!queue.isEmpty()) {
            PacketInfo packetInfo = queue.poll();
            assertEquals(expectedExpireTime[idx], packetInfo.getExpireTime());
            assertEquals(expectedPacketId[idx], packetInfo.getPacketId());
            PacketInfo packetInfo2 = map.get(expectedPacketId[idx]);
            assertSame(packetInfo, packetInfo2);
            idx++;
        }
    }

    public void testPullAllPacketInfos() {
        PacketManager pm = new PacketManager();

        DefaultPacket packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 1);
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 0);
        
        Collection<PacketInfo> packetInfos = pm.pullAllPacketInfos();
        HashMap<Integer, PacketInfo> map = pm.getPostedPacketMap();
        PriorityQueue<PacketInfo> queue = pm.getWaitingQueue();
        
        assertEquals(2, packetInfos.size());
        assertEquals(0, map.size());
        assertEquals(0, queue.size());
    }

    public void testPullPacketInfo() {
        PacketManager pm = new PacketManager();

        DefaultPacket packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 1);
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 0);
        
        
        PacketInfo packetInfo = pm.pullPacketInfo(packet);
        HashMap<Integer, PacketInfo> map = pm.getPostedPacketMap();
        PriorityQueue<PacketInfo> queue = pm.getWaitingQueue();
        
        assertEquals(2, packetInfo.getPacketId());
        assertEquals(1, map.size());
        assertEquals(1, queue.size());
    }
    
    public void testPullTimeoutPacketInfos() {
        PacketManager pm = new PacketManager();
        pm.setTimer(new SystemTimer() {
            public long getCurrentTime() {
                return 0;
            }
        });
        
        DefaultPacket packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 0);    
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 50);    
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 100);
        
        packet = new DefaultPacket();
        pm.addPacket(packet, null, null, 50);
        
        ArrayList<PacketInfo> packetInfos = pm.pullTimeoutPacketInfos(50);
        assertEquals(2, packetInfos.size());
        for (PacketInfo packetInfo : packetInfos) {
            assertEquals(50, packetInfo.getExpireTime());
        }
    }
}
