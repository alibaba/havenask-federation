package com.alibaba.search.common.arpc.packet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.util.Constants;
import com.alibaba.search.common.arpc.util.SystemTimer;

public class PacketManager {
    private final static Logger logger = LoggerFactory
            .getLogger(PacketManager.class);

    private AtomicInteger atomicPacketId = new AtomicInteger(1);
    private HashMap<Integer, PacketInfo> postedPacketMap = 
            new HashMap<Integer, PacketInfo>();
    private PriorityQueue<PacketInfo> waitingQueue = 
            new PriorityQueue<PacketInfo>();
    private SystemTimer timer = new SystemTimer();

    public void addPacket(Packet packet, PacketHandler packetHandler, 
            Object args, long timeout) 
    {
        if (timeout <= 0) {
            timeout = Constants.DEFAULT_PACKET_TIMEOUT;
        }
        long packetExpireTime = timer.getExpireTime(timeout);
        int packetId = atomicPacketId.getAndIncrement();
        if (packetId == Integer.MAX_VALUE) {
            atomicPacketId.set(1);
        }
        packet.getHeader().setPacketId(packetId);
        PacketInfo packetInfo = new PacketInfo(packetId, packetHandler, args, 
                packetExpireTime);
        postedPacketMap.put(packetId, packetInfo);
        waitingQueue.add(packetInfo);
    }
    
    public int size() {
        return postedPacketMap.size();
    }
    
    public ArrayList<PacketInfo> pullTimeoutPacketInfos(long now) {
        ArrayList<PacketInfo> packetInfos = new ArrayList<PacketInfo>();
        while (!waitingQueue.isEmpty()) {
            PacketInfo packetInfo = waitingQueue.peek();
            if (packetInfo.getExpireTime() > now) {
                break;
            }
            packetInfo = waitingQueue.poll();
            postedPacketMap.remove(packetInfo.getPacketId());
            packetInfos.add(packetInfo);
        }
        return packetInfos;
    }
    
    public ArrayList<PacketInfo> pullAllPacketInfos() {
        ArrayList<PacketInfo> packetInfos = new ArrayList<PacketInfo>();
        for (PacketInfo info : postedPacketMap.values()) {
            packetInfos.add(info);
        }
        postedPacketMap.clear();
        waitingQueue.clear();
        return packetInfos;
    }
    
    public PacketInfo pullPacketInfo(Packet packet) {
        int packetId = packet.getHeader().getPacketId();
        PacketInfo packetInfo = postedPacketMap.get(packetId);
        if (packetInfo == null) {
            return packetInfo;
        }
        postedPacketMap.remove(packetId);
        waitingQueue.remove(packetInfo);
        return packetInfo;
    }
    
    public void setTimer(SystemTimer timer) {
        this.timer = timer;
    }
    
    HashMap<Integer, PacketInfo> getPostedPacketMap() {
        return postedPacketMap;
    }
    
    PriorityQueue<PacketInfo> getWaitingQueue() {
        return waitingQueue;
    }
    
    public void clear() {
    	postedPacketMap.clear();
    	waitingQueue.clear();
    }
}
