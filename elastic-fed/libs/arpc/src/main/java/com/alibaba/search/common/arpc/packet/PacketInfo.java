package com.alibaba.search.common.arpc.packet;

public class PacketInfo implements Comparable<PacketInfo> {
    private int packetId = -1;
    private PacketHandler packetHandler = null;
    private Object args = null;
    private long expireTime = 0;

    public PacketInfo(int packetId, PacketHandler packetHandler, Object args, 
            long expireTime) {
        this.packetId = packetId;
        this.packetHandler = packetHandler;
        this.args = args;
        this.expireTime = expireTime;
    }

    public int getPacketId() {
        return packetId;
    }
    
    public PacketHandler getPacketHandler() {
        return packetHandler;
    }
    
    public Object getArgs() {
        return args;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public int compareTo(PacketInfo o) {
        if (expireTime - o.expireTime > 0) {
            return 1;
        } else if (expireTime - o.expireTime < 0) {
            return -1;
        } else {
            return packetId - o.packetId;
        }
    }

}
