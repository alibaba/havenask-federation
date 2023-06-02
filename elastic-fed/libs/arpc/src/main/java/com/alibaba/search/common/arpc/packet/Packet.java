package com.alibaba.search.common.arpc.packet;

import com.alibaba.search.common.arpc.util.DataBuffer;

public abstract class Packet {

    protected PacketHeader header;

    public Packet() {
        header = new PacketHeader();
    }
    
    public PacketHeader getHeader() {
        return header;
    }

    public void setHeader(PacketHeader header) {
        this.header = header;
    }
    
    public abstract boolean isRegularPacket();
    public abstract void decode(DataBuffer input);
    public abstract void encode(DataBuffer output);
}
