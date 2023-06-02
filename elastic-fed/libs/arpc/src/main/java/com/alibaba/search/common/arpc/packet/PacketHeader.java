package com.alibaba.search.common.arpc.packet;

public class PacketHeader {
    public int packetId;    //  packet id
    public int pcode;   // packet code, can carry some user defined information
    public int bodyLen; // length of body

    public PacketHeader() {
        packetId = -1;
        pcode = -1;
        bodyLen = 0;
    }
    
    public PacketHeader(int packetId, int pcode, int bodyLen) {
        this.packetId = packetId;
        this.pcode = pcode;
        this.bodyLen = bodyLen;
    }

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public int getPcode() {
        return pcode;
    }

    public void setPcode(int pcode) {
        this.pcode = pcode;
    }

    public int getBodyLen() {
        return bodyLen;
    }

    public void setBodyLen(int bodyLen) {
        this.bodyLen = bodyLen;
    }
}
