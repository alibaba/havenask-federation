package com.alibaba.search.common.arpc.packet.impl;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.util.DataBuffer;

public class DefaultPacket extends Packet {
    private byte[] body = null;

    @Override
    public void decode(DataBuffer input) {
        body = input.read(header.getBodyLen());
    }

    @Override
    public void encode(DataBuffer output) {
        if(body == null) {
            return;
        }
        output.write(body);
    }

    @Override
    public boolean isRegularPacket() {
        return true;
    }

    public byte[] getBody() {
        return body;
    }
    
    public void setBody(byte[] body) {
        this.body = body;
        this.header.setBodyLen(body.length);
    }
}
