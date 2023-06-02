package com.alibaba.search.common.arpc.packet.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.util.DataBuffer;

public class ControlPacket extends Packet {
    private final static Logger logger = LoggerFactory
            .getLogger(ControlPacket.class);
    
    public enum CmdType {
        CMD_BAD_PACKET,
        CMD_TIMEOUT_PACKET,
        CMD_CONNECTION_CLOSED,
        CMD_CAUGHT_EXCEPTION
    };

    private CmdType cmd;
    
    public ControlPacket(CmdType cmdType) {
        this.cmd = cmdType;
    }
    
    @Override
    public void decode(DataBuffer input) {
    	logger.warn("ControlPacket cant not be decoded");
    }

    @Override
    public void encode(DataBuffer output) {
    	logger.warn("ControlPacket cant not be encoded");
    }

    @Override
    public boolean isRegularPacket() {
        return false;
    }
    
    public CmdType getCmd() {
        return cmd;
    }
    
    public void setCmd(CmdType cmd) {
        this.cmd = cmd;
    }
}
