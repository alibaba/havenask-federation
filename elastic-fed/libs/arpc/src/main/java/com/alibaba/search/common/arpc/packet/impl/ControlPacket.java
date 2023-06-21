/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
