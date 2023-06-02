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
