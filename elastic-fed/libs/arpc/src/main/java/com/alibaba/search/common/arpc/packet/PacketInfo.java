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
