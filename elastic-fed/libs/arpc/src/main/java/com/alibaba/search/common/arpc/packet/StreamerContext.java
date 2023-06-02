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

public class StreamerContext {
    private Packet packet;
    private boolean completed;
    // handler start time， will reset when next package reaches
    private static ThreadLocal<Long> packetReturnStartTime = new ThreadLocal<>();

    /**
     * get last handler start time（the time header got）
     * @return last handler start time
     */
    public static Long lastPacketReturnStartTime() {
        return packetReturnStartTime.get();
    }

    public StreamerContext() {
        reset();
    }

    public void reset() {
        packet = null;
        completed = false;
        packetReturnStartTime.remove();
    }

    public Packet getPacket() {
        return packet;
    }

    public void setPacket(Packet packet) {
        this.packet = packet;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public void setPacketReturnStartTime(long time) {
        packetReturnStartTime.set(time);
    }

}
