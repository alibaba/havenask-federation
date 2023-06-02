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
