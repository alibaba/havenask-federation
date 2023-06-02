package com.alibaba.search.common.arpc.packet;

import com.alibaba.search.common.arpc.exceptions.ArpcInvalidPacketHeaderException;
import com.alibaba.search.common.arpc.util.DataBuffer;

public abstract class PacketStreamer {
    public abstract StreamerContext createStreamerContext();

    public abstract PacketHeader getPacketHeader(DataBuffer input)
            throws ArpcInvalidPacketHeaderException;

    public abstract void decode(DataBuffer dataBuffer,
            StreamerContext streamerContext)
            throws ArpcInvalidPacketHeaderException;
    
    public abstract byte[] encode(Packet packet);

}
