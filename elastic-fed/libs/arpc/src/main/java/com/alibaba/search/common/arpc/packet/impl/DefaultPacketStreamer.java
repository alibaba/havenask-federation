package com.alibaba.search.common.arpc.packet.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.exceptions.ArpcInvalidPacketHeaderException;
import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHeader;
import com.alibaba.search.common.arpc.packet.PacketStreamer;
import com.alibaba.search.common.arpc.packet.StreamerContext;
import com.alibaba.search.common.arpc.util.Constants;
import com.alibaba.search.common.arpc.util.DataBuffer;

public class DefaultPacketStreamer extends PacketStreamer {
    private final static Logger logger = LoggerFactory
            .getLogger(DefaultPacketStreamer.class);

    @Override
    public StreamerContext createStreamerContext() {
        return new StreamerContext();
    }

    @Override
    public PacketHeader getPacketHeader(DataBuffer input)
            throws ArpcInvalidPacketHeaderException {
        if (input.getDataLen() < 16) { // packet header needs four integer
            return null;
        }
        int flag = input.readInt();
        int pid = input.readInt();
        int pcode = input.readInt();
        int bodyLen = input.readInt();
        if (flag != Constants.ANET_PACKET_FLAG || bodyLen < 0
                || bodyLen > Constants.MAX_PACKET_SIZE) {
            throw new ArpcInvalidPacketHeaderException();
        }
        PacketHeader header = new PacketHeader(pid, pcode, bodyLen);
        return header;
    }

    @Override
    public void decode(DataBuffer dataBuffer, StreamerContext streamerContext)
            throws ArpcInvalidPacketHeaderException {
        Packet packet = streamerContext.getPacket();
        if (packet == null) {
            long returnStartTime = System.currentTimeMillis();
            PacketHeader packetHeader = getPacketHeader(dataBuffer);
            if (packetHeader == null) {
                // not enough data to construct the packet's header
                return;
            }
            packet = new DefaultPacket();
            packet.setHeader(packetHeader);
            streamerContext.setPacket(packet);
            streamerContext.setPacketReturnStartTime(returnStartTime);
        }
        PacketHeader packetHeader = packet.getHeader();
        if (dataBuffer.getDataLen() < packetHeader.getBodyLen()) {
            // not enough data to construct the packet's body
            return;
        }
        packet.decode(dataBuffer);
        streamerContext.setCompleted(true);
    }

    @Override
    public byte[] encode(Packet packet) {
        PacketHeader packetHeader = packet.getHeader();
        DataBuffer output = new DataBuffer(16 + packetHeader.getBodyLen());
        output.writeInt(Constants.ANET_PACKET_FLAG);
        output.writeInt(packetHeader.getPacketId());
        output.writeInt(packetHeader.getPcode());
        output.writeInt(packetHeader.getBodyLen());
        packet.encode(output);
        return output.getBuffer();
    }

}
