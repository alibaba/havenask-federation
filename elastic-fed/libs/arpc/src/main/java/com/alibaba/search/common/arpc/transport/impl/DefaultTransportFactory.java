package com.alibaba.search.common.arpc.transport.impl;

import com.alibaba.search.common.arpc.packet.PacketStreamer;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacketStreamer;
import com.alibaba.search.common.arpc.transport.Transport;
import com.alibaba.search.common.arpc.transport.TransportFactory;


public class DefaultTransportFactory implements TransportFactory {
    
    public Transport createTransport() {
        PacketStreamer packetStreamer = new DefaultPacketStreamer();
        return new TransportMinaImpl(packetStreamer);
    }

}
