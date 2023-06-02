package com.alibaba.search.common.arpc;

import com.alibaba.search.common.arpc.io.Connection;
import com.alibaba.search.common.arpc.packet.PacketStreamer;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacketStreamer;
import com.alibaba.search.common.arpc.transport.Transport;
import com.alibaba.search.common.arpc.transport.TransportFactory;

public class FakeTransportFactory implements TransportFactory {
	public FakeTransport transport = new FakeTransport(new DefaultPacketStreamer());
	
	public Transport createTransport() {
		return transport;
	}
}

class FakeTransport extends Transport {
    public boolean expectNullConnection = false;
    
	public FakeTransport(PacketStreamer packetStreamer) {
        super(packetStreamer);
    }

    @Override
	protected Connection doCreateConnection(String host, int port) {
    	if (expectNullConnection) {
    		return null;
    	}
    	FakeConnection connection = new FakeConnection();
		return connection;
	}

    @Override
    public void dispose() {}
}
