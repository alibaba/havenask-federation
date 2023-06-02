package com.alibaba.search.common.arpc.packet;

public interface PacketHandler {
	void handlePacket(Packet packet, Object args);
}
