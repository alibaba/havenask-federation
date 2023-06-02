package com.alibaba.search.common.arpc;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.io.Connection;
import com.alibaba.search.common.arpc.io.IoComponent;
import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketInfo;
import com.alibaba.search.common.arpc.packet.PacketManager;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacket;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacketStreamer;

public class FakeConnection extends Connection{
	private final static Logger logger = LoggerFactory.getLogger(FakeConnection.class);
	private HandlePacketThread thread = new HandlePacketThread();
	private boolean doHandler = true;
	private Packet expectPacket = new DefaultPacket();
	
	public FakeConnection() {
		super(new FakeIoComponent(), new DefaultPacketStreamer());
	}
	
	public void setExpecetPacket(Packet packet) {
	    expectPacket = packet;
	}
	
	public void setDoHandler(boolean flag) {
	    this.doHandler = flag;
	}
	
	public void start() {
		thread.start();
	}
	
	public void stop() {
		try {
			thread.setStop();
			thread.join();			
		} catch (Exception e){
			
		}
	}
	
	class HandlePacketThread extends Thread{
		private boolean isStop = false;
		
		public void setStop() {
			this.isStop = true;
		}
		
		public void run() {
			try {
				while(!isStop) {
				    long now = System.currentTimeMillis();
				    checkTimeout(now);
				    sleep(100);
				    if (!doHandler) {
				        continue;
				    }
				    PacketManager pm = getPacketManager();
				    ArrayList<PacketInfo> packetInfos = pm.pullAllPacketInfos();
				    for (PacketInfo info : packetInfos) {
				        info.getPacketHandler().handlePacket(expectPacket, info.getArgs());
				    }
				}
			} catch (Exception e) {
			    logger.error("", e);
			}
		}    
	}
}	

class FakeIoComponent extends IoComponent {
    private boolean isConnected = true;

    @Override
    public void write(ByteBuffer buf) {

    }

    @Override
    public void handleRead(ByteBuffer buf) {

    }

    @Override
    public void handleException(Throwable cause) {

    }

    @Override
    public void handleClose() {

    }

    @Override
    public boolean isConnected() {
        return isConnected;
    }

    @Override
    public void close() {
        isConnected = false;
    }
}





	
