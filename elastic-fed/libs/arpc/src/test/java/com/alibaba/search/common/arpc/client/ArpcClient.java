package com.alibaba.search.common.arpc.client;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.search.common.arpc.ANetRPCChannel;
import com.alibaba.search.common.arpc.ANetRPCChannelManager;
import com.alibaba.search.common.arpc.ANetRPCController;
import com.alibaba.search.common.arpc.test.IntegrationTesting;
import com.alibaba.search.common.arpc.test.IntegrationTesting.HeaderError;
import com.alibaba.search.common.arpc.test.IntegrationTesting.QueryType;
import com.alibaba.search.common.arpc.test.IntegrationTesting.Request;
import com.alibaba.search.common.arpc.test.IntegrationTesting.Response;
import com.google.protobuf.ServiceException;

public class ArpcClient extends Thread{
	private ConcurrentLinkedQueue<Action> actions = new ConcurrentLinkedQueue<Action>();
	private IntegrationTesting.IntegrationTestingService.BlockingInterface blockingService;
	private IntegrationTesting.IntegrationTestingService.Stub service;
	private boolean block = false;
	private boolean finish = false;
	private boolean close = false;
	private Lock lock = new ReentrantLock();
	private int waitForBackCount = 0;
	private ANetRPCChannelManager channelManager;
	private Responser responser = null;
				
	public ArpcClient(ANetRPCChannelManager channelManager, Responser responser) {
		super();
		blockingService = null;
		service = null;
		this.channelManager = channelManager;
		this.responser = responser;
	}
	
	public void addAction(Action action) {
		actions.add(action);
	}
	
	public void finish() {
		finish = true;
	}
	
	public void close() {
		close = true;
	}

	public void receiveOnePacket(ANetRPCController controller,	Response parameter) {
		if (controller.failed()) {
			responser.write(String.format("rpcCall failed: code [%s], msg [%s]",
						controller.getErrorCode(), controller.errorText()));
		} else {
			if (parameter == null) {
				responser.write("received one packet, content: null."); 
			} else {
				responser.write(String.format("received one packet, content: %s", 
						parameter.getResult()));		
			}
		}
		lock.lock();
		waitForBackCount--;
		lock.unlock();
		
	}
	public boolean open(String hostname, int port, boolean block) {
		this.block = block;
		ANetRPCChannel channel = channelManager.openChannel(hostname, port);
		if (null == channel) {
			return false;
		}
		if (block) {
			blockingService = IntegrationTesting.IntegrationTestingService.newBlockingStub(channel);
		} else {
			service = IntegrationTesting.IntegrationTestingService.newStub(channel);
		}
		return true;
	}
	
	private Request buildRequest(HashMap<String, String> packet) {
		Request.Builder builder = Request.newBuilder(); 
		String packetType = packet.get("type");
		if (packetType.equals("normal")) {
		    builder.setQueryType(QueryType.QT_NORMAL);
		    builder.setStrValue(packet.get("content"));
		} else if (packetType.equals("timeout")) {
		    builder.setQueryType(QueryType.QT_TIMEOUT);
		    builder.setIntValue(Integer.parseInt(packet.get("serverblocktime")));
		} else if (packetType.equals("failed")) {
		    builder.setQueryType(QueryType.QT_FAILED);
		    builder.setStrValue(packet.get("content"));
		} else if (packetType.equals("invalidHeader")) {
		    builder.setQueryType(QueryType.QT_HEADER_ERROR);
		    String errorType = packet.get("error");
		    if (errorType.equals("packetFlag")) {
		    	builder.setError(HeaderError.HE_PACKET_FLAG);
		    } else if(errorType.equals("bodyLen")) {
		    	builder.setError(HeaderError.HE_BODY_LEN);
		    } else {
				responser.write(String.format("unknown invalidHeader type: %s.", errorType));
				return null;
		    }
		} else if (packetType.equals("stop")) {
		    builder.setQueryType(QueryType.QT_STOP);
		} else {
			responser.write(String.format("unknown packet type: %d.", packetType));
			return null;
		}
		return builder.build();
	}
	
	private void sendPacket(Action action) {
		HashMap<String, String> packet = action.getPacketParameters();
		Request request = buildRequest(packet);
		if (null == request) {
			return;
		}
		String countStr = packet.get("count");
		int count = 1;
		if (null != countStr) {
			count = Integer.parseInt(countStr);
		}
		while (count-- > 0) {
			ANetRPCController controller = new ANetRPCController();
			String packetTimeoutStr = packet.get("timeout");
			if (null != packetTimeoutStr) {
				controller.setRequestTimeout(Integer.parseInt(packetTimeoutStr));			
			}
			lock.lock();
			waitForBackCount++;
			lock.unlock();
			try {
				Response response = null;
				if (block) {
					response = blockingService.query(controller, request);
					receiveOnePacket(controller, response);
				} else {
					DefaultCallback done = new DefaultCallback(this, controller);
					service.query(controller, request, done);	
				}
			} catch (ServiceException e) {
				e.printStackTrace();
			}
		}
	}

    public void run() {
    	while ((!close) && (!finish || actions.size() > 0 || waitForBackCount > 0)) {
    		if (actions.size() > 0) {
    			Action action = actions.poll();
    			if (null != action) {
    				sendPacket(action);
    			}
    		}
    		try {
    			Object obj = new Object();
    			synchronized(obj) {
    				obj.wait(100, 0);
    			}
    		} catch (InterruptedException e) {
    		}
    	}	
    }
}