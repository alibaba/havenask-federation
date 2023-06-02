package com.alibaba.search.common.arpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.io.Connection;
import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHandler;
import com.alibaba.search.common.arpc.util.ErrorDefine;
import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;

public class ANetRPCChannel implements RpcChannel, BlockingRpcChannel {
	private final static Logger logger = LoggerFactory.
	        getLogger(ANetRPCChannel.class);

	private final Connection connection;
	private final MessageCodec messageCodec;
	private PacketHandler packetHandler;
	private boolean postBlocking;
	
	public ANetRPCChannel(Connection connection) {
		this.connection = connection;
		this.messageCodec = new DefaultMessageCodec();
		this.packetHandler = new ANetRPCPacketHandler(messageCodec);
		this.postBlocking = true;
	}

	public void callMethod(Descriptors.MethodDescriptor method,
			RpcController controller, Message request,
			Message responsePrototype,
			com.google.protobuf.RpcCallback<Message> done) 
	{
		ANetRPCController anetController = (ANetRPCController)controller;  
		Packet packet = messageCodec.encode(request, method);
		if (null == packet) {
			setFailed(anetController, ErrorCode.ARPC_ERROR_ENCODE_PACKET, done);
			return;
		}
		
		RpcCallbackContext context = new RpcCallbackContext(anetController, 
					request, responsePrototype, done); 
		
		boolean succ = connection.postPacket(packet, packetHandler, 
				context, anetController.getRequestTimeout(), postBlocking);
		if (!succ) {
			setFailed(anetController, ErrorCode.ARPC_ERROR_POST_PACKET, done);	
		}
	}

	public Message callBlockingMethod(Descriptors.MethodDescriptor method,
			RpcController controller, Message request, 
			Message responsePrototype) 
	{
		SyncRpcCallback<Message> stub = new SyncRpcCallback<Message>();
		callMethod(method, controller, request, responsePrototype, stub);
		ANetRPCController anetController = (ANetRPCController)controller;

		if (!stub.waitResponse()) {
			int ec = ErrorCode.ARPC_ERROR_TIMEOUT;
			String errorMsg = ErrorDefine.getErrorMsg(ec);
			controller.setFailed(errorMsg);
			anetController.setErrorCode(ec);
			logger.error("call blocking method failed, {}", errorMsg);	
			return null;
		}
		
		if (controller.failed()) {
			return null;
		}
		return stub.getResponse();
	}
	
	public void setPostBlocking(boolean blocking) {
		this.postBlocking = blocking;
	}
	
	public boolean isPostBlocking() {
		return postBlocking;
	}
	
	public void setPostQueueSizeLimit(int size) {
		if (connection != null && size >= 1) {
			connection.setQueueLimit(size);
		}
	}
	
	public int getPostQueueSizeLimit() {
		if (connection != null) {
			return connection.getQueueLimit();
		}
		return 0;
	}
	
	public boolean isOpened() {
		if (connection == null) {
			return false;
		}
		return connection.isConnected();
	}
	
	public void setPacketHandler(PacketHandler packetHandler) {
		this.packetHandler = packetHandler;
	}
	
	public void setConnectionQueueLimit(int limit) {
	    if (limit <= 0) {
	        return;
	    }
	    connection.setQueueLimit(limit);
	}
	
	private void setFailed(ANetRPCController controller, int ec, 
			RpcCallback<Message> done) 
	{
		String errorMsg = ErrorDefine.getErrorMsg(ec);
		controller.setFailed(errorMsg);
		controller.setErrorCode(ec);	
		logger.error(errorMsg);
		done.run(null);
	}
	
	void close() {
	    connection.close();
	}

}
