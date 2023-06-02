package com.alibaba.search.common.arpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket;
import com.alibaba.search.common.arpc.util.ErrorDefine;
import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;
import com.google.protobuf.Message;

public class FakeRpcPacketHandler extends ANetRPCPacketHandler {
	private final static Logger logger = LoggerFactory.getLogger(FakeRpcPacketHandler.class);
	private Message response;
	private int errorCode = ErrorCode.ARPC_ERROR_NONE;
		
	public FakeRpcPacketHandler() {
		super(null);
	}
	
	public void setResponse(Message message) {
		this.response = message;
	}
	
	public void setErrorCode(int ec) {
		this.errorCode = ec;
	}
	
	public void handlePacket(Packet packet, Object arg) {
		logger.debug("handle packet");
		
		RpcCallbackContext rpcCallbackContext = (RpcCallbackContext)arg;  
		logger.debug("response " + response.toString());
		
		rpcCallbackContext.controller.setErrorCode(this.errorCode);
		if (errorCode !=  ErrorCode.ARPC_ERROR_NONE) {
			rpcCallbackContext.controller.setFailed(ErrorDefine.getErrorMsg(errorCode));
			this.response = null;
		} else {
		    if (!packet.isRegularPacket()) {
		        int ec = convertCmdType2ErrorCode(((ControlPacket)packet).getCmd()); 
		        rpcCallbackContext.controller.setErrorCode(ec);
		        rpcCallbackContext.controller.setFailed(ErrorDefine.getErrorMsg(ec));
		    }
		}
		
		rpcCallbackContext.responseType = this.response;
		rpcCallbackContext.callback.run(this.response);
	}
}
