package com.alibaba.search.common.arpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHandler;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket.CmdType;
import com.alibaba.search.common.arpc.util.ErrorDefine;
import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;
import com.google.protobuf.Message;

public class ANetRPCPacketHandler implements PacketHandler {
    private final static Logger logger = LoggerFactory
            .getLogger(ANetRPCPacketHandler.class);
    
	private MessageCodec messageCodec;
			
	public static int convertCmdType2ErrorCode(CmdType cmd) {
		switch (cmd) {
			case CMD_BAD_PACKET:
				return ErrorCode.ARPC_ERROR_BAD_PACKET;
			case CMD_TIMEOUT_PACKET:
				return ErrorCode.ARPC_ERROR_TIMEOUT;
			case CMD_CONNECTION_CLOSED:
			case CMD_CAUGHT_EXCEPTION:
				return ErrorCode.ARPC_ERROR_CONNECTION_CLOSED;	
			default:
			    return ErrorCode.ARPC_ERROR_UNKNOWN;
		}
	}
	
	public ANetRPCPacketHandler(MessageCodec messageCodec) {
		this.messageCodec = messageCodec;
	}
	
	public void handlePacket(Packet packet, Object arg) {
		logger.debug("handle packet");
		RpcCallbackContext context = (RpcCallbackContext)arg;

		if (packet.isRegularPacket()) {
			handleRegularPacket(packet, context);
		} else {
			handleControlPacket(packet, context);	
		}
	}
	
	private void handleRegularPacket(Packet packet, 
	        RpcCallbackContext context) 
	{
		ANetRPCController controller = context.controller;
		
		int pcode = packet.getHeader().pcode;
		if (pcode != ErrorCode.ARPC_ERROR_NONE) {
			controller.setErrorCode(pcode);
			Message errorMsg = messageCodec.decode(packet, 
			        RpcExtensions.ErrorMsg.getDefaultInstance());
			String errorStr; 
			if (errorMsg == null) {
				errorStr = "decode ErrorMsg from packet Failed";				
			} else {
                                errorStr = "decode ErrorMsg from packet Failed";
				//errorStr = (String)(((RpcExtensions.ErrorMsg)errorMsg).getErrorMsg());
			}
			controller.setFailed(errorStr);
			context.callback.run(null);
			return;
		}
		
		Message response = messageCodec.decode(packet, context.responseType);
		if (response == null) {
			logger.error("decode message from Packet failed, {}");
			int ec = ErrorCode.ARPC_ERROR_DECODE_PACKET;
			this.setFailed(controller, ec);
			context.callback.run(null);
			return;
		}
		context.callback.run(response);
	}
	
	private void handleControlPacket(Packet packet, 
	        RpcCallbackContext context) 
	{
		ControlPacket controlPacket = (ControlPacket)packet;
		ControlPacket.CmdType cmd = controlPacket.getCmd();
		int ec = convertCmdType2ErrorCode(cmd);
		this.setFailed(context.controller, ec);
		context.callback.run(null);
	}

	private void setFailed(ANetRPCController controller, int ec) {
		controller.setErrorCode(ec);
		String errorMsg = ErrorDefine.getErrorMsg(ec);
		controller.setFailed(errorMsg);
	}
}
