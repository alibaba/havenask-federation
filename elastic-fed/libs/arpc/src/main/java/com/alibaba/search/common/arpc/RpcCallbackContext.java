package com.alibaba.search.common.arpc;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

public class RpcCallbackContext {
	public ANetRPCController controller;
	public Message request;
	public Message responseType;
	public RpcCallback<Message> callback;
	
	public RpcCallbackContext(ANetRPCController controller,
				Message request, Message responseType, 
				RpcCallback<Message> callback) 
	{
		this.controller = controller;
		this.request = request;
		this.responseType = responseType;
		this.callback = callback;
	}
}
