package com.alibaba.search.common.arpc.client;

import com.alibaba.search.common.arpc.ANetRPCController;
import com.alibaba.search.common.arpc.test.IntegrationTesting.Response;
import com.google.protobuf.RpcCallback;

public class DefaultCallback implements RpcCallback<Response> {
	ArpcClient arpcClient;
	ANetRPCController controller;
	DefaultCallback(ArpcClient arpcClient, ANetRPCController controller) {
		this.arpcClient = arpcClient;
		this.controller = controller;
	}
	
	public void run(Response parameter) {
		arpcClient.receiveOnePacket(controller, parameter);
	}

}
