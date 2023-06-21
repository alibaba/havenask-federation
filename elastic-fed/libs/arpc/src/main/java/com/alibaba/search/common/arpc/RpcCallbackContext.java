/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
