package com.alibaba.search.common.arpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SyncRpcCallback<MessageType> extends DefaultRpcCallback<MessageType> {
    private final static Logger logger = LoggerFactory
            .getLogger(SyncRpcCallback.class);
    
    private MessageType response;
    	
	protected void doRun(MessageType response) {
		this.response = response;
	}
	
	public MessageType getResponse() {
		return response;
	}

}

