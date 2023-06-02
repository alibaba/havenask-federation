package com.alibaba.search.common.arpc;

import com.google.protobuf.RpcCallback;

public abstract class DefaultRpcCallback<MessageType> implements
        RpcCallback<MessageType> {
	private volatile boolean callbackDone = false;
	private Object waitObj = new Object();

	protected abstract void doRun(MessageType response);
	
	public final void run(MessageType response) {
		doRun(response);
		synchronized (waitObj) {
			callbackDone = true;
			waitObj.notify();
		}
	}

	public final boolean waitResponse() {
		synchronized (waitObj) {
			while (!callbackDone) {
				try {
					waitObj.wait();
					return callbackDone;
				} catch (InterruptedException e) {
					continue;
				}
			}
		}
        return callbackDone;
	}

}
