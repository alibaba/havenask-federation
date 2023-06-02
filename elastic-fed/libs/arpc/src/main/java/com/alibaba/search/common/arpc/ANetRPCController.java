package com.alibaba.search.common.arpc;

import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class ANetRPCController implements RpcController {
	private long requestTimeout;
	private int errorCode;
	private String errorText;
	private boolean isFailed;

	public ANetRPCController() {
		reset();
	}
	
	public String errorText() {
		return errorText;
	}

	public boolean failed() {
		return isFailed;
	}

	public boolean isCanceled() {
		return false;
	}
	
	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int ec) {
		this.errorCode = ec;
	}
	
	public void notifyOnCancel(RpcCallback<Object> callback) {
		
	}

	public void reset() {
		requestTimeout = 0;
		errorCode = ErrorCode.ARPC_ERROR_NONE;
		errorText = "";
		isFailed = false;
	}

	public void setFailed(String reason) {
		isFailed = true;
		errorText = reason;
	}

	public void startCancel() {

	}
	
	/**
	 * @return timeout of the request in milliseconds
	 */
	public long getRequestTimeout() {
		return requestTimeout;
	}
	
	/**
	 * @param timeout timeout of the request in milliseconds
	 */
	public void setRequestTimeout(long timeout) {
		requestTimeout = timeout;
	}

}
