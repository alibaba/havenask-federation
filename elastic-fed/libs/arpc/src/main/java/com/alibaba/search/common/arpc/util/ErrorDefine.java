package com.alibaba.search.common.arpc.util;
import java.util.HashMap;
import java.util.Map;
public final class ErrorDefine {
	public class ErrorCode {
		public static final int ARPC_ERROR_NONE = 0;
		public static final int ARPC_ERROR_UNKNOWN = 1;
		public static final int ARPC_ERROR_RPCCALL_MISMATCH = 2;
		public static final int ARPC_ERROR_TIMEOUT = 3;
		public static final int ARPC_ERROR_BAD_PACKET = 4;
		public static final int ARPC_ERROR_CONNECTION_CLOSED = 5;
		public static final int ARPC_ERROR_DECODE_PACKET = 6;
		public static final int ARPC_ERROR_ENCODE_PACKET = 7;
		public static final int ARPC_ERROR_UNKNOWN_PACKET = 8;
		public static final int ARPC_ERROR_POST_PACKET = 9;
		public static final int ARPC_ERROR_SYN_CALL = 10;
		public static final int ARPC_ERROR_NEW_NOTHROW = 11;
		public static final int ARPC_ERROR_PUSH_WORKITEM = 12;
		public static final int ARPC_ERROR_INVALID_VERSION = 13;
		public static final int ARPC_ERROR_NO_RESPONSE_SENT = 14;
		public static final int ARPC_ERROR_RESPONSE_TOO_LARGE = 15;
		public static final int ARPC_ERROR_APPLICATION = 101;
	}
	private static Map<Integer, String> errorMsgMap = new HashMap<Integer, String>();
	static {
		errorMsgMap.put(ErrorCode.ARPC_ERROR_NONE, "");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_UNKNOWN, "unknow error. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_RPCCALL_MISMATCH, "rpc call mismatch. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_TIMEOUT, "rpc call timeout. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_BAD_PACKET, "packet is bad. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_CONNECTION_CLOSED, "connection closed. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_DECODE_PACKET, "packet decode error. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_ENCODE_PACKET, "packet encode error. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_UNKNOWN_PACKET, "unknow packet. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_POST_PACKET, "post packet error. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_SYN_CALL, "sync call error. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_NEW_NOTHROW, "new (nothrow) return null. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_PUSH_WORKITEM, "push workitem to queue failed. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_INVALID_VERSION, "invalid packet version. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_NO_RESPONSE_SENT, "server does not send any response. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_RESPONSE_TOO_LARGE, "server's response message too large. ");
		errorMsgMap.put(ErrorCode.ARPC_ERROR_APPLICATION, "rpc application error");
	}
	public static String getErrorMsg(int ec) {
		return errorMsgMap.get(ec);
	}
}
