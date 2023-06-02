package com.alibaba.search.common.arpc.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.exceptions.ArpcException;
import com.alibaba.search.common.arpc.exceptions.ArpcInvalidAddressException;

public class IpAddressUtil {
	private final static Logger logger = LoggerFactory.
	        getLogger(IpAddressUtil.class);
	
	public static long encodeToLong(String hostname, int port) 
	        throws ArpcException {
		InetSocketAddress socketAddress = new InetSocketAddress(hostname, port);
		InetAddress inetAddress = socketAddress.getAddress();
		logger.debug("address: " + socketAddress.toString());
		if (null == inetAddress || socketAddress.isUnresolved()) {
			String errorMsg = String.format("encode address failed, " +
					"hostname [%s], port [%d]", hostname, port); 
			logger.error(errorMsg);
			throw new ArpcInvalidAddressException();
		}
		byte[] address = inetAddress.getAddress();
		long ret = 0;
		for (int i = 0; i < 4; i++) {
			ret <<= 8;
			ret |= address[i] & 0xFF;
		}
		ret <<= 32;
		ret |= port;
		return ret;
	}
}
