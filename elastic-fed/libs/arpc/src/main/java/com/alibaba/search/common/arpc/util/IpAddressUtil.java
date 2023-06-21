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
