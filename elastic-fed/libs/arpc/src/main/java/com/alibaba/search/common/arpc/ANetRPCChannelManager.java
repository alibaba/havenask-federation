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

import com.alibaba.search.common.arpc.util.MutexPool;
import java.util.HashMap;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.exceptions.ArpcException;
import com.alibaba.search.common.arpc.io.Connection;
import com.alibaba.search.common.arpc.transport.Transport;
import com.alibaba.search.common.arpc.transport.TransportFactory;
import com.alibaba.search.common.arpc.transport.impl.DefaultTransportFactory;
import com.alibaba.search.common.arpc.util.Constants;
import com.alibaba.search.common.arpc.util.IpAddressUtil;

public class ANetRPCChannelManager {
    private final static Logger logger = LoggerFactory
            .getLogger(ANetRPCChannelManager.class);

	private MutexPool<Long> mutexPool = new MutexPool<>();
	private final ReadWriteLock disposeLock = new ReentrantReadWriteLock();
	private HashMap<Long, ANetRPCChannel> channelMap =
            new HashMap<Long, ANetRPCChannel>();
    private Transport transport;

	public ANetRPCChannelManager() {
		this(new DefaultTransportFactory());
	}

	public ANetRPCChannelManager(TransportFactory transportFactory) {
		transport = transportFactory.createTransport();
		transport.start();
	}

	/**
	 * @param host either hostname or ip
	 * @param port port
	 * @return rpc channel which you can invoke rpc calles or null if faild.
	 *
	 * This method is thread-safe.
	 * You will get the same ANetRPCChannel object if you open the same
	 * host:port multi-times.
	 */
	public ANetRPCChannel openChannel(String host, int port) {
		return openChannel(host, port, true, Constants.DEFAULT_OUTPUT_QUEUE_LIMIT);
	}

	/**
	 * @param host either hostname or ip
	 * @param port port
	 * @param blocking whether block if the post queue is full
	 * @param postQueueLimit limit of the post queue
	 * @return rpc channel which you can invoke rpc calles or null if failed.
	 *
	 * This method is thread-safe.
	 * You will get the same ANetRPCChannel object if you open the same
	 * host:port multi-times.
	 */
	public ANetRPCChannel openChannel(String host, int port, boolean blocking,
	        int postQueueLimit)
	{
		logger.debug("Open channel " + host + ":" + port);
		ANetRPCChannel rpcChannel = null;
		// can not create channel when disposing，but can create channel concurrently when not disposing
		disposeLock.readLock().lock();
		try {
			Long key = IpAddressUtil.encodeToLong(host, port);
			rpcChannel = channelMap.get(key);
			if (null == rpcChannel || !rpcChannel.isOpened()) {
				synchronized (mutexPool.getMutex(key)) {
					rpcChannel = channelMap.get(key);
					if (null == rpcChannel || !rpcChannel.isOpened()) {
						Connection connection = transport.createConnection(host, port);
						if (null != connection) {
							rpcChannel = new ANetRPCChannel(connection);
							channelMap.put(key, rpcChannel);
						}
					}
				}
			}
		} catch (ArpcException e) {
			logger.error("failed to open channel. " + e.getMessage());
		} finally {
			disposeLock.readLock().unlock();
		}
		return rpcChannel;
	}

	/**
	 *
	 * @param host either hostname or ip
	 * @param port port
	 * @throws ArpcException if failed to close channel
	 */
	public void closeChannel(String host, int port) throws ArpcException {
		Long key = IpAddressUtil.encodeToLong(host, port);
		ANetRPCChannel channel = channelMap.remove(key);
		if (channel != null) {
			channel.close();
		}
	}

	/**
	 * destroy the AnetRPCChannelManager, you can not open channel with this
	 * object any more.
	 */
	public void dispose() {
		disposeLock.writeLock().lock();
	    transport.dispose();
	    channelMap.clear();
		disposeLock.writeLock().unlock();
	}

	public int getChannelCount() {
		return channelMap.size();
	}
}
