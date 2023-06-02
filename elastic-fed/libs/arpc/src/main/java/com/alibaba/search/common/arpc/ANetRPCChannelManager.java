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
	 * @param port
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
	 * @param port
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
