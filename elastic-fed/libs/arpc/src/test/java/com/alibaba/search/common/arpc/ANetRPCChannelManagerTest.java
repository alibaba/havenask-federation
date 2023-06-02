package com.alibaba.search.common.arpc;

import java.util.Vector;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.transport.TransportFactory;
import com.alibaba.search.common.arpc.util.Constants;

public class ANetRPCChannelManagerTest extends TestCase {
	private final Logger logger = LoggerFactory.getLogger(ANetRPCChannelManagerTest.class);
	private boolean openChannelSucFlag = true;
	
	public ANetRPCChannelManagerTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	private synchronized void setOpenChannelSucFlag(boolean flag) {
		this.openChannelSucFlag = flag;
	}

	public void testOpenChannel() {
		logger.debug("Begin test");
		
		TransportFactory transportFactory = new FakeTransportFactory();
		ANetRPCChannelManager rpcChannelMgr = new ANetRPCChannelManager(transportFactory);
		ANetRPCChannel rpcChannel = rpcChannelMgr.openChannel("127.0.0.1", 12345);
		assertNotNull(rpcChannel);
		assertEquals(1, rpcChannelMgr.getChannelCount());
		assertTrue(rpcChannel.isPostBlocking());
		assertEquals(Constants.DEFAULT_OUTPUT_QUEUE_LIMIT, rpcChannel.getPostQueueSizeLimit());
		
		assertSame(rpcChannel, rpcChannelMgr.openChannel("127.0.0.1", 12345));
		assertEquals(1, rpcChannelMgr.getChannelCount());
		
		ANetRPCChannel tmpRpcChannel = rpcChannelMgr.openChannel("127.0.0.1", 12346);
		assertNotNull(tmpRpcChannel);
		assertEquals(2, rpcChannelMgr.getChannelCount());
		assertNotSame(rpcChannel, tmpRpcChannel);
		assertEquals(tmpRpcChannel, rpcChannelMgr.openChannel("127.0.0.1", 12346));
		
		tmpRpcChannel = rpcChannelMgr.openChannel("127.0.0.2", 12345);
		assertNotNull(tmpRpcChannel);
		assertNotSame(rpcChannel, tmpRpcChannel);
		assertEquals(3, rpcChannelMgr.getChannelCount());
		
		tmpRpcChannel = rpcChannelMgr.openChannel("wrong address", 0);
		assertNull(tmpRpcChannel);
	}
	
	public void testOpenExistingNotOpenedChannel() {
		logger.debug("Begin test");
		
		FakeTransportFactory transportFactory = new FakeTransportFactory();
		ANetRPCChannelManager rpcChannelMgr = new ANetRPCChannelManager(transportFactory);
		
		ANetRPCChannel rpcChannel = rpcChannelMgr.openChannel("127.0.0.1", 12345);
		assertNotNull(rpcChannel);
		assertEquals(1, rpcChannelMgr.getChannelCount());
		
		rpcChannel.close();
		assertNotSame(rpcChannel, rpcChannelMgr.openChannel("127.0.0.1", 12345));
		assertEquals(1, rpcChannelMgr.getChannelCount());
	}
	
	public void testOpenChannelAfterDispose() {
		logger.debug("Begin test");
		
		FakeTransportFactory transportFactory = new FakeTransportFactory();
		ANetRPCChannelManager rpcChannelMgr = new ANetRPCChannelManager(transportFactory);
		
		ANetRPCChannel rpcChannel = rpcChannelMgr.openChannel("127.0.0.1", 12345);
		assertNotNull(rpcChannel);
		rpcChannelMgr.dispose();
		
		ANetRPCChannel rpcChannel2= rpcChannelMgr.openChannel("127.0.0.1", 12345);
		assertNotNull(rpcChannel2);
		
		assertNotSame(rpcChannel, rpcChannel2);
	}
	
	public void testOpenChannelWithTransportOpenNullConnection() {
		logger.debug("Begin test");
		
		FakeTransportFactory transportFactory = new FakeTransportFactory();
		ANetRPCChannelManager rpcChannelMgr = new ANetRPCChannelManager(transportFactory);
		transportFactory.transport.expectNullConnection = true;
		
		ANetRPCChannel rpcChannel = rpcChannelMgr.openChannel("127.0.0.1", 12345);
		assertNull(rpcChannel);
	}
	
	public void testOpenChannelInMultiThreads() {
		logger.debug("Begin test");
		FakeTransportFactory transportFactory = new FakeTransportFactory();
		ANetRPCChannelManager rpcChannelMgr = new ANetRPCChannelManager(transportFactory);
		
		Vector<String> hosts = new Vector<String>();
		Vector<Integer> ports = new Vector<Integer>();
		int channelNumber = 5;
		for (int i = 0; i < channelNumber; ++i) {
			hosts.add("127.0.0.1");
			ports.add(new Integer(5656 + i * 10));
		}
		
		Vector<OpenChannelThread> threads = new Vector<OpenChannelThread>();
		int threadNumber = 10;
		for (int i = 0; i < threadNumber; ++i) {
			threads.add(new OpenChannelThread(rpcChannelMgr, hosts, ports));
		}
		
		for (int i = 0; i < threadNumber; ++i) {
			threads.elementAt(i).start();
		}
		
		for (int i = 0; i < threadNumber; ++i) {
			try {
				threads.elementAt(i).join();
			} catch (Exception e) {
				fail("join thread " + i + " fail.");
			}
		}
		
		assertTrue(this.openChannelSucFlag);
		assertEquals(channelNumber, rpcChannelMgr.getChannelCount());
	}
	
	class OpenChannelThread extends Thread {
		private ANetRPCChannelManager manager;
		private Vector<String> hosts = new Vector<String>();
		private Vector<Integer> ports = new Vector<Integer>();
		
		public OpenChannelThread(ANetRPCChannelManager manager, 
				Vector<String> hosts, Vector<Integer> ports ) 
		{
			this.manager = manager;
			this.hosts = hosts;
			this.ports = ports;
		}
		
		public void run() {
			for (int i = 0; i < hosts.size(); ++i) {
				ANetRPCChannel channel = manager.openChannel(hosts.elementAt(i), ports.elementAt(i));
				if (channel == null) {
					logger.error("channel is null");
					setOpenChannelSucFlag(false);
				}
			}
		}
	}
}
