package com.alibaba.search.common.arpc;

import java.util.Vector;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.io.MinaServer;
import com.alibaba.search.common.arpc.test.TestMessage;
import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;

public class UseSceneTest extends TestCase {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private MinaServer minaServer = new MinaServer();
	private ANetRPCChannelManager channelManager;
	private String host = "localhost";	
	private int port = 8989;
	private boolean runSuccess = false; 
	
	public UseSceneTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		super.setUp();
		channelManager = new ANetRPCChannelManager();
		minaServer.start();		
		this.port = minaServer.getPort();
	}

	protected void tearDown() throws Exception {	
		channelManager.dispose();
		minaServer.unbind();
		super.tearDown();
	}
	
	public void testUseSceneSyncCall() {		
		ANetRPCChannel rpcChannel = channelManager.openChannel(host, port);
		assertNotNull(rpcChannel);
		
		ANetRPCController controller = new ANetRPCController();
		TestMessage.Query request = TestMessage.Query.newBuilder().setName("name").build();
		TestMessage.EchoTest.BlockingInterface stub = TestMessage.EchoTest.newBlockingStub(rpcChannel);
		
		TestMessage.Query response = null;
		try {
			response = stub.echo(controller, request);
		} catch (Exception e) {
			fail("sync query person meet exception. ");
		}
		
		assertFalse(controller.failed());
		assertEquals(ErrorCode.ARPC_ERROR_NONE, controller.getErrorCode());
		assertTrue(request.equals(response));
	}
	
	public void testUseSceneAsyncCall() {		
		ANetRPCChannel rpcChannel = channelManager.openChannel(host, port);
		assertNotNull(rpcChannel);
		
		ANetRPCController controller = new ANetRPCController();
		TestMessage.Query request = TestMessage.Query.newBuilder().setName("name").build();
		TestMessage.EchoTest.Interface stub = TestMessage.EchoTest.newStub(rpcChannel);
		
		SyncRpcCallback<TestMessage.Query> callback = new SyncRpcCallback<TestMessage.Query>();
		try {
			stub.echo(controller, request, callback);
			callback.waitResponse();
		} catch (Exception e) {
			fail("sync query person meet exception. ");
		}
		
		assertFalse(controller.failed());
		assertEquals(ErrorCode.ARPC_ERROR_NONE, controller.getErrorCode());
		assertNotNull(callback.getResponse());
		TestMessage.Query response = (TestMessage.Query)callback.getResponse(); 
		assertTrue(request.equals(response));
	}
	
	public void testUseSceneSyncCallWithTimeout() {
		syncCallWithError(ErrorCode.ARPC_ERROR_TIMEOUT);
	}
	
	public void testUseSceneSyncCallBadPacket() {
		syncCallWithError(ErrorCode.ARPC_ERROR_BAD_PACKET);
	}
	
	public void testUseSceneSyncCallConnectionClosed() {
		syncCallWithError(ErrorCode.ARPC_ERROR_CONNECTION_CLOSED);
	}
	
	public void syncCallWithError(int expectErrorCode) {
		minaServer.setSimulateError(expectErrorCode);
		ANetRPCChannel rpcChannel = channelManager.openChannel(host, port);
		assertNotNull(rpcChannel);
		
		ANetRPCController controller = new ANetRPCController();
		controller.setRequestTimeout(1000);
		TestMessage.Query request = TestMessage.Query.newBuilder().setName("name").build();
		TestMessage.EchoTest.BlockingInterface stub = TestMessage.EchoTest.newBlockingStub(rpcChannel);
		
		TestMessage.Query response = null;
		try {
			response = stub.echo(controller, request);
		} catch (Exception e) {
			fail("sync query person meet exception. ");
		}
		
		assertTrue(controller.failed());
		assertEquals(expectErrorCode, controller.getErrorCode());
		assertNull(response);
	}
	
	public void testUseSceneSyncCallInMultiThread() {
		ANetRPCChannel rpcChannel = channelManager.openChannel(host, port);
		assertNotNull(rpcChannel);
		
		TestMessage.EchoTest.BlockingInterface stub = TestMessage.EchoTest.newBlockingStub(rpcChannel);
		
		int threadNum = 20;
		Vector<Thread> threads = new Vector<Thread>();
		for (int i = 0; i < threadNum; i++) {
			threads.add(new SyncCallThread(stub));
		}
		
		runSuccess = true;
		
		for (Thread thread : threads) {
			thread.start();
		}
		
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (Exception e) {
				fail("join thread failed, " + e.toString());
			}
		}
		
		assertTrue(runSuccess);
	}
	
	public synchronized void setRunSuccess(boolean flag) {
		runSuccess = runSuccess && flag;
	}
	
	class SyncCallThread extends Thread {
		private TestMessage.EchoTest.BlockingInterface blockingStub;
		public SyncCallThread(TestMessage.EchoTest.BlockingInterface blockingStub) {
			this.blockingStub = blockingStub;
		}
		
		public void run() {
			logger.debug("run blocking call thread");
			for (int i = 0; i < 50; i++) {
				ANetRPCController controller = new ANetRPCController();
				TestMessage.Query request = TestMessage.Query.newBuilder().setName("name" + i).build();
				try {
					TestMessage.Query response = blockingStub.echo(controller, request);
					if ((!controller.failed()) && (ErrorCode.ARPC_ERROR_NONE == controller.getErrorCode()) 
							&& (response != null) && (request.equals(response)))
					{
						setRunSuccess(true);
					} else {
						setRunSuccess(false);
					}
				} catch (Exception e) {
					logger.error("there is Exception in multi-thread sync call test");
					setRunSuccess(false);
					return;
				}
			}
		}
	}
}
