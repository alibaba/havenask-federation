package com.alibaba.search.common.arpc;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.test.TestMessage;
import com.alibaba.search.common.arpc.util.ErrorDefine;
import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;

public class ANetRPCChannelTest extends TestCase {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	public static Message retResponse;
	private ANetRPCChannel rpcChannel;
	private FakeConnection fakeConnection;
	private FakeRpcPacketHandler fakeRpcPacketHandler;

	protected void setUp() throws Exception {
		super.setUp();
		fakeConnection = new FakeConnection();
		rpcChannel = new ANetRPCChannel(fakeConnection);
		fakeRpcPacketHandler = new FakeRpcPacketHandler();
		rpcChannel.setPacketHandler(fakeRpcPacketHandler);		
		
		fakeConnection.start();
	}

	protected void tearDown() throws Exception {
		fakeConnection.stop();
		super.tearDown();		
	}

	public void testCallMethod() {
		ANetRPCController controller = new ANetRPCController();

		TestMessage.Query request = TestMessage.Query.newBuilder()
				.setName("test_name").build();

		TestMessage.PersonInfo expectResponse = TestMessage.PersonInfo
				.newBuilder().setName("name").setAge(20).build();

		fakeRpcPacketHandler.setResponse(expectResponse);

		Descriptors.MethodDescriptor method = TestMessage.Test.getDescriptor()
				.getMethods().get(0);

		SyncRpcCallback<Message> done = new SyncRpcCallback<Message>();
		rpcChannel.callMethod(method, controller, request,
				TestMessage.PersonInfo.getDefaultInstance(), done);
		
		try {
			done.waitResponse();
		} catch (Exception e) {
			fail("wait for response fail");
		}
		
		assertFalse(controller.failed());
		assertEquals(ErrorCode.ARPC_ERROR_NONE, controller.getErrorCode());
		TestMessage.PersonInfo personInfo = (TestMessage.PersonInfo)(done.getResponse());
		assertEquals(expectResponse.getAge(), personInfo.getAge());
		assertEquals(expectResponse.getName(), personInfo.getName());
		
		// test post packet failed
		controller.reset();
		fakeConnection.close();
		rpcChannel.callMethod(method, controller, request,
				TestMessage.PersonInfo.getDefaultInstance(), done);
		assertTrue(controller.failed());
		int expectErrorCode = ErrorCode.ARPC_ERROR_POST_PACKET; 
		assertEquals(expectErrorCode, controller.getErrorCode());
		assertEquals(ErrorDefine.getErrorMsg(expectErrorCode), controller.errorText());
		
		// test build packet failed
		controller.reset();
		rpcChannel.callMethod(method, controller, null,
				TestMessage.PersonInfo.getDefaultInstance(), done);
		assertTrue(controller.failed());
		expectErrorCode = ErrorCode.ARPC_ERROR_ENCODE_PACKET;
		assertEquals(expectErrorCode, controller.getErrorCode());
		assertEquals(ErrorDefine.getErrorMsg(expectErrorCode), controller.errorText());
	}

	public void testCallBlockingMethod() {
		ANetRPCController controller = new ANetRPCController();

		TestMessage.Query request = TestMessage.Query.newBuilder()
				.setName("test_name").build();

		TestMessage.PersonInfo expectResponse = TestMessage.PersonInfo
				.newBuilder().setName("name").setAge(20).build();

		fakeRpcPacketHandler.setResponse(expectResponse);

		Descriptors.MethodDescriptor method = TestMessage.Test.getDescriptor()
				.getMethods().get(0);

		Message retResponse = rpcChannel.callBlockingMethod(method, controller, request,
				TestMessage.PersonInfo.getDefaultInstance());

		assertFalse(controller.failed());
		assertEquals(ErrorCode.ARPC_ERROR_NONE, controller.getErrorCode());
		assertEquals(expectResponse.getAge(),
				((TestMessage.PersonInfo) retResponse).getAge());
		assertEquals(expectResponse.getName(),
				((TestMessage.PersonInfo) retResponse).getName());
	}
	
	public void testCallBlockingMethodWithTimeout() {
		ANetRPCController controller = new ANetRPCController();
		controller.setRequestTimeout(100);

		TestMessage.Query request = TestMessage.Query.newBuilder()
				.setName("test_name").build();

		TestMessage.PersonInfo expectResponse = TestMessage.PersonInfo
				.newBuilder().setName("name").setAge(20).build();

		fakeRpcPacketHandler.setResponse(expectResponse);
		fakeConnection.setDoHandler(false);
		Descriptors.MethodDescriptor method = TestMessage.Test.getDescriptor()
				.getMethods().get(0);

		Message retResponse = rpcChannel.callBlockingMethod(method, controller, request,
				TestMessage.PersonInfo.getDefaultInstance());

		assertTrue(controller.failed());
		assertNull(retResponse);
		assertEquals(ErrorCode.ARPC_ERROR_TIMEOUT, controller.getErrorCode());
		assertEquals(ErrorDefine.getErrorMsg(ErrorCode.ARPC_ERROR_TIMEOUT), controller.errorText());
	}
	
	public void testCallBlockingMethodWithUnknownError() {
		ANetRPCController controller = new ANetRPCController();
		
		TestMessage.Query request = TestMessage.Query.newBuilder()
				.setName("test_name").build();

		TestMessage.PersonInfo expectResponse = TestMessage.PersonInfo
				.newBuilder().setName("name").setAge(20).build();

		fakeRpcPacketHandler.setResponse(expectResponse);
		fakeRpcPacketHandler.setErrorCode(ErrorCode.ARPC_ERROR_UNKNOWN);

		Descriptors.MethodDescriptor method = TestMessage.Test.getDescriptor()
				.getMethods().get(0);

		Message retResponse = rpcChannel.callBlockingMethod(method, controller, request,
				TestMessage.PersonInfo.getDefaultInstance());

		assertTrue(controller.failed());
		assertNull(retResponse);
		assertEquals(ErrorCode.ARPC_ERROR_UNKNOWN, controller.getErrorCode());
	}
	
	
	public void testSyncCall() {
		DefaultMessageCodec codec = new DefaultMessageCodec();
		TestMessage.PersonInfo expectPersonInfo= TestMessage.PersonInfo.newBuilder()
				.setName("name").setAge(10).build();
		MethodDescriptor method = TestMessage.Test.getDescriptor().getMethods().get(0);
		Packet packet = codec.encode(expectPersonInfo, method);
		packet.getHeader().pcode = ErrorCode.ARPC_ERROR_NONE;
		fakeConnection.setExpecetPacket(packet);		
		rpcChannel.setPacketHandler(new ANetRPCPacketHandler(codec));
		
		ANetRPCController controller = new ANetRPCController();
		TestMessage.Query query = TestMessage.Query.newBuilder().setName("name").build();
		TestMessage.Test.BlockingInterface stub = TestMessage.Test.newBlockingStub(rpcChannel);
		
		TestMessage.PersonInfo response = null;
		try {
			response = stub.queryPerson(controller, query);
		} catch (Exception e) {
			fail("sync query person meet exception. ");
		}
		
		assertFalse(controller.failed());
		assertEquals(ErrorCode.ARPC_ERROR_NONE, controller.getErrorCode());
		assertNotNull(response);
		assertTrue(response.equals(expectPersonInfo));
	}
	
	public void testAsyncCall() {
		DefaultMessageCodec codec = new DefaultMessageCodec();
		TestMessage.PersonInfo expectPersonInfo= TestMessage.PersonInfo.newBuilder()
				.setName("name").setAge(10).build();
		MethodDescriptor method = TestMessage.Test.getDescriptor().getMethods().get(0);
		Packet packet = codec.encode(expectPersonInfo, method);
		packet.getHeader().pcode = ErrorCode.ARPC_ERROR_NONE;
		
		fakeConnection.setExpecetPacket(packet);		
		rpcChannel.setPacketHandler(new ANetRPCPacketHandler(codec));
		
		ANetRPCController controller = new ANetRPCController();
		TestMessage.Query query = TestMessage.Query.newBuilder().setName("name").build();
		TestMessage.Test.Interface stub = TestMessage.Test.newStub(rpcChannel);
		
		TestMessage.PersonInfo response = null;
		SyncRpcCallback<TestMessage.PersonInfo> done = new SyncRpcCallback<TestMessage.PersonInfo>();
				
		try {
			stub.queryPerson((RpcController)controller, query, done);
		} catch (Exception e) {
			fail("async query person meet exception " + e.toString());
		}
		
		try {
			done.waitResponse();
		} catch(Exception e) {
			fail("async query person meet exception." + e.toString());
		}
		
		assertFalse(controller.failed());
		assertEquals(ErrorCode.ARPC_ERROR_NONE, controller.getErrorCode());
		response = done.getResponse();
		assertNotNull(response);
		assertTrue(response.equals(expectPersonInfo));
	}
}
