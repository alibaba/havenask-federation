.alibaba.search.common.arpc;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHeader;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket.CmdType;
import com.alibaba.search.common.arpc.packet.impl.DefaultPacket;
import com.alibaba.search.common.arpc.test.TestMessage;
import com.alibaba.search.common.arpc.util.ErrorDefine;
import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;
import com.google.protobuf.Message;

public class ANetRPCPacketHandlerTest extends TestCase {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private ANetRPCPacketHandler packetHandler; 
	
	public ANetRPCPacketHandlerTest(String name) {
		super(name);
		packetHandler = new ANetRPCPacketHandler(new DefaultMessageCodec());
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testHandleRegularPacket() {
		TestMessage.PersonInfo personInfo = TestMessage.PersonInfo.newBuilder()
				.setName("name").setAge(10).build();
		Packet packet = this.buildPacket(personInfo.toByteArray());
		
		ANetRPCController controller = new ANetRPCController();
		Message response = TestMessage.PersonInfo.getDefaultInstance();
		SyncRpcCallback<Message> done = new SyncRpcCallback<Message>();
		RpcCallbackContext context = new RpcCallbackContext(controller, null, response, done);
		
		packetHandler.handlePacket(packet, context);
		
		try {
			done.waitResponse();
		} catch (Exception e) {
			fail("wait for response fail");		
		}
		
		assertFalse(controller.failed());
		assertEquals(ErrorCode.ARPC_ERROR_NONE, controller.getErrorCode());
		TestMessage.PersonInfo info = (TestMessage.PersonInfo)done.getResponse();
		assertNotNull(info);
		assertEquals("name", info.getName());
		assertEquals(10, info.getAge());		
	}
	
	public void testHandleInvalidProtobufMessage() {
		Packet packet = buildPacket(new String("abcd").getBytes());

		ANetRPCController controller = new ANetRPCController();
		Message response = TestMessage.PersonInfo.getDefaultInstance();
		SyncRpcCallback<Message> done = new SyncRpcCallback<Message>();
		RpcCallbackContext context = new RpcCallbackContext(controller, null, response, done);
		
		packetHandler.handlePacket(packet, context);
		
		try {
			done.waitResponse();
		} catch (Exception e) {
			fail("wait for response fail");		
		}
		
		assertTrue(controller.failed());
		int ec = ErrorCode.ARPC_ERROR_DECODE_PACKET;
		assertEquals(ec, controller.getErrorCode());
		assertEquals(ErrorDefine.getErrorMsg(ec), controller.errorText());
		assertNull(done.getResponse());
	}
	
	public void testHandleUnRegularPacket() {
		Packet packet = new ControlPacket(CmdType.CMD_CAUGHT_EXCEPTION);
		ANetRPCController controller = new ANetRPCController();
		Message response = TestMessage.PersonInfo.getDefaultInstance();
		SyncRpcCallback<Message> done = new SyncRpcCallback<Message>();
		RpcCallbackContext context = new RpcCallbackContext(controller, null, response, done);
		
		packetHandler.handlePacket(packet, context);
		
		try {
			done.waitResponse();
		} catch (Exception e) {
			fail("wait for response fail");		
		}
		
		assertTrue(controller.failed());
		int ec = ErrorCode.ARPC_ERROR_CONNECTION_CLOSED;
		assertEquals(ec, controller.getErrorCode());
		assertEquals(ErrorDefine.getErrorMsg(ec), controller.errorText());
		assertNull(done.getResponse());
	}
	
	public void testHandlePacketWithNoneZeroPcode() {
		PacketHeader header = new PacketHeader();
		header.pcode = 1;
		DefaultPacket packet = new DefaultPacket();
		packet.setHeader(header);
		//String msgStr = "test error";
		//RpcExtensions.ErrorMsg errorMsg = RpcExtensions.ErrorMsg.newBuilder().setErrorMsg(msgStr).build();
		//packet.setBody(errorMsg.toByteArray());

		ANetRPCController controller = new ANetRPCController();
		Message responseType = RpcExtensions.ErrorMsg.getDefaultInstance(); 
		SyncRpcCallback<Message> done = new SyncRpcCallback<Message>();
		RpcCallbackContext context = new RpcCallbackContext(controller, null, responseType, done);
		
		packetHandler.handlePacket(packet, context);		
		try {
			done.waitResponse();
		} catch (Exception e) {
			fail("wait for response fail");		
		}
		
		assertTrue(controller.failed());
		assertEquals(header.pcode, controller.getErrorCode());
		//assertEquals(msgStr, controller.errorText());
		assertNull(done.getResponse());
	}
	
	public void testHandlePacketWithNoneZeroPcodeAndWrongMsg() {
		PacketHeader header = new PacketHeader();
		header.pcode = 1;
		DefaultPacket packet = new DefaultPacket();
		packet.setHeader(header);
		String msgStr = "test error";
		packet.setBody(msgStr.getBytes());

		ANetRPCController controller = new ANetRPCController();
		Message responseType = RpcExtensions.ErrorMsg.getDefaultInstance(); 
		SyncRpcCallback<Message> done = new SyncRpcCallback<Message>();
		RpcCallbackContext context = new RpcCallbackContext(controller, null, responseType, done);
		
		packetHandler.handlePacket(packet, context);		
		try {
			done.waitResponse();
		} catch (Exception e) {
			fail("wait for response fail");		
		}
		
		assertTrue(controller.failed());
		assertEquals(header.pcode, controller.getErrorCode());
		String expectErrorMsg = "decode ErrorMsg from packet Failed";
		assertEquals(expectErrorMsg, controller.errorText());
		assertNull(done.getResponse());
	}
	
	public void testConvertCmdType2ErrorCode() {	
		assertEquals(ErrorCode.ARPC_ERROR_BAD_PACKET, 
					ANetRPCPacketHandler.convertCmdType2ErrorCode(CmdType.CMD_BAD_PACKET));
		assertEquals(ErrorCode.ARPC_ERROR_TIMEOUT, 
					ANetRPCPacketHandler.convertCmdType2ErrorCode(CmdType.CMD_TIMEOUT_PACKET));
		assertEquals(ErrorCode.ARPC_ERROR_CONNECTION_CLOSED, 
					ANetRPCPacketHandler.convertCmdType2ErrorCode(CmdType.CMD_CONNECTION_CLOSED));
		assertEquals(ErrorCode.ARPC_ERROR_CONNECTION_CLOSED, 
					ANetRPCPacketHandler.convertCmdType2ErrorCode(CmdType.CMD_CAUGHT_EXCEPTION));
	}
	
	public Packet buildPacket(byte[] body) {
		DefaultPacket packet = new DefaultPacket();
		PacketHeader header = new PacketHeader();
		header.pcode = 0;
		header.bodyLen = body.length;
		packet.setHeader(header);
		packet.setBody(body);
		return packet;
	}
}
