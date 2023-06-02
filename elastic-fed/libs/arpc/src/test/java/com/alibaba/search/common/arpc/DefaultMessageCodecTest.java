/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */
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
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;

public class DefaultMessageCodecTest extends TestCase {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private MessageCodec messageCodec = new DefaultMessageCodec();
	
	public DefaultMessageCodecTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testEncode() {
		MethodDescriptor method = TestMessage.Test.getDescriptor().getMethods().get(0);
		TestMessage.Query message = TestMessage.Query.newBuilder().setName("test").build();
		
		Packet packet = messageCodec.encode(message, method);
		assertNotNull(packet);
		DefaultPacket defaultPacket = (DefaultPacket)packet;
		assertNotNull(defaultPacket);
		PacketHeader header = defaultPacket.getHeader();
		assertEquals(65535, (header.pcode >> 16) & 0xffff);
		assertEquals(32767, header.pcode & 0xffff);
		assertEquals(message.toByteArray().length, header.bodyLen);
		for (int i = 0; i < header.bodyLen; i++) {
			assertEquals(message.toByteArray()[i], defaultPacket.getBody()[i]);
		}
	}
	
	public void testEncodeNullMethod() {
		TestMessage.Query message = TestMessage.Query.newBuilder().setName("test").build();		
		Packet packet = messageCodec.encode(message, null);
		assertNull(packet);
	}
	
	public void testEncodeNullMessage() {
		MethodDescriptor method = TestMessage.Test.getDescriptor().getMethods().get(0);
		Packet packet = messageCodec.encode(null, method);
		assertNull(packet);
	}

	public void testDecode() {
		MethodDescriptor method = TestMessage.Test.getDescriptor().getMethods().get(0);
		TestMessage.Query message = TestMessage.Query.newBuilder().setName("test").build();
		
		Packet packet = messageCodec.encode(message, method);
		
		TestMessage.Query actualMessage = (TestMessage.Query)messageCodec.decode(packet, message);
		assertNotNull(actualMessage);
		assertEquals(message.getSerializedSize(), actualMessage.getSerializedSize());
		assertEquals(message.getName(), actualMessage.getName());
	}
	
	public void testDecodeWithWrongPacketType() {
		TestMessage.Query message = TestMessage.Query.newBuilder().setName("test").build();
		Packet packet = new ControlPacket(CmdType.CMD_BAD_PACKET);
		
		TestMessage.Query actualMessage = (TestMessage.Query)messageCodec.decode(packet, message);
		assertNull(actualMessage);
	}
	
	public void testDecodeWithWrongMessage() {
		DefaultPacket packet = new DefaultPacket();
		packet.setBody(new String("abcd").getBytes());
		Message actualMessage = messageCodec.decode(
			packet, TestMessage.Query.getDefaultInstance());
		assertNull(actualMessage);
	}

	public void testBuildPacketHeader() {
		MethodDescriptor method = TestMessage.Test.getDescriptor().getMethods().get(0);
		PacketHeader header = MessageCodec.buildPacketHeader(method, 10);
		assertNotNull(header);
		assertEquals(65535, (header.pcode >> 16) & 0xffff);
		assertEquals(32767, header.pcode & 0xffff);
		assertEquals(10, header.bodyLen);
	}
}
