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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHeader;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;

public abstract class MessageCodec {
    private final static Logger logger = LoggerFactory
            .getLogger(MessageCodec.class);

    public abstract Packet encode(Message message,
            MethodDescriptor methodDescriptor);

    public abstract Message decode(Packet packet, Message messageType);

    public static PacketHeader buildPacketHeader(
            MethodDescriptor methodDescriptor, int bodyLen) {
        try {
            int serviceId = methodDescriptor.getService().getOptions()
                    .getExtension(RpcExtensions.globalServiceId);
            int methodId = methodDescriptor.getOptions().getExtension(
                    RpcExtensions.localMethodId);
            logger.debug("serviceid:{}, methodid:{}", serviceId, methodId);
            int pcode = (serviceId << 16) | (methodId & 0xffff);
            return new PacketHeader(0, pcode, bodyLen);
        } catch (Exception e) {
            logger.error("failed to build packet header from message. " +
            		"exception:", e);
        }
        return null;
    }
}
