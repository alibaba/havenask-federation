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
import com.alibaba.search.common.arpc.packet.impl.DefaultPacket;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

public class DefaultMessageCodec extends MessageCodec {
    private final static Logger logger = LoggerFactory
            .getLogger(DefaultMessageCodec.class);

    @Override
    public Packet encode(Message message, MethodDescriptor methodDescriptor) {
        try {
            byte[] body = message.toByteArray();
            PacketHeader header = buildPacketHeader(methodDescriptor,
                    body.length);
            if (header == null) {
                return null;
            }
            DefaultPacket packet = new DefaultPacket();
            packet.setBody(body);
            packet.setHeader(header);
            return packet;
        } catch (Exception e) {
            logger.error("get byte from message failed, exception:", e);
        }
        return null;
    }

    @Override
    public Message decode(Packet packet, Message messageType) {
        if (!packet.isRegularPacket()) {
            logger.error("packet is not regular");
            return null;
        }
        try {
            DefaultPacket defaultPacket = (DefaultPacket) packet;
            Message message = messageType.newBuilderForType()
                    .mergeFrom(defaultPacket.getBody()).build();
            return message;
        } catch (InvalidProtocolBufferException e) {
            logger.error("decode meet exception:", e);
        } catch (Exception e) {
            logger.error("decode meet exception:", e);
        }

        return null;
    }
}
