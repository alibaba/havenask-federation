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

package com.alibaba.search.common.arpc.packet.impl;

import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.util.DataBuffer;

public class DefaultPacket extends Packet {
    private byte[] body = null;

    @Override
    public void decode(DataBuffer input) {
        body = input.read(header.getBodyLen());
    }

    @Override
    public void encode(DataBuffer output) {
        if(body == null) {
            return;
        }
        output.write(body);
    }

    @Override
    public boolean isRegularPacket() {
        return true;
    }

    public byte[] getBody() {
        return body;
    }
    
    public void setBody(byte[] body) {
        this.body = body;
        this.header.setBodyLen(body.length);
    }
}
