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

package com.alibaba.search.common.arpc.io.impl;

import java.nio.ByteBuffer;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.alibaba.search.common.arpc.io.IoComponent;

public class IoComponentMinaImpl extends IoComponent {
    private IoSession ioSession = null;
    
    public IoComponentMinaImpl(IoSession ioSession) {
        this.ioSession = ioSession;
    }
    
    @Override
    public void write(ByteBuffer buf) {
        ioSession.write(IoBuffer.wrap(buf));
    }

    @Override
    public void handleRead(ByteBuffer buf) {
        ioHandler.handleRead(buf);
    }

    @Override
    public void handleException(Throwable cause) {
        ioHandler.handleException(cause);
    }
    
    @Override
    public void handleClose() {
        ioHandler.handleClose();
    }
    
    @Override
    public boolean isConnected() {
        return !ioSession.isClosing();
    }

    @Override
    public void close() {   
        ioSession.close(true);
    }
    
}
