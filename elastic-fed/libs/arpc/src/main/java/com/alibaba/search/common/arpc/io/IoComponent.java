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

package com.alibaba.search.common.arpc.io;

import java.nio.ByteBuffer;

public abstract class IoComponent {
    protected IoHandler ioHandler;
    
    public void setIoHandler(IoHandler handler) {
        this.ioHandler = handler;
    }
    
    public abstract void write(ByteBuffer buf);
    public abstract void handleRead(ByteBuffer buf);
    public abstract void handleException(Throwable cause);
    public abstract void handleClose();
    
    public abstract boolean isConnected();
    public abstract void close();
}
