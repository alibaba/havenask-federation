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
