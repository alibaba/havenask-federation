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
