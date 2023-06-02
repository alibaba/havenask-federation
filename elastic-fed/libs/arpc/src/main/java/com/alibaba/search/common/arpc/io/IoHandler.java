package com.alibaba.search.common.arpc.io;

import java.nio.ByteBuffer;

public interface IoHandler {
    public void handleRead(ByteBuffer buf);
    public void handleException(Throwable cause);
    public void handleClose();
}
