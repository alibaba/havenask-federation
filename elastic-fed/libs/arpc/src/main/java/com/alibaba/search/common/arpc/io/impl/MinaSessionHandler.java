package com.alibaba.search.common.arpc.io.impl;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.io.IoComponent;
import com.alibaba.search.common.arpc.util.Constants;

public class MinaSessionHandler extends IoHandlerAdapter {
    private final static Logger logger = LoggerFactory
            .getLogger(MinaSessionHandler.class);

    @Override
    public void exceptionCaught(IoSession session, Throwable cause)
            throws Exception {
        logger.error("mina session caught exception: ", cause);
        
        IoComponent ioComponent = (IoComponent) session
                .getAttribute(Constants.IO_COMPONENT);
        ioComponent.handleException(cause);
    }

    @Override
    public void messageReceived(IoSession session, Object message)
            throws Exception {
        IoBuffer ioBuffer = (IoBuffer) message;
        logger.debug("mina session received message, size:{}",
                (ioBuffer.limit() - ioBuffer.position()));
        
        IoComponent ioComponent = (IoComponent) session
                .getAttribute(Constants.IO_COMPONENT);
        ioComponent.handleRead(ioBuffer.buf());
    }

    @Override
    public void messageSent(IoSession session, Object message) 
            throws Exception {
        IoBuffer ioBuffer = (IoBuffer) message;
        logger.debug("mina session sent message, size:{}",
                (ioBuffer.limit() - ioBuffer.position()));
    }

    @Override
    public void sessionClosed(IoSession session) throws Exception {
        logger.debug("mina session is being closed");
        
        IoComponent ioComponent = (IoComponent) session
                .getAttribute(Constants.IO_COMPONENT);
        ioComponent.handleClose();
    }

    @Override
    public void sessionCreated(IoSession session) throws Exception {
        logger.debug("mina session is created");
    }

    @Override
    public void sessionOpened(IoSession session) throws Exception {
        logger.debug("mina session is opened");
    }

}
