package com.alibaba.search.common.arpc.io;

import java.net.InetSocketAddress;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.util.ErrorDefine.ErrorCode;

public class MinaServer {
    private final static Logger logger = LoggerFactory
            .getLogger(MinaServer.class);
    
    private int port = 59527;
    private boolean running = false;
    private IoAcceptor acceptor = new NioSocketAcceptor();
    private IoHandlerAdapter serverHandler = new MinaServerHandler();
    private int simulateError = ErrorCode.ARPC_ERROR_NONE;
    
    public void start() {
        if (running) {
            return;
        }
        acceptor.setHandler(serverHandler);
        while (true) {
            try {
                acceptor.bind(new InetSocketAddress(port));
                break;
            } catch (Exception e) {
                port++;
            }
        }
        logger.debug("bind to localhost:{}", port);
        running = true;
    }

    public void unbind() {
    	logger.debug("unbind the MinaServer");
        acceptor.unbind();
        running = false;
    }

    public int getPort() {
        return port;
    }

    public void setSimulateError(int errorCode) {
    	simulateError = errorCode;
    }
  
    class MinaServerHandler extends IoHandlerAdapter {
        @Override
        public void messageReceived(IoSession session, Object message)
                throws Exception 
        {
        	if (simulateError == ErrorCode.ARPC_ERROR_TIMEOUT) {
        		return;
        	} else if (simulateError == ErrorCode.ARPC_ERROR_BAD_PACKET) {
        		IoBuffer msg = IoBuffer.wrap("0000000000000000000".getBytes());
        		session.write(msg);
        		return;
        	} else if (simulateError == ErrorCode.ARPC_ERROR_CONNECTION_CLOSED) {
        		session.close(true);
        		return;
        	}
        	
            IoBuffer msg = (IoBuffer)message;
            int packetLength = msg.limit() - msg.position();
            logger.debug("server: messageReceived, size: " + packetLength);
            session.write(message);
        }
    }
}
