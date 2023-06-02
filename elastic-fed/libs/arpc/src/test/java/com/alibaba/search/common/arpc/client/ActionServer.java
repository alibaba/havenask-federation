package com.alibaba.search.common.arpc.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActionServer {
	private final static Logger logger = LoggerFactory.getLogger(ActionServer.class);
	private ServerSocket server = null;
	
	public void start(int port) {
		try {
			server = new ServerSocket(port);
			logger.debug("listening on port: {}.", port);
			while (true) {
				Socket socket = server.accept();
				IntegrationTestingClient client = new IntegrationTestingClient();
				client.setSocket(socket);
				client.start();
			}
		} catch(Exception e) {
		        logger.error("start Action Server (port:{})failed. exception: {}", 
				     port, e.getMessage());
		} finally {
			logger.debug("end listening on port: {}.", port);
			if (server != null) {
				try {
					server.close();
				} catch (IOException e) {
				}
			}
		}
	}	
}
