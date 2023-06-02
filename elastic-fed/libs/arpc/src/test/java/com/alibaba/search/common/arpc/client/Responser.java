package com.alibaba.search.common.arpc.client;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Responser extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(Responser.class);
	private ConcurrentLinkedQueue<String> responses = new ConcurrentLinkedQueue<String>();
	private DataOutputStream out = null;
	private boolean isClose = false;
	private Object obj = new Object();
	
	public void setDataOutputStream(DataOutputStream out) {
		this.out = out;
	}
	public void write(String response) {
		responses.add(response);
	}

	public void close() {
		isClose = true;
	}
	
	public void run() {
		while (!isClose) {
			if (responses.size() > 0) {
				String response = responses.poll();
				if (out != null) {
					try {
						out.write((response + '\n').getBytes());
						out.flush();
					} catch (IOException e) {
						logger.info("write Response failed: ", e.toString());
					}
				} else {
					logger.info(response);
				}
			}
			try {
				synchronized(obj) {
					obj.wait(100, 0);
				}
			} catch (InterruptedException e) {
			}		
	   }
	}
}
