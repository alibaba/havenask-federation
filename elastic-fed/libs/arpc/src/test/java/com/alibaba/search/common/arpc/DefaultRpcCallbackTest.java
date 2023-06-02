package com.alibaba.search.common.arpc;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRpcCallbackTest extends TestCase{
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private InnerDefaultRpcCallback callback = new InnerDefaultRpcCallback();
			
	public DefaultRpcCallbackTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testWithWaitInfinite() {
		RunThread runThread = new RunThread(10);
		runThread.start();
		
		try {
			callback.waitResponse();
		} catch (Exception e) {
			fail("meet exception " + e.toString());
		} finally {
			runThread.isStop = true;
			try {
				runThread.join();
			} catch (Exception e) {				
			}
		}
		
		assertEquals(new Integer(10), callback.value);				
	}
	
	public void testWithWaitNotInfinite() {
		RunThread runThread = new RunThread(20);
		runThread.start();
		
		try {
			callback.waitResponse();
		} catch (Exception e) {
			fail("meet exception " + e.toString());
		} finally {
			runThread.isStop = true;
			try {
				runThread.join();
			} catch (Exception e) {				
			}
		}
		
		assertEquals(new Integer(20), callback.value);				
	}
	
	class InnerDefaultRpcCallback extends DefaultRpcCallback<Integer> {
		public Integer value;
		protected void doRun(Integer parameter) {
			value = parameter;
		}
	}
	
	class RunThread extends Thread {
		public int value = 0;
		public boolean isStop = false;
		
		public RunThread(int value) {
			this.value = value;
		}
		
		public void run() {
			while(!isStop) {
				try {
					Thread.sleep(100);
					callback.run(value);
				} catch (Exception e) {
					logger.error("sleep meet exception {}.", e.toString());
				}
			}
		}
	}
}

