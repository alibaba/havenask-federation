package com.alibaba.search.common.arpc.client;

import java.util.Date;
import java.util.HashMap;

public class Action {
	private String actionStr;
	private String actionType;
	private HashMap<String, String> parameters = new HashMap<String, String>();
	private IntegrationTestingClient client;
	private Responser responser = null;
	public Action(IntegrationTestingClient client) {
		this.client = client; 
		responser = client.getResponser();
	}
	
	public boolean parse(String actionStr) {
		this.actionStr = actionStr;
		String[] actionArr = actionStr.split(":");
		if (actionArr.length != 2) {
			responser.write(String.format("parse action [%s] error.", actionStr));
			return false;
		}
		actionType = actionArr[0];
		String[] parametersArr = actionArr[1].split("&");
		for (int i = 0; i < parametersArr.length; i++) {
			String[] keyValue = parametersArr[i].split("=");
			if (keyValue.length != 2) {
				responser.write(String.format("parse action [%s] error.", actionStr));
				return false;
			}
			parameters.put(keyValue[0], keyValue[1]);
		}
		return check();
	}

	private boolean check() {
		// TODO Auto-generated method stub
		return true;
	}
	
	public boolean run() {
		if (actionType.equals("sleep")) {
			long sleepTime = Long.parseLong(parameters.get("time"));
			long endTime = (new Date()).getTime() + sleepTime;
			try {
				while(sleepTime > 0) {
					Object obj = new Object();
					synchronized(obj) {
						obj.wait(sleepTime, 0);
						sleepTime = endTime - (new Date()).getTime();
					}
				}
			} catch (InterruptedException e) {
				return false;
			}
		} else if (actionType.equals("open")) {
			String sid = parameters.get("sid");
			String hostname = parameters.get("host");
			int port = Integer.parseInt(parameters.get("port"));
			boolean block = Boolean.parseBoolean(parameters.get("block"));
			if(!client.openArpcClient(sid, hostname, port, block)) {
				return false;
			}
		} else if (actionType.equals("close")) {
			String sid = parameters.get("sid");
			if (!client.closeArpcClient(sid)) {
				return false;
			}
		} else if (actionType.equals("packet")) {
			String sid = parameters.get("sid");
			ArpcClient arpcClient;
			if (null == sid) {
			    arpcClient = client.getRandomArpcClient();
			    if (null == arpcClient) {
			    	responser.write(String.format("no opened stub available.", sid));
			    	return false;
			    }
			} else {
			    arpcClient = client.getArpcClient(sid);
			    if (null == arpcClient) {
			    	responser.write(String.format("sid [%s] hasn't been opened.", sid));
				return false;
			    }
			}
			arpcClient.addAction(this);
		} else {
	    	responser.write(String.format("unknown action, action type: %d.", actionType));
			return false;
		}
		return true;
	}

	public HashMap<String, String> getPacketParameters() {
		return parameters;
	}
	
	public String toString() {
		return actionStr;
	}
}
