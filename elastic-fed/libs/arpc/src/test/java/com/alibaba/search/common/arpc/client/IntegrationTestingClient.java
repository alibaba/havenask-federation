package com.alibaba.search.common.arpc.client;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.ANetRPCChannelManager;

public class IntegrationTestingClient extends Thread{
	private final static Logger logger = LoggerFactory.getLogger(IntegrationTestingClient.class);		
	public final static String usage = "Usage: \n" +
			"server mode: -port PORT\n" +
			"Single mode: -actions ACTION[;ACTION...]\n" +
			"Support ACTION:\n" + 
			"   1. open:host=Hostname&port=Port&block=Boolean&sid=Sid\n" +
			"                         : open a stub, marked by Sid(a global int).\n" +
			"   2. close:sid=Sid      : close a stub.\n" +
			"   3. sleep:time=MS(millisecond)\n" +
	        "                         : sleep some time before executing next action\n" +
			"   4. packet:sid=Sid&type=normal&timeout=MS&content=Content&count=Count\n" +
			"   5. packet:sid=Sid&type=timeout&timeout=MS&serverblocktime=MS&count=Count\n" +
			"   6. packet:sid=Sid&type=failed&timeout=MS&content=Content&count=Count\n" +
			"   7. packet:sid=Sid&type=invalidHeader&timeout=MS&error=ErrorType=count=Count\n" +
			"   8. packet:sid=Sid&type=stop&timeout=MS\n" +
			"                      : send some packets.\n" +
			"                      : count(default:1) is the number of packets.\n" +
			"                      : sid(default:random) is one of the opened stub's id.\n" +
			"                      : error(packetFlag or bodyLen) is the type of invalidHeader.\n";
	
	private Random random = new Random();
	private ArrayList<Action> actions = new ArrayList<Action>();
	private ConcurrentHashMap<String, ArpcClient> arpcClientMap = new ConcurrentHashMap<String, ArpcClient>();
	private ConcurrentHashMap<String, ArpcClient> removedArpcClientMap = new ConcurrentHashMap<String, ArpcClient>();
	private Socket socket = null;
	private ANetRPCChannelManager channelManager = new ANetRPCChannelManager();
	private Responser responser = new Responser();
	
	public IntegrationTestingClient() {
		responser.start();
	}
	
	public void setSocket(Socket socket) {
		this.socket = socket;
	}
	
	public void run() {
		if (socket == null) {
			return;
		}
		try {
			OutputStream outputStream = socket.getOutputStream();
			DataOutputStream out = new DataOutputStream(outputStream);
			responser.setDataOutputStream(out);
			InputStream inputStream = socket.getInputStream();
			BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
			while (true) {
				String actionStr = in.readLine();
				if (actionStr == null) {
					break;
				}
				process(actionStr);
			}
		} catch (IOException e) {
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
			}
		}
		joinArpcClient(arpcClientMap);
		joinArpcClient(removedArpcClientMap);
		channelManager.dispose();
		responser.close();
	}
	
	public boolean process(String actionsStr) {
	        actions.clear();
		if(!parse(actionsStr)) {
			return false;
		}
		Iterator<Action> it = actions.iterator();
		while(it.hasNext()){
			Action action = it.next();
			if (!action.run()) {
				responser.write(String.format("execute action [%s] failed.", action.toString()));				
				return false;
			}
		}
		if (socket == null) {
			joinArpcClient(arpcClientMap);
			joinArpcClient(removedArpcClientMap);
		    channelManager.dispose();
		}
		return true;
	}
	
	private void joinArpcClient(ConcurrentHashMap<String, ArpcClient> arpcClients) {
		Iterator<Entry<String, ArpcClient>> it1 = arpcClients.entrySet().iterator();
		while (it1.hasNext()) {
			Map.Entry<String, ArpcClient> entry = it1.next();
			ArpcClient arpcClient = entry.getValue();
			arpcClient.finish();
			try {
				arpcClient.join();
			} catch (InterruptedException e) {
				logger.error("join arpcClient thread failed.\n {}", e.toString());	
			}
		}
	}
	
	private boolean parse(String actionsStr) {
		String[] actionsStrArr = actionsStr.split(";");
		for (int i = 0; i < actionsStrArr.length; i++) {
			Action action = new Action(this);
			if (!action.parse(actionsStrArr[i])) {
				return false;
			}
			actions.add(action);
		}
		return true;
	}

    public ArpcClient getRandomArpcClient() {
	   if (arpcClientMap.size() == 0) {
	       return null;
	   }
	   Set<String> keySet = arpcClientMap.keySet();
	   String[] keyArr = new String[keySet.size()];
	   keySet.toArray(keyArr);
	   String sid = keyArr[random.nextInt(keyArr.length)];
	   return arpcClientMap.get(sid); 
       }
	
	public ArpcClient getArpcClient(String sid) {
	    if (null == sid) {
		return null;
	    }
	    return arpcClientMap.get(sid); 
	}
	
	public boolean closeArpcClient(String sid) {
		if (null == sid) {
			responser.write("sid is null.");
		 }
		 ArpcClient arpcClient = arpcClientMap.get(sid);
		if (null == arpcClient) {
			responser.write(String.format("sid [%s] hasn't been opened.", sid));
			return false;
		}
		arpcClientMap.remove(sid);
		arpcClient.close();
		removedArpcClientMap.put(sid, arpcClient); 
		return true;
	}
	
	public boolean openArpcClient(String sid, String hostname, int port , boolean block) {
		if (sid == null) {
			responser.write("sid is null.");
			return false;
		}
		if (arpcClientMap.get(sid) != null) {
			responser.write(String.format("sid [%s] has been opened.", sid));
			return false;
		}
		ArpcClient arpcClient = new ArpcClient(channelManager, responser);
		if (!arpcClient.open(hostname, port, block)) {
			responser.write(String.format("open arpcClient (spec = %s:%s, block = %s) failed. ",
						      hostname, port,
						      block ? "true" : "false"));
			return false;
		}
		arpcClientMap.put(sid, arpcClient);
		arpcClient.start();
		return true;
	}
	
	public Responser getResponser() {
		return responser;
	}
}
