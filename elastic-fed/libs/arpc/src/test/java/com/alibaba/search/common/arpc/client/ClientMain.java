package com.alibaba.search.common.arpc.client;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class ClientMain {
	@Option(name = "-actions", required = false)
	private String actions = "";
	
	@Option(name = "-port", required = false)
	private int port = 0;
	
	public static void main(String[] args) {
		ClientMain clientMain = new ClientMain();
		clientMain.doMain(args);
	}

	private int doMain(String[] args) {
		CmdLineParser parser = new CmdLineParser(this); 
		try {  
			parser.parseArgument(args);  
		} catch( CmdLineException e ) {  
			System.err.println(e.getMessage());  
			System.err.println(IntegrationTestingClient.usage);
			return -1;  
		}
		if (!actions.isEmpty()) {
			IntegrationTestingClient client = new IntegrationTestingClient();
			if(!client.process(actions)) {
				return -1;
			}			
		} else if (port != 0) {
			ActionServer server = new ActionServer();
			server.start(port);
		}
		return 0;
	}
}
