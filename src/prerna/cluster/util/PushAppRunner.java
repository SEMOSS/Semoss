package prerna.cluster.util;

import java.io.IOException;

import prerna.cluster.util.clients.AbstractCloudClient;

public class PushAppRunner implements Runnable {

	private final String appId;
	
	public PushAppRunner(String appId) {
		this.appId = appId;
	}
	
	@Override
	public void run() {
		try {
			AbstractCloudClient.getClient().pushApp(appId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}

}
