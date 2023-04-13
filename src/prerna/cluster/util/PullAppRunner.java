package prerna.cluster.util;

import java.io.IOException;

import prerna.cluster.util.clients.AbstractCloudClient;

public class PullAppRunner implements Runnable {

	private final String appId;
	
	public PullAppRunner(String appId) {
		this.appId = appId;
	}
	
	@Override
	public void run() {
		try {
			AbstractCloudClient.getClient().pullApp(appId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

}
