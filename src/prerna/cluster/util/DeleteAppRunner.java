package prerna.cluster.util;

import java.io.IOException;

import prerna.cluster.util.clients.AbstractCloudClient;

public class DeleteAppRunner implements Runnable {

	private final String appId;
	
	public DeleteAppRunner(String appId) {
		this.appId = appId;
	}
	
	@Override
	public void run() {
		try {
			AbstractCloudClient.getClient().deleteApp(appId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

}
