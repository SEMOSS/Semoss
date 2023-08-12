package prerna.cluster.util;

import prerna.cluster.util.clients.AbstractCloudClient;

public class DeleteAppRunner implements Runnable {

	private final String appId;
	
	public DeleteAppRunner(String appId) {
		this.appId = appId;
	}
	
	@Override
	public void run() {
		try {
			AbstractCloudClient.getClient().deleteDatabase(appId);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
