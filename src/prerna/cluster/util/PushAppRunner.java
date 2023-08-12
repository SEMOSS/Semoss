package prerna.cluster.util;

import prerna.cluster.util.clients.AbstractCloudClient;

public class PushAppRunner implements Runnable {

	private final String appId;
	
	public PushAppRunner(String appId) {
		this.appId = appId;
	}
	
	@Override
	public void run() {
		try {
			AbstractCloudClient.getClient().pushDatabase(appId);
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

}
