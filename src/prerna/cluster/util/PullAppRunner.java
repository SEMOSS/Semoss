package prerna.cluster.util;

import prerna.cluster.util.clients.AbstractCloudClient;

public class PullAppRunner implements Runnable {

	private final String appId;
	
	public PullAppRunner(String appId) {
		this.appId = appId;
	}
	
	@Override
	public void run() {
		try {
			AbstractCloudClient.getClient().pullDatabase(appId);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
