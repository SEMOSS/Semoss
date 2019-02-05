package prerna.cluster.util;

import java.io.IOException;

public class DeleteAppRunner implements Runnable {

	private final String appId;
	
	public DeleteAppRunner(String appId) {
		this.appId = appId;
	}
	
	@Override
	public void run() {
		try {
			CloudClient.getClient().deleteApp(appId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

}
