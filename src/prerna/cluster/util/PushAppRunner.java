package prerna.cluster.util;

import java.io.IOException;

public class PushAppRunner implements Runnable {

	private final String appId;
	
	public PushAppRunner(String appId) {
		this.appId = appId;
	}
	
	@Override
	public void run() {
		try {
			CloudClient.getClient().pushApp(appId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}

}
