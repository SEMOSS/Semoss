package prerna.cluster.util;

import java.io.IOException;

import prerna.cluster.util.clients.AbstractCloudClient;

public class DeleteProjectRunner implements Runnable {

	private final String projectId;
	
	public DeleteProjectRunner(String projectId) {
		this.projectId = projectId;
	}
	
	@Override
	public void run() {
		try {
			AbstractCloudClient.getClient().deleteProject(projectId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
