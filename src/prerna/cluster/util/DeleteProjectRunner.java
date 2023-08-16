package prerna.cluster.util;

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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
