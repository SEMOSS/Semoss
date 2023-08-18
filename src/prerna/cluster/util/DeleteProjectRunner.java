package prerna.cluster.util;

public class DeleteProjectRunner implements Runnable {

	private final String projectId;
	
	public DeleteProjectRunner(String projectId) {
		this.projectId = projectId;
	}
	
	@Override
	public void run() {
		try {
			ClusterUtil.deleteProject(projectId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
