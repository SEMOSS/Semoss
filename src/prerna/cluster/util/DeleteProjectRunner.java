package prerna.cluster.util;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.util.Constants;

public class DeleteProjectRunner implements Runnable {

	protected static final Logger logger = LogManager.getLogger(DeleteProjectRunner.class);

	private final String projectId;
	
	public DeleteProjectRunner(String projectId) {
		this.projectId = projectId;
	}
	
	@Override
	public void run() {
		try {
			ClusterUtil.deleteProject(projectId);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

}
