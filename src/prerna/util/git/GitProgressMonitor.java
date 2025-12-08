package prerna.util.git;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.lib.ProgressMonitor;

public class GitProgressMonitor implements ProgressMonitor {

	private static final Logger classLogger = LogManager.getLogger(GitProgressMonitor.class);

	boolean complete = false;

	@Override
	public void beginTask(String arg0, int arg1) {
		classLogger.info("Started this task !!");

	}

	@Override
	public void endTask() {
		classLogger.info("Completed this task !!");
		complete = true;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public void start(int arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void update(int arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void showDuration(boolean enabled) {
		// TODO Auto-generated method stub

	}

}
