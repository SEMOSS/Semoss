package prerna.util.git;

import org.apache.log4j.Logger;
import org.eclipse.jgit.lib.ProgressMonitor;

public class GitProgressMonitor implements ProgressMonitor {
	
	Logger logger = Logger.getLogger(this.getClass());

	boolean complete = false;
	
	@Override
	public void beginTask(String arg0, int arg1) {
		// TODO Auto-generated method stub
		logger.info("Started this task !!");

	}

	@Override
	public void endTask() {
		// TODO Auto-generated method stub
		logger.info("Completed this task !!");
		complete = true;
	}

	@Override
	public boolean isCancelled() {
		// TODO Auto-generated method stub
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

}
