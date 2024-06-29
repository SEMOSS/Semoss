package prerna.quartz;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.engine.api.IDatabaseEngine;
import prerna.util.Utility;

public class WaitForEngineToLoadJob implements org.quartz.Job {

	public static final String IN_ENGINE_NAME_KEY = CommonDataKeys.ENGINE_NAME;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		String engineName = dataMap.getString(IN_ENGINE_NAME_KEY);

		// Do work
		boolean loadingEngine = true;
		IDatabaseEngine engine = null;
		while (loadingEngine) {
			engine = Utility.getDatabase(engineName);
			if (engine == null) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
//					classLogger.error(Constants.STACKTRACE, e);
				}
			} else {
				loadingEngine = false;
			}
		}

		// Store outputs
		// No outputs to store here

		System.out.println();
		System.out.println("The engine " + engineName + " has loaded");
		System.out.println();
	}

}
