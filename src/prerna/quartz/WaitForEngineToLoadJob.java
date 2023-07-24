package prerna.quartz;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.engine.api.IDatabase;
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
		IDatabase engine = null;
		while (loadingEngine) {
			engine = Utility.getEngine(engineName);
			if (engine == null) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
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
