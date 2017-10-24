package prerna.rpa.quartz.jobs.example;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

public class BakePiesJob implements org.quartz.InterruptableJob {
	
	private static final Logger LOGGER = LogManager.getLogger(BakePiesJob.class.getName());
	
	/** {@code long} in seconds */
	public static final String IN_DURATION_KEY = BakePiesJob.class + ".bakeDuration";
	
	/** {@code String} - the pie's condition */
	public static final String OUT_PIE_CONDITION = BakePiesJob.class + ".pieCondition";

	private boolean wasInterrupted = false;

	private static Object monitor = new Object(); 
		
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		String jobName = context.getJobDetail().getKey().getName();
		JobDataMap dataMap = context.getMergedJobDataMap();
		long duration = dataMap.getLong(IN_DURATION_KEY);

		////////////////////
		// Do work
		////////////////////
		// "Bake" the pie for the given duration
		synchronized (monitor) {
			try {
				monitor.wait(duration * 1000);
			} catch (InterruptedException e) {
				LOGGER.error("Thread for the " + jobName + " interrupted in an unexpected manner.", e);
			}
		}
		if (wasInterrupted) {
			LOGGER.info("Gracefully terminated the " + jobName + " job.");
			return;
		}
		
		////////////////////
		// Store outputs
		////////////////////
		String pieCondition;
		if (duration < 2) {
			pieCondition = "undercooked";
		} else if (duration < 4) {
			pieCondition = "just right";
		} else if (duration < 6) {
			pieCondition = "burnt";
		} else {
			throw new JobExecutionException("This pie has caught fire!");
		}
		dataMap.put(OUT_PIE_CONDITION, pieCondition);

		LOGGER.info("Baked pie for " + duration + " seconds." + " This pie is " + pieCondition + ".");
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		wasInterrupted = true;
		synchronized (monitor) {
			monitor.notify();
		}
	}

}
