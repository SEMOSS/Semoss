package prerna.rpa.quartz.jobs.example;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.rpa.quartz.CommonDataKeys;

public class BakePieJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(BakePieJob.class.getName());

	/** {@code long} in seconds */
	public static final String IN_DURATION_KEY = BakePieJob.class + ".bakeDuration";

	/** {@code String} - the pie's condition */
	public static final String OUT_PIE_CONDITION_KEY = BakePieJob.class + ".pieCondition";

	/** {@code boolean} - true if the pie is safe to eat */
	public static final String OUT_IS_PIE_SAFE_TO_EAT = CommonDataKeys.BOOLEAN;

	private volatile boolean wasInterrupted = false;

	private Object monitor = new Object();

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
				
				// Preserve interrupt status
				Thread.currentThread().interrupt();
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
		boolean pieIsSafeToEat;
		if (duration < 2) {
			pieCondition = "undercooked";
			pieIsSafeToEat = false;
		} else if (duration < 4) {
			pieCondition = "just right";
			pieIsSafeToEat = true;
		} else if (duration < 6) {
			pieCondition = "burnt";
			pieIsSafeToEat = true;
		} else {
			throw new JobExecutionException("This pie has caught fire!");
		}
		dataMap.put(OUT_PIE_CONDITION_KEY, pieCondition);
		dataMap.put(OUT_IS_PIE_SAFE_TO_EAT, pieIsSafeToEat);
		LOGGER.info("Baked pie for " + duration + " seconds." + " This pie is " + pieCondition + ".");
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		wasInterrupted = true;
		synchronized (monitor) {
			monitor.notifyAll();
		}
	}

}
