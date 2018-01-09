package prerna.rpa.quartz.jobs.example;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

public class EatPieJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(EatPieJob.class.getName());
	
	/** {@code String} - the pie's condition */
	public static final String IN_PIE_CONDITION_KEY = BakePieJob.OUT_PIE_CONDITION_KEY;
	
	private String jobName;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		jobName = context.getJobDetail().getKey().getName();
		JobDataMap dataMap = context.getMergedJobDataMap();
		String pieCondition = dataMap.getString(IN_PIE_CONDITION_KEY);
		
		////////////////////
		// Do work
		////////////////////
		if (pieCondition.equals("just right")) {
			LOGGER.info("Yum.");
		} else {
			LOGGER.info("Ick.");
		}
		
		////////////////////
		// Store outputs
		////////////////////
		// Nothing to store here
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");		
	}

}
