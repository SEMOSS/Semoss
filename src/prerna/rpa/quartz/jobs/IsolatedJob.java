package prerna.rpa.quartz.jobs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;

public class IsolatedJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(IsolatedJob.class.getName());
	
	/** {@code JobDetail} */
	public static final String IN_ISOLATED_JOB_KEY = IsolatedJob.class + ".isolatedJob";
	
	private String jobName;
	private boolean interrupted = false;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		jobName = context.getJobDetail().getKey().getName();
		JobDataMap jobDataMap = context.getMergedJobDataMap();
		JobDetail isolatedJob = (JobDetail) jobDataMap.get(IN_ISOLATED_JOB_KEY);
		String isolatedJobName = isolatedJob.getKey().getName();
		
		////////////////////
		// Do work
		////////////////////
		if (!interrupted) {
			LOGGER.info("Triggering the job " + isolatedJobName + ".");
			Scheduler scheduler = context.getScheduler();
			try {
				scheduler.addJob(isolatedJob, true, true);
				scheduler.triggerJob(isolatedJob.getKey());
			} catch (SchedulerException e) {
				String triggerJobExceptionMessage = "Failed to trigger the job " + isolatedJobName + ".";
				LOGGER.error(triggerJobExceptionMessage);
				throw new JobExecutionException(triggerJobExceptionMessage);
			}
		}
		
		////////////////////
		// Store outputs
		////////////////////
		// No outputs to store here
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("The " + jobName + " job was interrupted. Will not trigger the isolated job.");		
	}

}
