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

import prerna.rpa.quartz.CommonDataKeys;

public class IfJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(IfJob.class.getName());
	
	/** {@code boolean} */
	public static final String IN_BOOLEAN_KEY = CommonDataKeys.BOOLEAN;
	
	/** {@code JobDetail} */
	public static final String IN_IF_TRUE_JOB_KEY = IfJob.class + ".ifTrueJob";

	private String jobName;
	private boolean interrupted = false;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		////////////////////
		// Get inputs
		////////////////////
		jobName = context.getJobDetail().getKey().getName();
		JobDataMap jobDataMap = context.getMergedJobDataMap();
		boolean isTrue = jobDataMap.getBoolean(IN_BOOLEAN_KEY);
		JobDetail ifTrueJob = (JobDetail) jobDataMap.get(IN_IF_TRUE_JOB_KEY);
		String ifTrueJobName = ifTrueJob.getKey().getName();
		
		////////////////////
		// Do work
		////////////////////
		
		// For the data map, add the context first
		// Then add the map for the if true job
		// Thus values in the map for the if true job will take precedence over the context
		// Don't want to override the original job data map, so we create a new one
		JobDataMap ifTrueJobDataMap = new JobDataMap();
		ifTrueJobDataMap.putAll(jobDataMap);
		ifTrueJobDataMap.putAll(ifTrueJob.getJobDataMap());
		
		// Update the data map to include the context
		ifTrueJob = ifTrueJob.getJobBuilder().usingJobData(ifTrueJobDataMap).build();
		if (isTrue && !interrupted) {
			LOGGER.info("Condition was met, triggering the job " + ifTrueJobName + ".");
			Scheduler scheduler = context.getScheduler();
			try {
				scheduler.addJob(ifTrueJob, true, true);
				scheduler.triggerJob(ifTrueJob.getKey());
			} catch (SchedulerException e) {
				String triggerJobExceptionMessage = "Failed to trigger the job " + ifTrueJobName + ".";
				LOGGER.error(triggerJobExceptionMessage);
				throw new JobExecutionException(triggerJobExceptionMessage);
			}
		} else {
			LOGGER.info("Condition was not met, will not trigger the job " + ifTrueJobName + ".");
		}

		////////////////////
		// Store outputs
		////////////////////
		// No outputs to store here
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("The " + jobName + " job was interrupted. Will not trigger the if true job.");
	}

}
