package prerna.quartz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.util.Constants;

public class IfJob implements org.quartz.Job {

	private static final Logger classLogger = LogManager.getLogger(IfJob.class);

	public static final String IN_BOOLEAN_KEY = CommonDataKeys.BOOLEAN;
	public static final String IN_IF_TRUE_JOB = CommonDataKeys.IF_TRUE_JOB;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		boolean isTrue = dataMap.getBoolean(IN_BOOLEAN_KEY);
		JobDetail job = (JobDetail) dataMap.get(IN_IF_TRUE_JOB);
		String jobName = job.getKey().getName();

		// Do work
		if (isTrue) {
			System.out.println("Condition was met, triggering the job " + jobName);
			Scheduler scheduler = context.getScheduler();
			try {
				scheduler.addJob(job, true, true);
				scheduler.triggerJob(job.getKey());
			} catch (SchedulerException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			System.out.println("Condition was not met, will not trigger the job " + jobName);
		}

		// Store outputs
		// No outputs to store here
	}

}
