package prerna.quartz;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

public class IfJob implements org.quartz.Job {

	public static final String IN_BOOLEAN_KEY = LinkedDataKeys.BOOLEAN;
	public static final String IN_IF_TRUE_JOB = "ifTrueJob";

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
				e.printStackTrace();
			}
		} else {
			System.out.println("Condition was not met, will not trigger the job " + jobName);
		}

		// Store outputs
		// No outputs to store here
	}

}
