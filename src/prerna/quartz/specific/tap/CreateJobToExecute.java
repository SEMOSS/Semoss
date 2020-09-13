package prerna.quartz.specific.tap;

import static org.quartz.JobBuilder.newJob;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import prerna.quartz.LinkedDataKeys;

public class CreateJobToExecute implements org.quartz.Job {

	public static final String OUT_TRUE_JOB_KEY = LinkedDataKeys.IF_TRUE_JOB;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		//build the job that should execute after IfJob
		JobDataMap dataMap = context.getMergedJobDataMap();
		
		JobDetail executeableJob = newJob(TestJob.class)
				.withIdentity("jobToExecute", "group2")
				.usingJobData(dataMap)
				.build();
		
		dataMap.put(LinkedDataKeys.IF_TRUE_JOB, executeableJob);
		
	}
}
