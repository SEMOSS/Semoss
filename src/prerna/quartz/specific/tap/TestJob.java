package prerna.quartz.specific.tap;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class TestJob implements org.quartz.Job {
	public static final String IN_JSON_STRING_KEY = CheckCriteriaJob.JSON_STRING;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		//get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		String anomalyString = dataMap.getString(IN_JSON_STRING_KEY);

		// Do work
		System.out.println("Test Job successfully completed");
		System.out.println(anomalyString);

		// Store outputs
		// No outputs to store here
	}

}
