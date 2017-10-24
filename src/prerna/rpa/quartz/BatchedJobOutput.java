package prerna.rpa.quartz;

import org.quartz.JobDataMap;

public class BatchedJobOutput {

	private JobDataMap jobDataMap;
	private boolean success;
	
	public BatchedJobOutput(JobDataMap jobDataMap, boolean success) {
		this.jobDataMap = jobDataMap;
		this.success = success;
	}
	
	public JobDataMap getJobDataMap() {
		return jobDataMap;
	}
	
	public boolean wasSuccessful() {
		return success;
	}
}
