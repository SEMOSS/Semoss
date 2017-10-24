package prerna.rpa.quartz;

import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;

public class BatchedJobInput {
	private final JobDataMap jobDataMap;
	private final Class<? extends InterruptableJob> jobClass;

	public BatchedJobInput(JobDataMap jobDataMap, Class<? extends InterruptableJob> jobClass) {
		this.jobDataMap = jobDataMap;
		this.jobClass = jobClass;
	}

	public JobDataMap getJobDataMap() {
		return jobDataMap;
	}

	public Class<? extends InterruptableJob> getJobClass() {
		return jobClass;
	}
}