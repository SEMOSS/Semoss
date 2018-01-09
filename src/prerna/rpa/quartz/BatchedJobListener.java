package prerna.rpa.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class BatchedJobListener extends org.quartz.listeners.JobListenerSupport {

	final String name;
	final JobBatch jobBatch;
	
	public BatchedJobListener(String name, JobBatch jobBatch) {
		this.name = name;
		this.jobBatch = jobBatch;
	}
	
	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
		jobBatch.completeJob(context, jobException);
	}
	
}
