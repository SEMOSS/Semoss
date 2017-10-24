package prerna.rpa.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ChainedJobListener extends org.quartz.listeners.JobListenerSupport {

	String name;
	JobChain jobChain;

	public ChainedJobListener(String name, JobChain chainJob) {
		this.name = name;
		this.jobChain = chainJob;
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
		jobChain.proceedToNextJob(context, jobException);
	}

}
