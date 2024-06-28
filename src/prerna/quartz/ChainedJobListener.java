package prerna.quartz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import prerna.util.Constants;

public class ChainedJobListener extends org.quartz.listeners.JobListenerSupport {

	private static final Logger classLogger = LogManager.getLogger(ChainedJobListener.class);

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
		try {
			jobChain.setDataMap(context.getMergedJobDataMap());
			jobChain.executeElement();
		} catch (SchedulerException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

}
