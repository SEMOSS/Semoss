package prerna.rpa.quartz;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.impl.matchers.GroupMatcher.jobGroupEquals;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;

public class JobChain implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(JobChain.class.getName());
	
	/** {@code List<Class<? extends InterruptableJob>>} - a sequence of job classes */
	public static final String IN_CHAIN_SEQUENCE_KEY = CommonDataKeys.CHAIN_SEQUENCE;

	private String jobName;
	private String chainedJobGroup;
	private Scheduler scheduler;
	private JobDataMap jobDataMap;
	private List<Class<? extends InterruptableJob>> sequence;

	private int terminal;
	private int offset = 0;
	
	private Exception failureException;

	private String terminationMessage;
	
	private JobKey currentlyExecutingJobKey;
	
	private volatile boolean failed = false;
	private volatile boolean interrupted = false;

	// Don't make this static in case there is more than one JobChain
	private Object jobMonitor = new Object();
	
	@SuppressWarnings("unchecked")
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		jobName = context.getJobDetail().getKey().getName();
		chainedJobGroup = jobName + "ChainGroup";
		scheduler = context.getScheduler();
		jobDataMap = context.getMergedJobDataMap();
		sequence = (List<Class<? extends InterruptableJob>>) jobDataMap.get(IN_CHAIN_SEQUENCE_KEY);
		terminal = sequence.size();
		terminationMessage = "Will terminate the " + this.jobName + " job chain.";
		
		////////////////////
		// Do work
		////////////////////
		
		// Proceed to the first job in the chain
		JobListener chainedJobListener = new ChainedJobListener(chainedJobGroup + ".chainedJobListener", this);
		try {
			scheduler.getListenerManager().addJobListener(chainedJobListener, jobGroupEquals(chainedJobGroup));
			proceedToNextJob(context, null);
		} catch (SchedulerException e) {
			String getListenerExceptionMessage = "Failed to add the job chain listener to the scheduler. ";
			LOGGER.error(getListenerExceptionMessage + terminationMessage);
			throw new JobExecutionException(getListenerExceptionMessage, e);
		}
		
		// Keep the job alive until the chain is finished or interrupted
		// Wait for proceedToNextJob to notify (either in error or in completion)
		synchronized (jobMonitor) {
			try {
				jobMonitor.wait();
			} catch (InterruptedException e) {
				LOGGER.error("Thread for the " + jobName + " interrupted in an unexpected manner.", e);
				
				// Preserve interrupt status
				Thread.currentThread().interrupt();
			}
		}
		if (failed) {
			String failureMessage = "Failed to execute jobs in the " + jobName + " job chain.";
			LOGGER.error(failureMessage);
			throw new JobExecutionException(failureMessage, failureException);
		}
		if (interrupted) {
			LOGGER.info("Gracefully terminated the " + jobName + " job chain.");			
		}
		
		////////////////////
		// Store outputs
		////////////////////
		// No outputs to store here
	}

	protected void proceedToNextJob(JobExecutionContext context, JobExecutionException jobException) {
		
		// Pull data from the last job
		// Update the data map with the latest merged map
		jobDataMap = context.getMergedJobDataMap();
		String previousJobName = context.getJobDetail().getKey().getName();
		
		// Check to see whether the previous job was interrupted
		if (interrupted) {
			LOGGER.warn("The previous job in the chain, " + previousJobName + " was interrupted. " + terminationMessage);
			
			// Terminate the chain without throwing an exception
			notifyJobMonitor();
			return;
		}
		
		// Check to see whether the previous job failed
		if (jobException != null) {
			LOGGER.error("The previous job in the chain, " + previousJobName + " failed. " + terminationMessage);
			
			// Terminate the chain
			fail(jobException);
			return;
		}
					
		// Continue if there are more jobs in the sequence
		if (offset < terminal) {
			Class<? extends InterruptableJob> jobClass = sequence.get(offset);
			String chainedJobName = jobClass.getSimpleName() + "@" + jobName + "[" + offset + "]";
			JobDetail chainedJob = newJob(jobClass).withIdentity(chainedJobName, chainedJobGroup).usingJobData(jobDataMap).build();
			
			// If the scheduler cannot add the next job in the sequence, then terminate the chain
			try {
				scheduler.addJob(chainedJob, true, true);
			} catch (SchedulerException e) {
				LOGGER.error("Failed to add the job " + chainedJobName + " to the scheduler. " + terminationMessage, e);
								
				// Terminate the chain
				fail(e);
				return;
			}
			LOGGER.info("Added the job " + chainedJobName + " to " + chainedJobGroup + ".");
			JobKey chainedJobKey = chainedJob.getKey();
			
			// If the scheduler cannot trigger job, then try deleting the job before terminating the chain
			try {
				scheduler.triggerJob(chainedJobKey);
				
				// If successfully triggered, make a note of it as currently executing
				// In case the chain is interrupted, we can terminate this job
				currentlyExecutingJobKey = chainedJobKey;
				LOGGER.info("Triggered the job " + chainedJobName + ".");
			} catch (SchedulerException e) {
				LOGGER.error("Failed to trigger job " + chainedJobName + ". " + terminationMessage, e);
				QuartzUtil.removeJob(scheduler, chainedJobKey);
				
				// Terminate the chain
				fail(e);
				return;
			}
			offset++;
		} else {
			LOGGER.info("Finished executing jobs in the " + jobName + " job chain.");
			notifyJobMonitor();
		}
	}

	// Set failed to true so that we can throw the exception
	private void fail(Exception e) {
		failed = true;
		failureException = e;
		notifyJobMonitor();
	}
	
	private void notifyJobMonitor() {
		synchronized (jobMonitor) {
			jobMonitor.notifyAll();
		}
	}
	
	@Override
	public void interrupt() throws UnableToInterruptJobException {
		interrupted = true;
		LOGGER.warn("The " + jobName + " job chain has been interrupted.");
		
		// Do this by terminating the currently executing job
		QuartzUtil.terminateJob(scheduler, currentlyExecutingJobKey);
	}

}
