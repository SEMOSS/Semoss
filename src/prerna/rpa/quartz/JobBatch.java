package prerna.rpa.quartz;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.impl.matchers.GroupMatcher.jobGroupEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;

public class JobBatch implements org.quartz.InterruptableJob {
	
	private static final Logger LOGGER = LogManager.getLogger(JobBatch.class.getName());
		
	/** {@code long} in seconds */
	public static final String IN_TIMEOUT_KEY = JobBatch.class + ".timeout";
	
	/** {@code Map<String, BatchedJobInput>} - (identifier, (JobDataMap, Class<? extends InterruptableJob>)) */
	public static final String IN_BATCH_INPUT_MAP_KEY = CommonDataKeys.BATCH_INPUT_MAP;
	
	/** {@code Map<String, BatchedJobOutput>} - (identifier, (JobDataMap, success)) */
	public static final String OUT_BATCH_OUTPUT_MAP_KEY = CommonDataKeys.BATCH_OUTPUT_MAP;
	
	/** {@code boolean} */
	public static final String OUT_ALL_JOBS_SUCCESSFUL_KEY = CommonDataKeys.BOOLEAN;
	
	private String jobName;
	private String batchedJobGroup;
	private Scheduler scheduler;
	private int totalJobs;
	private int noCompleted;
	
	private boolean timedOut = false;
	private boolean interrupted = false;
	private boolean completed = false;
	
	// (Identifier, JobDataMap)
	private Map<String, BatchedJobOutput> batchOutputMap = new HashMap<String, BatchedJobOutput>();
	
	// (Identifier, Success)
	private Map<String, Boolean> batchStatus = new HashMap<String, Boolean>();
	
	// Don't make this static in case there is more than one JobBatche
	private Object jobMonitor = new Object();
	
	@SuppressWarnings("unchecked")
	public void execute(JobExecutionContext context) throws JobExecutionException {
		jobName = context.getJobDetail().getKey().getName();
		batchedJobGroup = jobName + "BatchGroup";
		scheduler = context.getScheduler();
		JobDataMap jobDataMap = context.getMergedJobDataMap();
				
		// (Identifier, (JobDataMap, Class<? extends Job>))
		Map<String, BatchedJobInput> batchMap = (Map<String, BatchedJobInput>) jobDataMap.get(IN_BATCH_INPUT_MAP_KEY);
		totalJobs = batchMap.size();
		
		// How long before we stop job execution
		long timeout = jobDataMap.getLong(IN_TIMEOUT_KEY);
		
		// Add a listener to the scheduler
		JobListener batchedJobListener = new BatchedJobListener(batchedJobGroup + ".batchedJobListener", this);

		try {
			
			// If there is nesting of chains/batches, then the sub chain/batch will override the group by prepending its info
			// Therefore, check whether it ends with the group
			scheduler.getListenerManager().addJobListener(batchedJobListener, jobGroupEquals(batchedJobGroup));
		} catch (SchedulerException e) {
			terminateBatch("Failed to add the job batch listener to the scheduler.", e);
		}
					
		// Loop through each job and trigger it
		for (String identifier : batchMap.keySet()) {
			BatchedJobInput batchedJobTuple = batchMap.get(identifier);
			
			// For the data map, add the context first
			// Then add the map for the batched job
			// Thus values in the map for the batched job will take precedence over the context
			JobDataMap batchedJobDataMap = jobDataMap;
			batchedJobDataMap.putAll(batchedJobTuple.getJobDataMap());
			JobDetail batchedJob = newJob(batchedJobTuple.getJobClass()).withIdentity(identifier, batchedJobGroup).usingJobData(batchedJobDataMap).build();
			JobKey batchedJobKey = batchedJob.getKey();
			try {
				scheduler.addJob(batchedJob, true, true);
			} catch (SchedulerException e) {
				terminateBatch("Failed to add the job " + identifier + " to the scheduler.", e);
			}
			LOGGER.info("Added the job " + identifier + " to " + batchedJobGroup + ".");
			try {
				scheduler.triggerJob(batchedJobKey);
				LOGGER.info("Triggered the job " + identifier + ".");
			} catch (SchedulerException e) {
				terminateBatch("Failed to trigger the job " + identifier + ".", e);
			}
		}
		
		// Set an alarm for the timeout
		JobBatchTimeoutAlarm alarm = new JobBatchTimeoutAlarm(jobMonitor, timeout, this);
		Thread alarmThread = new Thread(alarm);
		alarmThread.setName(jobName + "_TimeoutAlarm");
		alarmThread.start();
		
		// Wait for all the jobs to finish and report the progress
		long startTime = System.currentTimeMillis();
		while (!completed) {
			
			// The jobMonitor is notified whenever a job is completed, or when a timeout occurs
			synchronized (jobMonitor) {
				try {
					jobMonitor.wait();
				} catch (InterruptedException e) {
					LOGGER.error("Thread for the " + jobName + " interrupted in an unexpected manner.", e);
				}
			}
			
			// The timeout alarm will set timeOut to true if a timeout has occurred
			// If so, terminate the batch
			if (timedOut) {
				terminateBatch("The job batch " + jobName + " has timed out after a period of " + QuartzUtility.minutesSinceStartTime(startTime) + " minutes.");
			}
			
			// This job will set interrupted to true when interrupted
			// If so, terminate the batch
			if (interrupted) {
				terminateBatch("The " + jobName + " job chain has been interrupted.");
			}
		}
		
		// If the jobs have been completed, notify the timeout alarm thread to stop waiting
		alarm.interrupt();
		
		// Send the accumulated data from each job as the output
		jobDataMap.put(OUT_BATCH_OUTPUT_MAP_KEY, batchOutputMap);
		
		// Send whether all jobs were successful
		long successfulJobs = batchStatus.values().stream().filter(s -> s == true).count();
		jobDataMap.put(OUT_ALL_JOBS_SUCCESSFUL_KEY, successfulJobs == totalJobs);
		
		long elapsedTime = QuartzUtility.minutesSinceStartTime(startTime);
		LOGGER.info("Job batch complete. Elapsed time " + elapsedTime + " minutes. " + successfulJobs + "/" +  totalJobs + " jobs completed successfully.");
	}
	
	protected void completeJob(JobExecutionContext context, JobExecutionException jobException) {
		
		// Pull data from the completed job
		// Batched job name is equal to the identifier
		String batchedJobName = context.getJobDetail().getKey().getName();
		JobDataMap batchedJobDataMap = context.getMergedJobDataMap();

		// If the job completed in error or was terminated, mark the job as failed
		boolean success = false;
		if (timedOut) {
			LOGGER.error("The job " + batchedJobName + " was terminated due to a timeout condition.");
		} else if (interrupted) {
			LOGGER.error("The job " + batchedJobName + " was interrupted.");
		} else if (jobException != null) {
			jobException.printStackTrace();
			LOGGER.error("The job " + batchedJobName + " failed.");
		} else {
			LOGGER.info("The job " + batchedJobName + " has successfully completed.");
			success = true;
		}
		batchStatus.put(batchedJobName, success);
		
		// Accumulate the data for each completed job
		// Go ahead and put the data map in even if not successful, there will be some data
		// It just won't be complete
		batchOutputMap.put(batchedJobName, new BatchedJobOutput(batchedJobDataMap, success));
		
		// Increment the number of completed jobs
		noCompleted++;
		
		// Specify whether the jobs have been completed
		completed = noCompleted == totalJobs;
		
		// Notify the execute method that a job has been completed
		synchronized (jobMonitor) {
			jobMonitor.notify();
		}
	}

	private void terminateBatch(String errorMessage) throws JobExecutionException {
		terminateBatch(errorMessage, null);
	}
	
	private void terminateBatch(String errorMessage, Exception e) throws JobExecutionException {
		boolean withException = e != null;
		LOGGER.error(errorMessage);
		LOGGER.error("Terminating the " + jobName + " job batch.");
		QuartzUtility.terminateAllJobsInGroup(scheduler, batchedJobGroup);
		QuartzUtility.removeAllJobsInGroup(scheduler, batchedJobGroup);
		
		// Throw an exception so that :
		// 1) stop executing this job 
		// 2) other jobs are aware this was terminated/failed
		if (withException) {
			throw new JobExecutionException(errorMessage, e);
		} else {
			throw new JobExecutionException(errorMessage);
		}
	}
	
	public void setTimedOut(boolean timedOut) {
		this.timedOut = timedOut;
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {

		// Notify the waiting jobMonitor with the interrupted=true flag
		interrupted = true;
		synchronized (jobMonitor) {
			jobMonitor.notify();
		}	
	}
	
}
