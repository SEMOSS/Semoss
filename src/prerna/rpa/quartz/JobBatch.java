package prerna.rpa.quartz;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.impl.matchers.GroupMatcher.jobGroupEquals;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;

import prerna.rpa.RPAUtil;

public class JobBatch implements org.quartz.InterruptableJob {
	
	private static final Logger LOGGER = LogManager.getLogger(JobBatch.class.getName());
		
	/** {@code long} in seconds */
	public static final String IN_TIMEOUT_KEY = JobBatch.class + ".timeout";
	
	/** {@code Map<String, BatchedJobInput>} - (identifier, ({@code JobDataMap}, {@code Class<? extends InterruptableJob>})) */
	public static final String IN_BATCH_INPUT_MAP_KEY = CommonDataKeys.BATCH_INPUT_MAP;
	
	/** {@code Map<String, BatchedJobOutput>} - (identifier, ({@code JobDataMap}, success)) */
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
	
	private JobBatchTimeoutAlarm alarm;
	
	// (Identifier, JobDataMap)
	private final Map<String, BatchedJobOutput> batchOutputMap = new ConcurrentHashMap<>();
	
	// (Identifier, Success)
	private final Map<String, Boolean> batchStatus = new ConcurrentHashMap<>();
		
	// Don't make this static in case there is more than one JobBatch
	private final Object jobMonitor = new Object();
	
	@SuppressWarnings("unchecked")
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		jobName = context.getJobDetail().getKey().getName();
		batchedJobGroup = jobName + "BatchGroup";
		scheduler = context.getScheduler();
		JobDataMap jobDataMap = context.getMergedJobDataMap();
				
		// (Identifier, (JobDataMap, Class<? extends Job>))
		Map<String, BatchedJobInput> batchMap = (Map<String, BatchedJobInput>) jobDataMap.get(IN_BATCH_INPUT_MAP_KEY);
		totalJobs = batchMap.size();
		
		// How long before we stop job execution
		long timeout = jobDataMap.getLong(IN_TIMEOUT_KEY);
		
		////////////////////
		// Do work
		////////////////////
		
		// Set an alarm for the timeout
		alarm = new JobBatchTimeoutAlarm(jobMonitor, timeout, this);
		Thread alarmThread = new Thread(alarm);
		alarmThread.setName(jobName + "_TimeoutAlarm");
		alarmThread.start();
		
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
		triggerBatch(batchMap, jobDataMap);
				
		// Wait for all the jobs to finish and report the progress
		long startTime = System.currentTimeMillis();
		while (!completed) {
			
			// The jobMonitor is notified whenever a job is completed, or when a timeout occurs
			synchronized (jobMonitor) {
				try {
					jobMonitor.wait();
				} catch (InterruptedException e) {
					LOGGER.error("Thread for the " + jobName + " interrupted in an unexpected manner.", e);
					
					// Preserve interrupt status
					Thread.currentThread().interrupt();
				}
			}
			
			// The timeout alarm will set timeOut to true if a timeout has occurred
			// If so, terminate the batch
			if (timedOut) {
				terminateBatch("The job batch " + jobName + " has timed out after a period of " + RPAUtil.minutesSinceStartTime(startTime) + " minutes.");
			}
			
			// This job will set interrupted to true when interrupted
			// If so, terminate the batch
			if (interrupted) {
				try {
					terminateBatch("The " + jobName + " job batch has been interrupted.");
				} catch (JobExecutionException e) {
					
					// When interrupted, do not need to throw an exception
					LOGGER.info("Gracefully terminated the " + jobName + " job batch.");
					return;
				}
			}
		}
		
		// If the jobs have been completed, notify the timeout alarm thread to stop waiting
		alarm.interrupt();
		
		////////////////////
		// Store outputs
		////////////////////
		
		// Send the accumulated data from each job as the output
		jobDataMap.put(OUT_BATCH_OUTPUT_MAP_KEY, batchOutputMap);
		
		// Send whether all jobs were successful
		long successfulJobs = batchStatus.values().stream().filter(s -> s).count();
		jobDataMap.put(OUT_ALL_JOBS_SUCCESSFUL_KEY, successfulJobs == totalJobs);
		
		long elapsedTime = RPAUtil.minutesSinceStartTime(startTime);
		LOGGER.info("Job batch complete. Elapsed time " + elapsedTime + " minutes. " + successfulJobs + "/" +  totalJobs + " jobs completed successfully.");
	}
	
	private void triggerBatch(Map<String, BatchedJobInput> batchMap, JobDataMap jobDataMap) throws JobExecutionException {
		for (Entry<String, BatchedJobInput> entry : batchMap.entrySet()) {
			String identifier = entry.getKey();
			BatchedJobInput batchedJobTuple = entry.getValue();
			
			// For the data map, add the context first
			// Then add the map for the batched job
			// Thus values in the map for the batched job will take precedence over the context
			// Don't want to override the original job data map, so we create a new one
			JobDataMap batchedJobDataMap = new JobDataMap();
			batchedJobDataMap.putAll(jobDataMap);
			batchedJobDataMap.putAll(batchedJobTuple.getJobDataMap());
			JobDetail batchedJob = newJob(batchedJobTuple.getJobClass()).withIdentity(identifier, batchedJobGroup).usingJobData(batchedJobDataMap).build();
			JobKey batchedJobKey = batchedJob.getKey();
			try {
				scheduler.addJob(batchedJob, true, true);
			} catch (SchedulerException e) {
				terminateBatch("Failed to add the job " + identifier + " to the scheduler.", e);
			}
			LOGGER.info("Added the job " + identifier + " to " + batchedJobGroup + ".");
			if (!interrupted) {
				try {
					scheduler.triggerJob(batchedJobKey);
					LOGGER.info("Triggered the job " + identifier + ".");
				} catch (SchedulerException e) {
					terminateBatch("Failed to trigger the job " + identifier + ".", e);
				}
			}
		}
	}
	
	protected void completeJob(JobExecutionContext context, JobExecutionException jobException) {
		
		// Pull data from the completed job
		// Batched job name is equal to the identifier
		String batchedJobName = context.getJobDetail().getKey().getName();
		JobDataMap batchedJobDataMap = context.getMergedJobDataMap();

		// If the job completed in error or was terminated, mark the job as failed
		boolean success = false;
		if (timedOut) {
			LOGGER.warn("The job " + batchedJobName + " was terminated due to a timeout condition.");
		} else if (interrupted) {
			LOGGER.warn("The job " + batchedJobName + " was interrupted.");
		} else if (jobException != null) {
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
			jobMonitor.notifyAll();
		}
	}

	private void terminateBatch(String errorMessage) throws JobExecutionException {
		terminateBatch(errorMessage, null);
	}
	
	private void terminateBatch(String errorMessage, Exception e) throws JobExecutionException {
		LOGGER.warn(errorMessage);
		LOGGER.warn("Terminating the " + jobName + " job batch.");
		QuartzUtil.terminateAllJobsInGroup(scheduler, batchedJobGroup);
		
		// Before terminating, notify the timeout alarm thread to stop waiting
		alarm.interrupt();
		
		// Throw an exception so that :
		// 1) stop executing this job 
		// 2) other jobs are aware this was terminated/failed
		if (e != null) {
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
		interrupted = true;
		
		// Notify the waiting jobMonitor with the interrupted=true flag
		synchronized (jobMonitor) {
			jobMonitor.notifyAll();
		}	
	}
	
}
