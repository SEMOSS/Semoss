package prerna.util.jobrunr;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Background retry scheduler for JobRunr jobs.
 * Polls for failed jobs and automatically retries them within configured limits.
 * 
 * This runs as a background task and only executes on the leader node in clustered environments.
 */
public class JobRunrRetryScheduler {
	
	private static final Logger LOGGER = LogManager.getLogger(JobRunrRetryScheduler.class);
	
	// Configuration
	private static final int POLLING_INTERVAL_SECONDS = 30; // Check every 30 seconds
	private static final int MAX_RETRIES = 3; // Default max retries per job
	private static final int BATCH_SIZE = 10; // Max jobs to retry per poll
	private static final int RETRY_DELAY_SECONDS = 60; // Wait 60 seconds before retrying failed job
	
	private final ScheduledExecutorService scheduler;
	private final JobRunrService jobRunrService;
	private volatile boolean running = false;
	
	/**
	 * Create retry scheduler
	 */
	public JobRunrRetryScheduler(JobRunrService jobRunrService) {
		this.jobRunrService = jobRunrService;
		this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread thread = new Thread(r, "JobRunr-Retry-Scheduler");
			thread.setDaemon(true);
			return thread;
		});
	}
	
	/**
	 * Start the retry scheduler
	 */
	public void start() {
		if (running) {
			LOGGER.warn("Retry scheduler is already running");
			return;
		}
		
		if (!JobRunrService.isJobRunrEnabled()) {
			LOGGER.info("JobRunr is not enabled, retry scheduler will not start");
			return;
		}
		
		running = true;
		LOGGER.info("Starting JobRunr retry scheduler (Polling every {} seconds, Max retries: {}, Batch size: {})",
			POLLING_INTERVAL_SECONDS, MAX_RETRIES, BATCH_SIZE);
		
		// Schedule polling task
		scheduler.scheduleAtFixedRate(
			this::pollAndRetryFailedJobs,
			10, // Initial delay of 10 seconds
			POLLING_INTERVAL_SECONDS,
			TimeUnit.SECONDS
		);
		
		LOGGER.info("JobRunr retry scheduler started successfully");
	}
	
	/**
	 * Stop the retry scheduler
	 */
	public void stop() {
		if (!running) {
			return;
		}
		
		running = false;
		LOGGER.info("Stopping JobRunr retry scheduler...");
		
		scheduler.shutdown();
		try {
			if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
				scheduler.shutdownNow();
			}
		} catch (InterruptedException e) {
			scheduler.shutdownNow();
			Thread.currentThread().interrupt();
		}
		
		LOGGER.info("JobRunr retry scheduler stopped");
	}
	
	/**
	 * Poll for failed jobs and retry them
	 */
	private void pollAndRetryFailedJobs() {
		try {
			LOGGER.debug("Polling for failed jobs that need retry...");
			
			// Get jobs that failed and can be retried
			List<Map<String, String>> failedJobs = jobRunrService.getJobsNeedingRetry(MAX_RETRIES, BATCH_SIZE);
			
			if (failedJobs == null || failedJobs.isEmpty()) {
				LOGGER.debug("No failed jobs found that need retry");
				return;
			}
			
			LOGGER.info("Found {} failed jobs that need retry", failedJobs.size());
			
			// Retry each failed job
			int successCount = 0;
			int failureCount = 0;
			
			for (Map<String, String> job : failedJobs) {
				String jobId = job.get("jobId");
				String jobName = job.get("jobName");
				String errorMessage = job.get("errorMessage");
				String retryCount = job.get("retryCount");
				
				try {
					LOGGER.info("Retrying failed job: {} (Name: {}, Retry: {}/{}, Last Error: {})",
						jobId, jobName, retryCount, MAX_RETRIES, errorMessage);
					
					// Trigger retry
					jobRunrService.retryFailedJob(jobId);
					successCount++;
					
					LOGGER.info("Successfully triggered retry for job: {}", jobId);
					
					// Add delay between retries to avoid overwhelming the system
					Thread.sleep(RETRY_DELAY_SECONDS * 1000);
					
				} catch (Exception e) {
					failureCount++;
					LOGGER.error("Failed to retry job: {} - Error: {}", jobId, e.getMessage(), e);
				}
			}
			
			LOGGER.info("Retry polling complete: {} succeeded, {} failed out of {} jobs",
				successCount, failureCount, failedJobs.size());
			
		} catch (Exception e) {
			LOGGER.error("Error during retry polling", e);
		}
	}
	
	/**
	 * Check if the scheduler is running
	 */
	public boolean isRunning() {
		return running;
	}
	
	/**
	 * Trigger immediate polling (for testing or manual trigger)
	 */
	public void triggerImmediatePoll() {
		LOGGER.info("Triggering immediate retry poll...");
		pollAndRetryFailedJobs();
	}
}
