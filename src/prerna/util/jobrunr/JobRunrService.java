/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.jobrunr;

import java.io.File;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jobrunr.configuration.JobRunr;
import org.jobrunr.configuration.JobRunrConfiguration.JobRunrConfigurationResult;
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jobrunr.server.BackgroundJobServerConfiguration;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.storage.StorageProviderUtils.DatabaseOptions;
import org.jobrunr.storage.sql.h2.H2StorageProvider;

import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.rpa.jobrunr.jobs.JobRunrPixelExecutionJobRequest;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.jobrunr.model.JobMetadata;
import prerna.util.jobrunr.model.JobStatus;

public class JobRunrService {

	private static final Logger LOGGER = LogManager.getLogger(JobRunrService.class);
	private static final String DEFAULT_ENABLED_VALUE = "false";
	private static JobRunrService instance = null;
	private JobRequestScheduler jobRequestScheduler;
	private StorageProvider storageProvider;
	private JobRunrRetryScheduler retryScheduler;  // Retry scheduler for failed jobs
	private boolean enabled;

	private JobRunrService() {
		this.enabled = isJobRunrEnabled();
		LOGGER.info("JobRunr Service initialized{}", enabled ? "" : " (disabled)");
		if (enabled) {
			init();
		}
	}

	/**
	 * Get singleton instance of JobRunrService
	 * 
	 * @return Singleton instance
	 */
	public static synchronized JobRunrService getInstance() {
		if (instance == null) {
			instance = new JobRunrService();
		}
		return instance;
	}

	/**
	 * Reset instance
	 */
	public static synchronized void resetInstance() {
		if (instance != null) {
			instance.shutdown(false);
			instance = null;
		}
	}

	public static boolean isJobRunrEnabled() {
		try {
			String flag = Utility.getDIHelperProperty(Constants.SCHEDULER_USE_JOBRUNR);
			if (flag == null) {
				flag = DEFAULT_ENABLED_VALUE; // Default to false
			}
			return Boolean.parseBoolean(flag);
		} catch (Exception e) {
			LOGGER.warn("Could not read " + Constants.SCHEDULER_USE_JOBRUNR + ", defaulting to false");
			return false;
		}
	}

	public static JobRunrService getJobRunrService() {
		if (!isJobRunrEnabled()) {
			throw new IllegalStateException("JobRunr is disabled. Set " + Constants.SCHEDULER_USE_JOBRUNR + "=true to enable.");
		}

		try {
			return JobRunrService.getInstance();
		} catch (Exception e) {
			throw new RuntimeException("Failed to initialize JobRunrService", e);
		}
	}

	/**
	 * Initialize storage provider using SEMOSS database configuration
	 */
	private void init() {
		try {
			javax.sql.DataSource dataSource = getDataSourceFromDIHelper();

			if (dataSource == null) {
				throw new RuntimeException("Cannot obtain DataSource for JobRunr");
			}

			String jobrunrPortStr = Utility.getDIHelperProperty(Constants.JOBRUNR_DASHBOARD_PORT);
			int jobrunrPort = 8000; // default port

			if (jobrunrPortStr != null && !jobrunrPortStr.trim().isEmpty()) {
				try {
					jobrunrPort = Integer.parseInt(jobrunrPortStr);
				} catch (NumberFormatException e) {
					LOGGER.warn("Invalid jobrunr_dashboard_port value: {}. Using default 8000", jobrunrPortStr);
				}
			}
			
			// Get configurable worker count
			int workerCount = getWorkerCount();
			LOGGER.info("Configuring JobRunr with {} workers", workerCount);

			this.storageProvider = new H2StorageProvider(dataSource, DatabaseOptions.CREATE);

			JobRunrConfigurationResult config = JobRunr.configure().useStorageProvider(storageProvider)
					.useBackgroundJobServer(BackgroundJobServerConfiguration
							.usingStandardBackgroundJobServerConfiguration().andWorkerCount(workerCount))
					.useDashboard(jobrunrPort).initialize();

			// Get instances
			this.jobRequestScheduler = config.getJobRequestScheduler();
			LOGGER.info("JobRunr initialized successfully");
			LOGGER.info("Dashboard: http://localhost:" + jobrunrPort);
			
			// Initialize and start retry scheduler for failed jobs
			this.retryScheduler = new JobRunrRetryScheduler(this);
			this.retryScheduler.start();
			LOGGER.info("JobRunr retry scheduler started");

		} catch (Exception e) {
			LOGGER.error("Failed to initialize JobRunr", e);
			throw new RuntimeException("JobRunr initialization failed", e);
		}
	}

	/**
	 * Get DataSource from SEMOSS configuration Reads database connection from
	 * scheduler.smss file (SEMOSS standard)
	 */
	private javax.sql.DataSource getDataSourceFromDIHelper() {
		try {
			String baseFolder = Utility.getDIHelperProperty(Constants.BASE_FOLDER);
			String schedulerSmssPath = baseFolder + "/db/scheduler.smss";
			File smssFile = new File(schedulerSmssPath);

			if (smssFile.exists()) {
				LOGGER.info("Loading scheduler.smss from: {}", schedulerSmssPath);
				Properties smssProp = Utility.loadProperties(schedulerSmssPath);

				if (smssProp != null) {
					String connectionUrl = smssProp.getProperty("CONNECTION_URL");
					String rdbmsType = smssProp.getProperty("RDBMS_TYPE");
					String username = smssProp.getProperty("USERNAME");
					String password = smssProp.getProperty("PASSWORD");
					String driverClass = smssProp.getProperty("DRIVER");

					if (connectionUrl != null) {
						// Replace @BaseFolder@ and @ENGINE@ placeholders
						connectionUrl = connectionUrl.replace("@BaseFolder@", baseFolder);
						connectionUrl = connectionUrl.replace("@ENGINE@", "scheduler");

						// Use driver from .smss or determine from RDBMS type
						if (driverClass == null || driverClass.trim().isEmpty()) {
							driverClass = determineDriverClass(rdbmsType);
						}

						return createDataSource(connectionUrl, driverClass, username, password);
					} else {
						LOGGER.error("CONNECTION_URL not found in scheduler.smss");
					}
				} else {
					LOGGER.error("Could not load properties from scheduler.smss");
				}
			} else {
				LOGGER.error("scheduler.smss not found at: {}", schedulerSmssPath);
			}

			LOGGER.warn("No DataSource configuration found for JobRunr");

		} catch (Exception e) {
			LOGGER.error("Error obtaining DataSource", e);
		}

		return null;
	}

	/**
	 * Determine JDBC driver class based on RDBMS type
	 */
	private String determineDriverClass(String rdbmsType) {
		if (rdbmsType == null) {
			return "org.h2.Driver"; // Default to H2
		}

		switch (rdbmsType.toUpperCase()) {
		case "H2_DB":
		case "H2":
			return "org.h2.Driver";
		case "MYSQL":
		case "MARIADB":
			return "com.mysql.cj.jdbc.Driver";
		case "SQLSERVER":
		case "MSSQL":
			return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		case "POSTGRESQL":
		case "POSTGRES":
			return "org.postgresql.Driver";
		case "ORACLE":
			return "oracle.jdbc.OracleDriver";
		default:
			LOGGER.warn("Unknown RDBMS type: {}, defaulting to H2", rdbmsType);
			return "org.h2.Driver";
		}
	}

	/**
	 * Create DataSource from configuration parameters
	 */
	private javax.sql.DataSource createDataSource(String jdbcUrl, String driver, String user, String password) {
		try {
			LOGGER.info("Creating DataSource with JDBC URL: {}", jdbcUrl);
			LOGGER.info("Driver: {}, Username: {}, Password: {}", driver, user,
					password != null && !password.isEmpty() ? "***" : "(empty)");

			// Load JDBC driver
			LOGGER.info("Loading JDBC driver: {}", driver);
			Class.forName(driver);
			LOGGER.info("JDBC driver loaded successfully");
			// Create HikariCP DataSource (if available) or basic DataSource
			try {
				LOGGER.info("Attempting to create HikariCP DataSource...");
				com.zaxxer.hikari.HikariDataSource ds = new com.zaxxer.hikari.HikariDataSource();
				ds.setJdbcUrl(jdbcUrl);
				ds.setUsername(user);
				ds.setPassword(password);
				ds.setMaximumPoolSize(getWorkerCount());
				ds.setMinimumIdle(2);
				ds.setConnectionTimeout(30000);

				// CRITICAL: Test the connection to ensure database is accessible
				LOGGER.info("Testing database connection...");
				java.sql.Connection testConn = ds.getConnection();
				if (testConn != null) {
					LOGGER.info(" Database connection successful!");
					LOGGER.info("  Database product: {} {}", testConn.getMetaData().getDatabaseProductName(),
							testConn.getMetaData().getDatabaseProductVersion());
					testConn.close();

					LOGGER.info(" HikariCP DataSource created and validated successfully");
					return ds;
				} else {
					LOGGER.error("Could not obtain database connection");
					ds.close();
					return null;
				}

			} catch (NoClassDefFoundError e) {
				// HikariCP not available, use basic DataSource
				LOGGER.warn("HikariCP not available ({}), using basic DataSource", e.getMessage());
				return createBasicDataSource(jdbcUrl, driver, user, password);
			}

		} catch (Exception e) {
			LOGGER.error("Failed to create DataSource", e);
			LOGGER.error("Error: {}", e.getMessage());
			return null;
		}
	}

	/**
	 * Create basic DataSource (fallback if HikariCP not available)
	 */
	private javax.sql.DataSource createBasicDataSource(String jdbcUrl, String driver, String user, String password) {
		try {
			javax.sql.DataSource ds;
			try {
				Class<?> dbcpClass = Class.forName("org.apache.commons.dbcp2.BasicDataSource");
				Object dbcpDs = dbcpClass.getDeclaredConstructor().newInstance();
				dbcpClass.getMethod("setDriverClassName").invoke(dbcpDs, driver);
				dbcpClass.getMethod("setUrl").invoke(dbcpDs, jdbcUrl);
				dbcpClass.getMethod("setUsername").invoke(dbcpDs, user);
				dbcpClass.getMethod("setPassword").invoke(dbcpDs, password);
				dbcpClass.getMethod("setMaxTotal").invoke(dbcpDs, 10);
				dbcpClass.getMethod("setMaxIdle").invoke(dbcpDs, 5);

				ds = (javax.sql.DataSource) dbcpDs;
				LOGGER.info("Created DBCP DataSource for JobRunr");
			} catch (ClassNotFoundException | NoClassDefFoundError e) {
				LOGGER.warn("DBCP not available, cannot create DataSource");
				return null;
			}

			return ds;

		} catch (Exception e) {
			LOGGER.error("Failed to create basic DataSource", e);
			return null;
		}
	}

	public String scheduleRecurring(String jobId, String cronExpression, String zoneId, JobRequest request) {
		validateEnabled();
		try {
			jobRequestScheduler.scheduleRecurrently(jobId, cronExpression, ZoneId.of(zoneId), request);

			LOGGER.info("Scheduled recurring job {} with cron {} (timezone: {})", jobId, cronExpression, zoneId);

			return jobId;

		} catch (Exception e) {
			LOGGER.error("Failed to schedule recurring job {}", jobId, e);
			throw new RuntimeException("Failed to schedule recurring job", e);
		}
	}
	
	public JobId enqueue(JobRunrPixelExecutionJobRequest jobRequest) {
		validateEnabled();
		try {
			JobId jobId = jobRequestScheduler.enqueue(jobRequest);

			LOGGER.info("Enqueued job for immediate execution: {}", jobId);
			return jobId;

		} catch (Exception e) {
			LOGGER.error("Failed to enqueue job", e);
			throw new RuntimeException("Failed to enqueue job: " + e.getMessage(), e);
		}
	}

	/**
	 * Delete a recurring job
	 * 
	 * @param jobId Job identifier to delete
	 */
	public void deleteRecurringJob(String jobId) {
		validateEnabled();

		try {
			storageProvider.deleteRecurringJob(jobId);
			LOGGER.info("Deleted recurring job: {}", jobId);
		} catch (Exception e) {
			LOGGER.error("Failed to delete recurring job: {}", jobId, e);
			throw new RuntimeException("Failed to delete recurring job: " + e.getMessage());
		}
	}

	/**
	 * Shutdown JobRunr service
	 * 
	 * @param deleteJobs Whether to delete all jobs on shutdown
	 */
	public void shutdown(boolean deleteJobs) {
		LOGGER.info("Shutting down JobRunr service...");

		// Stop retry scheduler first
		if (retryScheduler != null) {
			try {
				retryScheduler.stop();
				LOGGER.info("JobRunr retry scheduler stopped");
			} catch (Exception e) {
				LOGGER.error("Error stopping retry scheduler", e);
			}
		}

		if (storageProvider != null) {
			try {
				storageProvider.close();
				LOGGER.info("JobRunr storage provider closed");
			} catch (Exception e) {
				LOGGER.error("Error closing storage provider", e);
			}
		}

	}

	private void validateEnabled() {
		if (!enabled || jobRequestScheduler == null) {
			throw new IllegalStateException("JobRunr is not initialized or disabled");
		}
	}

	/**
	 * Pause a recurring job by deleting it from scheduler but keeping metadata in database
	 * The job can be resumed later by reading metadata from SMSS_JOB_RECIPES table
	 * 
	 * Enhanced with execution guard and metadata tracking
	 */
	public void pauseRecurringJob(String jobId) {
		validateEnabled();

		try {
			// Check if job exists
			RecurringJob recurringJob = null;

			for (RecurringJob job : storageProvider.getRecurringJobs()) {
			    if (jobId.equals(job.getId())) { 
			        recurringJob = job;
			        break;
			    }
			}

			if (recurringJob == null) {
			    throw new IllegalArgumentException("Recurring job not found: " + jobId);
			}

			// Delete from JobRunr scheduler (stops future executions)
			storageProvider.deleteRecurringJob(jobId);

			// Update status in SMSS_JOB_RECIPES table to PAUSED
			SchedulerDatabaseUtility.updateJobStatus(jobId, "PAUSED");
			
			SchedulerDatabaseUtility.updateJobRunningFlag(jobId, false);

			LOGGER.info("Paused recurring job: {} (deleted from scheduler, status updated in database)", jobId);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			LOGGER.error("Failed to pause recurring job: {}", jobId, e);
			throw new RuntimeException("Failed to pause recurring job: " + e.getMessage(), e);
		}
	}
	
	
	/**
	 * Resume a paused recurring job by reading metadata from database and rescheduling
	 */
	public void resumeRecurringJob(String jobId) {
		validateEnabled();

		try {
			// Get job metadata from SMSS_JOB_RECIPES table
			Map<String, String> jobData = SchedulerDatabaseUtility.getJobById(jobId);
			
			if (jobData == null) {
				throw new IllegalArgumentException("Job metadata not found in database: " + jobId);
			}

			String cronExpression = jobData.get("cronExpression");
			String cronTz = jobData.get("cronTz");
			String recipe = jobData.get("recipe");
			String recipeParameters = jobData.get("recipeParameters");
			String jobName = jobData.get("jobName");
			String jobGroup = jobData.get("jobGroup");
			String userAccess = jobData.get("userAccess");  // Retrieve stored user credentials
			if (cronExpression == null || recipe == null) {
				throw new IllegalArgumentException("Invalid job data for resume: " + jobId);
			}
			
			// Check if already scheduled
			boolean alreadyScheduled = storageProvider.getRecurringJobs().stream()
					.anyMatch(j -> j.getId().equals(jobId));
			
			if (alreadyScheduled) {
				LOGGER.warn("Job {} is already scheduled, skipping resume", jobId);
				return;
			}

			// Create job request with system access marker
			JobRunrPixelExecutionJobRequest jobRequest = new JobRunrPixelExecutionJobRequest(
					recipe,
					recipeParameters != null ? recipeParameters : "",
					userAccess,
					null,
					jobId,
					jobGroup,
					jobName
			);

			// Reschedule with original cron expression
			String zoneId = (cronTz != null && !cronTz.isEmpty()) ? 
					cronTz : Utility.getApplicationTimeZoneId();
			
			jobRequestScheduler.scheduleRecurrently(jobId, cronExpression, ZoneId.of(zoneId), jobRequest);

			// Update status in database to ACTIVE
			SchedulerDatabaseUtility.updateJobStatus(jobId, "ACTIVE");

			LOGGER.info("Resumed recurring job: {} (Cron: {}, Timezone: {})", jobId, cronExpression, zoneId);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			LOGGER.error("Failed to resume recurring job: {}", jobId, e);
			throw new RuntimeException("Failed to resume recurring job: " + e.getMessage(), e);
		}
	}

	/**
	 * Trigger a recurring job to execute immediately without affecting its schedule
	 * 
	 * Enhanced with execution guard to prevent duplicate execution
	 */
	public void triggerRecurringJobNow(String jobId) {
		validateEnabled();
		
		try {
			//Check if job is already running
			Boolean isRunning = SchedulerDatabaseUtility.isJobRunning(jobId);
			if (isRunning != null && isRunning) {
				LOGGER.warn("Job is already running, skipping trigger: {}", jobId);
				throw new IllegalStateException("Job is currently running: " + jobId);
			}
			
			// Mark job as running
			boolean marked = SchedulerDatabaseUtility.markJobAsRunning(jobId);
			if (!marked) {
				LOGGER.warn("Failed to mark job as running (race condition): {}", jobId);
				throw new IllegalStateException("Job is currently running or was modified: " + jobId);
			}
			
			try {
				// Check if job exists in scheduler
				boolean jobExists = storageProvider.getRecurringJobs().stream()
						.anyMatch(j -> j.getId().equals(jobId));
				
				if (!jobExists) {
					// Job might be paused, try to get from database
					Map<String, String> jobData = SchedulerDatabaseUtility.getJobById(jobId);
					
					if (jobData == null) {
						throw new IllegalArgumentException("Could not find recurring job with id = " + jobId);
					}

					String recipe = jobData.get("recipe");
					String recipeParameters = jobData.get("recipeParameters");
					String jobName = jobData.get("jobName");
					String jobGroup = jobData.get("jobGroup");
					String userAccess = jobData.get("userAccess");  // Retrieve stored user credentials

					if (recipe == null) {
						throw new IllegalArgumentException("Invalid job data for trigger: " + jobId);
					}
					// Create immediate execution request
					String execId = java.util.UUID.randomUUID().toString();
					JobRunrPixelExecutionJobRequest jobRequest = new JobRunrPixelExecutionJobRequest(
							recipe,
							recipeParameters != null ? recipeParameters : "",
							userAccess,
							execId,
							jobId,
							jobGroup,
							jobName
					);

					// Enqueue for immediate execution
					jobRequestScheduler.enqueue(jobRequest);
					
					LOGGER.info("Triggered paused job {} for immediate execution (execId: {})", jobId, execId);
				} else {
					// Job is active in scheduler, get its details and trigger
					RecurringJob recurringJob = storageProvider.getRecurringJobs().stream()
							.filter(j -> j.getId().equals(jobId))
							.findFirst()
							.orElseThrow(() -> new IllegalArgumentException("Recurring job not found: " + jobId));

					// Get job metadata from database
					Map<String, String> jobData = SchedulerDatabaseUtility.getJobById(jobId);
					
					if (jobData == null) {
						throw new IllegalArgumentException("Job metadata not found in database: " + jobId);
					}

					String recipe = jobData.get("recipe");
					String recipeParameters = jobData.get("recipeParameters");
					String jobName = jobData.get("jobName");
					String jobGroup = jobData.get("jobGroup");
					String userAccess = jobData.get("userAccess");  // Retrieve stored user credentials

					// Create immediate execution request
					String execId = java.util.UUID.randomUUID().toString();
					JobRunrPixelExecutionJobRequest jobRequest = new JobRunrPixelExecutionJobRequest(
							recipe,
							recipeParameters != null ? recipeParameters : "",
							userAccess,
							execId,
							jobId,
							jobGroup,
							jobName
					);

					jobRequestScheduler.enqueue(jobRequest);
					
					LOGGER.info("Triggered recurring job {} for immediate execution (execId: {})", jobId, execId);
				}
			} catch (Exception e) {
				SchedulerDatabaseUtility.updateJobRunningFlag(jobId, false);
				throw e;
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (IllegalStateException e) {
			throw e;
		} catch (Exception e) {
			LOGGER.error("Failed to trigger recurring job: {}", jobId, e);
			throw new RuntimeException("Failed to trigger recurring job: " + e.getMessage(), e);
		}
	}

	/**
	 * Record successful job execution
	 * Updates execution count, resets retry count, and clears error message
	 */
	public void recordJobSuccess(String jobId) {
		try {
			SchedulerDatabaseUtility.recordJobSuccess(jobId, Timestamp.from(Instant.now()));
			LOGGER.info("Recorded successful execution for job: {}", jobId);
		} catch (Exception e) {
			LOGGER.error("Failed to record job success: {}", jobId, e);
		}
	}
	
	/**
	 * Record failed job execution
	 * Updates retry count and stores error message
	 */
	public void recordJobFailure(String jobId, String errorMessage) {
		try {
			SchedulerDatabaseUtility.recordJobFailure(jobId, errorMessage, Timestamp.from(Instant.now()));
			LOGGER.warn("Recorded failed execution for job: {} - Error: {}", jobId, errorMessage);
		} catch (Exception e) {
			LOGGER.error("Failed to record job failure: {}", jobId, e);
		}
	}
	
	/**
	 * Get job metadata with enhanced tracking information
	 */
	public JobMetadata getJobMetadata(String jobId) {
		try {
			Map<String, String> jobData = SchedulerDatabaseUtility.getJobById(jobId);
			if (jobData == null) {
				return null;
			}
			
			JobMetadata metadata = new JobMetadata();
			metadata.setJobId(jobId);
			metadata.setJobName(jobData.get("jobName"));
			metadata.setJobGroup(jobData.get("jobGroup"));
			metadata.setCronExpression(jobData.get("cronExpression"));
			metadata.setTimezone(jobData.get("cronTz"));
			
			// Get enhanced metadata
			Map<String, Object> enhancedData = SchedulerDatabaseUtility.getJobMetadata(jobId);
			if (enhancedData != null) {
				if (enhancedData.containsKey("JOB_STATUS")) {
					String status = (String) enhancedData.get("JOB_STATUS");
					metadata.setStatus(status != null ? JobStatus.valueOf(status) : JobStatus.ACTIVE);
				}
				if (enhancedData.containsKey("IS_RUNNING")) {
					metadata.setRunning((Boolean) enhancedData.get("IS_RUNNING"));
				}
				if (enhancedData.containsKey("EXECUTION_COUNT")) {
					metadata.setExecutionCount((Long) enhancedData.get("EXECUTION_COUNT"));
				}
				if (enhancedData.containsKey("RETRY_COUNT")) {
					metadata.setRetryCount((Integer) enhancedData.get("RETRY_COUNT"));
				}
				if (enhancedData.containsKey("LAST_EXECUTION_STATUS")) {
					metadata.setLastExecutionStatus((String) enhancedData.get("LAST_EXECUTION_STATUS"));
				}
				if (enhancedData.containsKey("ERROR_MESSAGE")) {
					metadata.setErrorMessage((String) enhancedData.get("ERROR_MESSAGE"));
				}
			}
			
			return metadata;
		} catch (Exception e) {
			LOGGER.error("Failed to get job metadata: {}", jobId, e);
			return null;
		}
	}
	
	/**
	 * Check if a job is currently running
	 */
	public boolean isJobRunning(String jobId) {
		try {
			Boolean isRunning = SchedulerDatabaseUtility.isJobRunning(jobId);
			return isRunning != null && isRunning;
		} catch (Exception e) {
			LOGGER.error("Failed to check if job is running: {}", jobId, e);
			return false;
		}
	}
	
	/**
	 * Get jobs that failed and can be retried
	 * Returns jobs with FAILED status and retry count below max retries
	 */
	public List<Map<String, String>> getJobsNeedingRetry(int maxRetries, int limit) {
		try {
			return SchedulerDatabaseUtility.getJobsNeedingRetry(maxRetries, limit);
		} catch (Exception e) {
			LOGGER.error("Failed to get jobs needing retry", e);
			return new ArrayList<>();
		}
	}
	
	/**
	 * Retry a failed job
	 * Checks retry count and triggers execution if within limits
	 */
	public void retryFailedJob(String jobId) {
		validateEnabled();
		
		try {
			Map<String, Object> metadata = SchedulerDatabaseUtility.getJobMetadata(jobId);
			if (metadata == null) {
				throw new IllegalArgumentException("Job not found: " + jobId);
			}
			
			String lastStatus = (String) metadata.get("LAST_EXECUTION_STATUS");
			if (!"FAILED".equals(lastStatus)) {
				throw new IllegalStateException("Job has not failed: " + jobId + 
					" (Status: " + lastStatus + ")");
			}
			
			Integer retryCount = (Integer) metadata.get("RETRY_COUNT");
			int maxRetries = 3; // Default max retries
			if (metadata.containsKey("MAX_RETRIES") && metadata.get("MAX_RETRIES") != null) {
				maxRetries = (Integer) metadata.get("MAX_RETRIES");
			}
			
			if (retryCount != null && retryCount >= maxRetries) {
				throw new IllegalStateException("Job has exceeded max retries: " + jobId + 
					" (" + retryCount + "/" + maxRetries + ")");
			}
			
			// Trigger the job
			triggerRecurringJobNow(jobId);
			LOGGER.info("Retrying failed job: {} (Attempt {}/{})", 
				jobId, (retryCount != null ? retryCount + 1 : 1), maxRetries);
			
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (IllegalStateException e) {
			throw e;
		} catch (Exception e) {
			LOGGER.error("Failed to retry job: {}", jobId, e);
			throw new RuntimeException("Failed to retry job: " + e.getMessage(), e);
		}
	}
	
	/**
	 * Get job execution history
	 * Returns execution count, last execution time, and status
	 */
	public Map<String, Object> getJobExecutionHistory(String jobId) {
		try {
			return SchedulerDatabaseUtility.getJobMetadata(jobId);
		} catch (Exception e) {
			LOGGER.error("Failed to get job execution history: {}", jobId, e);
			return new HashMap<>();
		}
	}
	
	/**
	 * Get worker count configuration for JobRunr background job server
	 * Reads from JOBRUNR_WORKER_COUNT property, defaults to 10
	 * 
	 * @return Number of workers for processing jobs
	 */
	public int getWorkerCount() {
		String workerCountStr = Utility.getDIHelperProperty(Constants.JOBRUNR_WORKER_COUNT);
		int workerCount = 10; // default workers

		if (workerCountStr != null && !workerCountStr.trim().isEmpty()) {
			try {
				workerCount = Integer.parseInt(workerCountStr);
			} catch (NumberFormatException e) {
				LOGGER.warn("Invalid jobrunr_worker_count value: {}. Using default 10", workerCountStr);
			}
		}
		return workerCount;
	}

}
