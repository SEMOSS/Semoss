package prerna.util.jobrunr;

import java.io.File;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jobrunr.configuration.JobRunr;
import org.jobrunr.configuration.JobRunrConfiguration.JobRunrConfigurationResult;
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.scheduling.BackgroundJobRequest;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jobrunr.server.BackgroundJobServerConfiguration;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.storage.StorageProviderUtils.DatabaseOptions;
import org.jobrunr.storage.sql.h2.H2StorageProvider;

import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.rpa.jobrunr.jobs.PixelExecutionJobRequest;
import prerna.util.Constants;
import prerna.util.Utility;

public class JobRunrService {

	private static final Logger LOGGER = LogManager.getLogger(JobRunrService.class);
	private static final String JOBRUNR_ENABLED_FLAG = "scheduler_use_jobrunr";
	private static final String DEFAULT_ENABLED_VALUE = "false";
	private static JobRunrService instance = null;
	private JobRequestScheduler jobRequestScheduler;
	private StorageProvider storageProvider;
	private boolean enabled;

	private JobRunrService() {
		this.enabled = isEnabledFromConfig();
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

	/**
	 * Check if JobRunr is enabled via feature flag
	 */
	private boolean isEnabledFromConfig() {
		try {
			String enabled = Utility.getDIHelperProperty("scheduler_use_jobrunr");
			if (enabled == null) {
				enabled = DEFAULT_ENABLED_VALUE; // Default to false
			}
			return Boolean.parseBoolean(enabled);
		} catch (Exception e) {
			LOGGER.debug("Could not read scheduler_use_jobrunr from DIHelper, defaulting to false");
			return false;
		}
	}

	public static boolean isJobRunrEnabled() {
		try {
			String flag = Utility.getDIHelperProperty(JOBRUNR_ENABLED_FLAG);
			if (flag == null) {
				flag = DEFAULT_ENABLED_VALUE; // Default to false for safety
			}
			return Boolean.parseBoolean(flag);
		} catch (Exception e) {
			// Log warning but don't fail
			LOGGER.warn("Could not read " + JOBRUNR_ENABLED_FLAG + ", defaulting to false");
			return false;
		}
	}

	public static JobRunrService getJobRunrService() {
		if (!isJobRunrEnabled()) {
			throw new IllegalStateException("JobRunr is disabled. Set " + JOBRUNR_ENABLED_FLAG + "=true to enable.");
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

			String jobrunrPortStr = Utility.getDIHelperProperty("jobrunr_dashboard_port");
			int jobrunrPort = 8000; // default port

			if (jobrunrPortStr != null && !jobrunrPortStr.trim().isEmpty()) {
				try {
					jobrunrPort = Integer.parseInt(jobrunrPortStr);
				} catch (NumberFormatException e) {
					LOGGER.warn("Invalid jobrunr_dashboard_port value: {}. Using default 8000", jobrunrPortStr);
				}
			}

			this.storageProvider = new H2StorageProvider(dataSource, DatabaseOptions.CREATE);

			JobRunrConfigurationResult config = JobRunr.configure().useStorageProvider(storageProvider)
					.useBackgroundJobServer(BackgroundJobServerConfiguration
							.usingStandardBackgroundJobServerConfiguration().andWorkerCount(10))
					.useDashboard(jobrunrPort).initialize();

			// Get instances
			this.jobRequestScheduler = config.getJobRequestScheduler();
			LOGGER.info("JobRunr initialized successfully");
			LOGGER.info("Dashboard: http://localhost:" + jobrunrPort);

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
			// This follows the same pattern as SMSSWebWatcher.loadNewEngine()
			String baseFolder = Utility.getDIHelperProperty(Constants.BASE_FOLDER);

			// Fallback to current working directory if BASE_FOLDER not set
			if (baseFolder == null || baseFolder.trim().isEmpty()) {
				baseFolder = System.getProperty("user.dir");
				LOGGER.warn("BASE_FOLDER not set in DIHelper, using current directory: {}", baseFolder);
			}

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

						LOGGER.info("Using scheduler.smss configuration - URL: {}, Driver: {}", connectionUrl,
								driverClass);
						LOGGER.info("Database username: {}, password: {}", username,
								password != null && !password.isEmpty() ? "***" : "(empty)");
						return createDataSource(connectionUrl, driverClass, username, password);
					} else {
						LOGGER.error("CONNECTION_URL not found in scheduler.smss");
					}
				} else {
					LOGGER.error("Could not load properties from scheduler.smss");
				}
			} else {
				LOGGER.error("scheduler.smss not found at: {}", schedulerSmssPath);
				LOGGER.error("Please ensure the file exists at this location");
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
				ds.setMaximumPoolSize(10);
				ds.setMinimumIdle(2);
				ds.setConnectionTimeout(30000);

				// CRITICAL: Test the connection to ensure database is accessible
				LOGGER.info("Testing database connection...");
				java.sql.Connection testConn = ds.getConnection();
				if (testConn != null) {
					LOGGER.info(" Database connection successful!");
					LOGGER.info("  Connection URL: {}", jdbcUrl);
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
			LOGGER.error("JDBC URL: {}", jdbcUrl);
			LOGGER.error("Driver: {}", driver);
			LOGGER.error("Username: {}", user);
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
	
	public JobId enqueue(PixelExecutionJobRequest jobRequest) {
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
			// Use storage provider directly to delete recurring job
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
	 */
	public void pauseRecurringJob(String jobId) {
		validateEnabled();

		try {
			// Check if job exists
			RecurringJob recurringJob = null;

			for (RecurringJob job : storageProvider.getRecurringJobs()) {
			    if (jobId.equals(job.getId())) { // safer (avoids NPE)
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
			PixelExecutionJobRequest jobRequest = new PixelExecutionJobRequest(
					recipe,
					recipeParameters != null ? recipeParameters : "",
					"SYSTEM:RESUMED",
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
	 */
	public void triggerRecurringJobNow(String jobId) {
		validateEnabled();
		
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

				if (recipe == null) {
					throw new IllegalArgumentException("Invalid job data for trigger: " + jobId);
				}

				// Create immediate execution request
				String execId = java.util.UUID.randomUUID().toString();
				PixelExecutionJobRequest jobRequest = new PixelExecutionJobRequest(
						recipe,
						recipeParameters != null ? recipeParameters : "",
						"SYSTEM:TRIGGERED",
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

				// Create immediate execution request
				String execId = java.util.UUID.randomUUID().toString();
				PixelExecutionJobRequest jobRequest = new PixelExecutionJobRequest(
						recipe,
						recipeParameters != null ? recipeParameters : "",
						"SYSTEM:TRIGGERED",
						execId,
						jobId,
						jobGroup,
						jobName
				);

				// Enqueue for immediate execution (doesn't affect recurring schedule)
				jobRequestScheduler.enqueue(jobRequest);
				
				LOGGER.info("Triggered recurring job {} for immediate execution (execId: {})", jobId, execId);
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			LOGGER.error("Failed to trigger recurring job: {}", jobId, e);
			throw new RuntimeException("Failed to trigger recurring job: " + e.getMessage(), e);
		}
	}

	private boolean recurringJobExists(String jobId) {
		return storageProvider.getRecurringJobs().stream().anyMatch(job -> job.getId().equals(jobId));
	}

}
