package prerna.util.jobrunr;

import java.io.File;
import java.time.ZoneId;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jobrunr.configuration.JobRunr;
import org.jobrunr.configuration.JobRunrConfiguration.JobRunrConfigurationResult;
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.scheduling.BackgroundJob;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.storage.StorageProviderUtils.DatabaseOptions;
import org.jobrunr.storage.sql.h2.H2StorageProvider;

import prerna.rpa.jobrunr.jobs.PixelExecutionJobRequest;
import prerna.util.Constants;
import prerna.util.Utility;

public class JobRunrService {

	private static final Logger LOGGER = LogManager.getLogger(JobRunrService.class);

	private static JobRunrService instance = null;
	private JobRequestScheduler jobRequestScheduler;
	private final StorageProvider storageProvider;
	private final boolean enabled;

	private JobRunrService() {
		this.enabled = isEnabledFromConfig();
		this.storageProvider = initializeStorageProvider();
		LOGGER.info("JobRunr Service initialized{}", enabled ? "" : " (disabled)");
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
				enabled = "false"; // Default to false
			}
			return Boolean.parseBoolean(enabled);
		} catch (Exception e) {
			LOGGER.debug("Could not read scheduler_use_jobrunr from DIHelper, defaulting to false");
			return false;
		}
	}

	/**
	 * Initialize storage provider using SEMOSS database configuration
	 */
	private StorageProvider initializeStorageProvider() {
		if (!enabled) {
			LOGGER.info("JobRunr is disabled via scheduler_use_jobrunr=false");
			return null;
		}

		try {
			LOGGER.info("Initializing JobRunr storage provider...");

			// This allows JobRunr to connect to the same H2 database
//			try {
//				LOGGER.info("Checking if Quartz scheduler is running...");
//				org.quartz.Scheduler quartzScheduler = org.quartz.impl.StdSchedulerFactory.getDefaultScheduler();
//				if (quartzScheduler != null && !quartzScheduler.isShutdown()) {
//					LOGGER.info("Shutting down Quartz scheduler to release database lock...");
//					quartzScheduler.shutdown(false);
//					LOGGER.info(" Quartz scheduler shut down successfully");
//				} else {
//					LOGGER.info("Quartz scheduler is not running or already shutdown");
//				}
//			} catch (Exception e) {
//				LOGGER.warn("Could not shutdown Quartz scheduler (may not be initialized): {}", e.getMessage());
//			}

			// Get DataSource from SEMOSS configuration
			// Uses same database as Quartz for consistency
			javax.sql.DataSource dataSource = getDataSourceFromDIHelper();

			if (dataSource == null) {
				LOGGER.error("Could not obtain DataSource from scheduler.smss");
				LOGGER.error("Please check db/scheduler.smss file exists and has valid configuration");
				throw new RuntimeException("Cannot obtain DataSource for JobRunr");
			}

			LOGGER.info(" DataSource obtained successfully");

			StorageProvider provider = new H2StorageProvider(dataSource, DatabaseOptions.CREATE);

			String jobrunrPortStr = Utility.getDIHelperProperty("jobrunr_dashboard_port");
			int jobrunrPort = 8000; // default port

			if (jobrunrPortStr != null && !jobrunrPortStr.trim().isEmpty()) {
				try {
					jobrunrPort = Integer.parseInt(jobrunrPortStr);
				} catch (NumberFormatException e) {
					LOGGER.warn("Invalid jobrunr_dashboard_port value: {}. Using default 8000", jobrunrPortStr);
				}
			}

			// Initialize JobRunr using Fluent API
			JobRunrConfigurationResult jobRunrConfiguration = JobRunr.configure()
					.useStorageProvider(new H2StorageProvider(dataSource, DatabaseOptions.CREATE))
					.useBackgroundJobServer().useDashboard(jobrunrPort).initialize();

			// Get instances
			this.jobRequestScheduler = jobRunrConfiguration.getJobRequestScheduler();
			LOGGER.info("JobRunr initialized successfully");

			LOGGER.info(" JobRunr SQL storage provider initialized successfully");
			LOGGER.info("Tables will be auto-created when first job is scheduled");
			return provider;

		} catch (Exception e) {
			LOGGER.error(" Failed to initialize JobRunr storage provider", e);
			LOGGER.error("Error type: {}", e.getClass().getName());
			LOGGER.error("Error details: {}", e.getMessage());
			if (e.getCause() != null) {
				LOGGER.error("Root cause: {}", e.getCause().getMessage());
			}
			throw new RuntimeException("Failed to initialize JobRunr storage provider", e);
		}
	}

	/**
	 * Create JobRunr tables manually (for non-Spring environments)
	 */
//	private void createJobRunrTablesManually(javax.sql.DataSource dataSource) {
//		java.sql.Connection conn = null;
//		try {
//			conn = dataSource.getConnection();
//
//			// Check if tables already exist
//			java.sql.DatabaseMetaData meta = conn.getMetaData();
//			java.sql.ResultSet tables = meta.getTables(null, null, "JOBRUNR_%", new String[] { "TABLE" });
//
//			boolean hasTables = false;
//			while (tables.next()) {
//				hasTables = true;
//				break;
//			}
//			tables.close();
//
//			if (hasTables) {
//				LOGGER.info("JobRunr tables already exist");
//				return;
//			}
//
//			// Create tables using JobRunr's DDL statements
//			LOGGER.info("Creating JobRunr tables...");
//
//			try (java.sql.Statement stmt = conn.createStatement()) {
//				// Create JOBRUNR_JOB table
//				stmt.execute("CREATE TABLE IF NOT EXISTS JOBRUNR_JOB (" + "  id VARCHAR(255) PRIMARY KEY, "
//						+ "  status VARCHAR(50) NOT NULL, " + "  scheduledAt TIMESTAMP NOT NULL, "
//						+ "  startedAt TIMESTAMP, " + "  succeededAt TIMESTAMP, " + "  failedAt TIMESTAMP, "
//						+ "  deletedAt TIMESTAMP, " + "  processingServer VARCHAR(255), "
//						+ "  createdAt TIMESTAMP NOT NULL, " + "  updatedAt TIMESTAMP NOT NULL, "
//						+ "  version BIGINT NOT NULL, " + "  jobDetails CLOB NOT NULL, " + "  metadata CLOB" + ")");
//
//				// Create JOBRUNR_RECURRINGJOB table
//				stmt.execute("CREATE TABLE IF NOT EXISTS JOBRUNR_RECURRINGJOB (" + "  id VARCHAR(255) PRIMARY KEY, "
//						+ "  cronExpression VARCHAR(100) NOT NULL, " + "  timeZoneId VARCHAR(100), "
//						+ "  jobDetails CLOB NOT NULL, " + "  createdAt TIMESTAMP NOT NULL, "
//						+ "  updatedAt TIMESTAMP NOT NULL, " + "  version BIGINT NOT NULL, "
//						+ "  paused BOOLEAN DEFAULT FALSE, " + "  metadata CLOB" + ")");
//
//				// Create JOBRUNR_JOBVERSION table
//				stmt.execute("CREATE TABLE IF NOT EXISTS JOBRUNR_JOBVERSION ("
//						+ "  workerName VARCHAR(255) PRIMARY KEY, " + "  version BIGINT NOT NULL" + ")");
//
//				// Insert initial version record
//				stmt.execute("INSERT INTO JOBRUNR_JOBVERSION (workerName, version) VALUES ('default', 0)");
//			}
//
//			conn.commit();
//			LOGGER.info(" JobRunr tables created successfully");
//
//		} catch (Exception e) {
//			LOGGER.error("Error creating JobRunr tables", e);
//			throw new RuntimeException("Failed to create JobRunr tables", e);
//		} finally {
//			if (conn != null) {
//				try {
//					conn.close();
//				} catch (Exception e) {
//					/* ignore */ }
//			}
//		}
//	}

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

		if (!enabled) {
			throw new IllegalStateException("JobRunr not enabled");
		}

		if (jobRequestScheduler == null) {
			throw new IllegalStateException("JobRunr not initialized - JobRequestScheduler is null");
		}

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
		if (!enabled || storageProvider == null) {
			throw new IllegalStateException("JobRunr is not enabled or not properly configured");
		}

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
		if (!enabled) {
			LOGGER.warn("JobRunr is not enabled, skipping job deletion: {}", jobId);
			return;
		}

		if (storageProvider == null) {
			LOGGER.warn("StorageProvider not initialized, cannot delete job: {}", jobId);
			return;
		}

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

	/**
	 * Pause a recurring job
	 * 
	 * @param jobId Job identifier to pause
	 */
	public void pauseRecurringJob(String jobId) {

		if (!enabled) {
			LOGGER.warn("JobRunr disabled. Cannot pause {}", jobId);
			return;
		}

		try {
			BackgroundJob.delete(jobId);

			LOGGER.info("Paused recurring job {}", jobId);

		} catch (Exception e) {
			LOGGER.error("Failed to pause recurring job {}", jobId, e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Resume a paused recurring job
	 * 
	 * @param jobId Job identifier to resume
	 */
	public String resumeRecurringJob(String jobId, String cronExpression, JobRequest request) {

		if (!enabled) {
			throw new IllegalStateException("JobRunr not enabled");
		}

		try {

			String id = BackgroundJob.scheduleRecurrently(jobId, cronExpression, JobRunrAdapter.from(request));

			LOGGER.info("Resumed recurring job: {}", jobId);

			return id;

		} catch (Exception e) {
			LOGGER.error("Failed to resume recurring job {}", jobId, e);
			throw new RuntimeException(e);
		}
	}

}
