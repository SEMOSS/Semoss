package prerna.rpa.quartz.jobs.db.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.rpa.RPAUtil;
import prerna.rpa.db.jdbc.JDBCUtil;
import prerna.rpa.quartz.CommonDataKeys;
import prerna.util.Constants;

public class ExecuteSQLJob implements org.quartz.InterruptableJob {
	
	private static final Logger LOGGER = LogManager.getLogger(ExecuteSQLJob.class.getName());
	
	private static final String NEW_LINE = System.lineSeparator();

	// Connection details
	// Since the package is JDBC, it is implied for these keys
	/** {@code String} */
	public static final String IN_DRIVER_KEY = CommonDataKeys.JDBC_DRIVER;

	/** {@code String} */
	public static final String IN_CONNECTION_URL_KEY = CommonDataKeys.JDBC_CONNECTION_URL;

	/** {@code String} */
	public static final String IN_USERNAME_KEY = CommonDataKeys.JDBC_USERNAME;

	/** {@code String} */
	public static final String IN_PASSWORD_KEY = CommonDataKeys.JDBC_PASSWORD;

	/** {@code String} */
	public static final String IN_SQL_KEY = ExecuteSQLJob.class + ".SQL";
	
	// Closable objects
	private Connection connection;
	private Statement statement;
	
	private String jobName;
	
	private String connectionURL;

	private volatile boolean closed = false;
	private volatile boolean interrupted = false;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		String terminationMessage = "Will terminate the " + jobName + " job.";
		
		String driver = dataMap.getString(IN_DRIVER_KEY);
		connectionURL = dataMap.getString(IN_CONNECTION_URL_KEY);
		String username = dataMap.getString(IN_USERNAME_KEY);
		String password = dataMap.getString(IN_PASSWORD_KEY);
		String sql = dataMap.getString(IN_SQL_KEY);
		
		////////////////////
		// Do work
		////////////////////
		JDBCUtil.loadDriver(driver);
		
		// Execute the sql		
		long startTime = System.currentTimeMillis();
		try {
			connection = DriverManager.getConnection(connectionURL, username, password);
			statement = connection.createStatement();
			LOGGER.info(jobName + ": " + "Running the following SQL statement:" + NEW_LINE + sql);
			for (String individualSQL : sql.split(";")) {
				individualSQL = individualSQL.trim();
				if (individualSQL.length() > 0) {
					LOGGER.info(jobName + ": " + "Running the following individual SQL statement:" + NEW_LINE + StringUtils.abbreviate(individualSQL, 1000));
					statement.execute(individualSQL);
				}
			}
			LOGGER.info(jobName + ": " + "Completed executing the SQL statement.");
			LOGGER.info(jobName + ": " + "Elapsed time " + RPAUtil.secondsSinceStartTime(startTime) + " seconds.");
		} catch (SQLException e) {
			
			// Unless the exception occurred due to an interruption, raise it again
			if (!interrupted) {
				String executeSqlExceptionMessage = "A SQL exception occurred while executing the SQL. ";
				LOGGER.error(jobName + ": " + executeSqlExceptionMessage + terminationMessage);
				LOGGER.error(Constants.STACKTRACE, e);
				throw new JobExecutionException(executeSqlExceptionMessage);
			}
		} finally {
			closeConnections();
		}
		
		////////////////////
		// Store outputs
		////////////////////
		// No outputs to store here
	}
	
	private void closeConnections() {
		if (!closed) {
			closed = true;
			try {
				if (statement != null) statement.close();
				if (connection != null) connection.close();
				LOGGER.info(jobName + ": " + "Closed all connections to " + connectionURL + ".");
			} catch (SQLException e) {
				LOGGER.error(Constants.STACKTRACE, e);
				LOGGER.error(jobName + ": " + "Failed to close all connections. ");
			}	
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		interrupted = true;
		LOGGER.warn(jobName + ": " + "The " + jobName + " job was interrupted. Will attempt to close all connections and terminate the job.");
		closeConnections();
	}
	
}
