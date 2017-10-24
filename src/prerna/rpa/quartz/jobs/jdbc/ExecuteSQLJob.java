package prerna.rpa.quartz.jobs.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import prerna.rpa.quartz.CommonDataKeys;
import prerna.rpa.quartz.QuartzUtility;

public class ExecuteSQLJob implements org.quartz.InterruptableJob {
	
	private static final Logger LOGGER = LogManager.getLogger(ExecuteSQLJob.class.getName());

	// Connection details
	// Since the package is JDBC, it is implied for these keys
	/** {@code String} */
	public static final String DRIVER_KEY = CommonDataKeys.JDBC_DRIVER;

	/** {@code String} */
	public static final String CONNECTION_URL_KEY = CommonDataKeys.JDBC_CONNECTION_URL;

	/** {@code String} */
	public static final String USERNAME_KEY = CommonDataKeys.JDBC_USERNAME;

	/** {@code String} */
	public static final String PASSWORD_KEY = CommonDataKeys.JDBC_PASSWORD;

	/** {@code String} */
	public static final String SQL_KEY = ExecuteSQLJob.class + ".SQL";
	
	// Closable objects
	private Connection connection;
	private Statement statement;
	
	private String jobName;
	private String terminationMessage;

	private String connectionURL;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		terminationMessage = "Will terminate the " + jobName + " job.";
		
		String driver = dataMap.getString(DRIVER_KEY);
		connectionURL = dataMap.getString(CONNECTION_URL_KEY);
		String username = dataMap.getString(USERNAME_KEY);
		String password = dataMap.getString(PASSWORD_KEY);
		String sql = dataMap.getString(SQL_KEY);
		
		////////////////////
		// Do work
		////////////////////
		JDBCUtil.loadDriver(driver);
		
		// Execute the sql		
		long startTime = System.currentTimeMillis();
		try {
			connection = DriverManager.getConnection(connectionURL, username, password);
			statement = connection.createStatement();
			LOGGER.info(jobName + ": " + "Running the following SQL statement:\n" + sql);
			for (String individualSQL : sql.split(";")) {
				individualSQL = individualSQL.trim();
				if (individualSQL.length() > 0) {
					LOGGER.info(jobName + ": " + "Running the following individual SQL statement:\n" + StringUtils.abbreviate(individualSQL, 1000));
					statement.execute(individualSQL);
				}
			}
			LOGGER.info(jobName + ": " + "Completed executing the SQL statement.");
			LOGGER.info(jobName + ": " + "Elapsed time " + QuartzUtility.secondsSinceStartTime(startTime) + " seconds.");
		} catch (SQLException e) {
			String executeSqlExceptionMessage = "A SQL exception occured while executing the SQL. ";
			LOGGER.error(jobName + ": " + executeSqlExceptionMessage + terminationMessage);
			throw new JobExecutionException(executeSqlExceptionMessage, e);
		} finally {
			closeConnections();
		}
		
		////////////////////
		// Store outputs
		////////////////////
		// No outputs to store here
	}
	
	private void closeConnections() {
		try {
			if (statement != null) statement.close();
			if (connection != null) connection.close();
			LOGGER.info(jobName + ": " + "Closed all connections to " + connectionURL + ".");
		} catch (SQLException e) {
			LOGGER.error(jobName + ": " + "Failed to close all connections. ", e);
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn(jobName + ": " + "The " + jobName + " job was interrupted. Will attempt to close all connections and terminate the job.");
		closeConnections();
	}
	
}
