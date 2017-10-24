package prerna.rpa.quartz.jobs.jdbc.maria;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import prerna.rpa.quartz.QuartzUtility;
import prerna.rpa.quartz.jobs.jdbc.JDBCUtil;
import prerna.rpa.quartz.jobs.jdbc.maria.ETLJob;

public class ETLJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(ETLJob.class.getName());

	// Inputs
	// From
	/** {@code String} */
	public static final String IN_FROM_DRIVER_KEY = ETLJob.class + ".fromDriver";

	/** {@code String} */
	public static final String IN_FROM_CONNECTION_URL_KEY = ETLJob.class + ".fromConnectionURL";

	/** {@code String} */
	public static final String IN_FROM_USERNAME_KEY = ETLJob.class + ".fromUsername";

	/** {@code String} */
	public static final String IN_FROM_PASSWORD_KEY = ETLJob.class + ".fromPassword";

	/**
	 * {@code String} - Optional SQL string to be executed (using
	 * {@link java.sql.Statement#execute(String sql)} not
	 * {@link java.sql.Statement#executeQuery(String)}) before the query is run.
	 * This is useful when you need to execute SQLs like CREATE INDEX that cannot be
	 * run within {@code executeQuery} before grabbing your results.
	 */
	public static final String IN_FROM_SQL_EXECUTE_KEY = ETLJob.class + ".fromSQLExecute"; // optional execute

	/** {@code String} */
	public static final String IN_FROM_SQL_QUERY_KEY = ETLJob.class + ".fromSQLQuery";

	// To
	/** {@code String} */
	public static final String IN_TO_DRIVER_KEY = ETLJob.class + ".toDriver";

	/** {@code String} */
	public static final String IN_TO_CONNECTION_URL_KEY = ETLJob.class + ".toConnectionURL";

	/** {@code String} */
	public static final String IN_TO_USERNAME_KEY = ETLJob.class + ".toUsername";

	/** {@code String} */
	public static final String IN_TO_PASSWORD_KEY = ETLJob.class + ".toPassword";

	/** {@code String} */
	public static final String IN_TO_TABLE_NAME_KEY = ETLJob.class + ".toTableName";

	// No outputs

	// Closable objects
	private Connection fromConnection;
	private Statement fromStatement;
	private ResultSet fromResults;
	private Connection toConnection;
	private Statement toStatement;
	private PreparedStatement toPreparedStatement;
	private ResultSet toTables;

	private String jobName;
	private String terminationMessage;

	private String fromConnectionURL;
	private String toConnectionURL;
	
	private static final int BATCH_SIZE = 10000;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		// From
		String fromDriver = dataMap.getString(IN_FROM_DRIVER_KEY);
		fromConnectionURL = dataMap.getString(IN_FROM_CONNECTION_URL_KEY);
		String fromUsername = dataMap.getString(IN_FROM_USERNAME_KEY);
		String fromPassword = dataMap.getString(IN_FROM_PASSWORD_KEY);
		
		// Since this is optional, if the dataMap doesn't assign a key set as an empty string
		String fromSQLExecute = dataMap.containsKey(IN_FROM_SQL_EXECUTE_KEY) ? dataMap.getString(IN_FROM_SQL_EXECUTE_KEY) : "";
		String fromSQLQuery = dataMap.getString(IN_FROM_SQL_QUERY_KEY);

		// To
		// This job will only work for MariaDB connections
		String toDriver = dataMap.getString(IN_TO_DRIVER_KEY);
		if (!toDriver.contains("mariadb")) {
			String notMariaDBMessage = "This job only works for an ETL into a MariaDB connection. ";
			LOGGER.error(jobName + ": " + notMariaDBMessage + terminationMessage);
			throw new JobExecutionException(notMariaDBMessage);
		}
		toConnectionURL = dataMap.getString(IN_TO_CONNECTION_URL_KEY);
		String toUsername = dataMap.getString(IN_TO_USERNAME_KEY);
		String toPassword = dataMap.getString(IN_TO_PASSWORD_KEY);
		String toTableName = dataMap.getString(IN_TO_TABLE_NAME_KEY);

		// Message to display before terminating this job
		terminationMessage = "Will terminate the " + jobName + " job.";

		////////////////////
		// Do work
		////////////////////
		JDBCUtil.loadDriver(fromDriver);
		JDBCUtil.loadDriver(toDriver);

		// Copy SQL query results into the to-database
		// First connect to the from-database
		// Keeps the exception handling divided into exceptions caused by the from-database and those caused by the to-database
		long startTime = System.currentTimeMillis();
		try {
			fromConnection = DriverManager.getConnection(fromConnectionURL, fromUsername, fromPassword);
			fromStatement = fromConnection.createStatement();
			
			// If there is an execute SQL to run, then execute it
			if (fromSQLExecute.length() > 0) {
				LOGGER.info(jobName + ": " + "Running the following SQL statement:\n" + StringUtils.abbreviate(fromSQLExecute, 1000));
				
				// execute
				fromStatement.execute(fromSQLExecute);
				LOGGER.info(jobName + ": " + "Succesfully executed the SQL statement. ");
			}
			
			// Grab results using the from SQL query
			LOGGER.info(jobName + ": " + "Running the following SQL statement:\n" + StringUtils.abbreviate(fromSQLQuery, 1000));

			// query
			fromResults = fromStatement.executeQuery(fromSQLQuery);
			LOGGER.info(jobName + ": " + "Successfully retrieved results from the SQL query. ");


			// Now connect to the to-database
			try {
				toConnection = DriverManager.getConnection(toConnectionURL, toUsername, toPassword);
				toStatement = toConnection.createStatement();

				// Check whether the to table exists
				DatabaseMetaData toMetaData = toConnection.getMetaData();
				toTables = toMetaData.getTables(null, null, toTableName, null);
				boolean tableExists = toTables.next();

				// If the to table exists, then drop
				if (tableExists) {
					toStatement.execute("DROP TABLE " + toTableName + ";");
					LOGGER.info(jobName + ": " + "Dropped the table " + toTableName + ".");
				} 
				
				// Create the table
				String toTableCreateSQL = JDBCUtil.generateCreateTableSQL(fromResults, toTableName);
				LOGGER.info(jobName + ": " + "Creating the table " + toTableName + " using the following SQL:\n" + toTableCreateSQL);
				toStatement.execute(toTableCreateSQL);
				LOGGER.info(jobName + ": " + "Created the table " + toTableName + ".");

				// Get the number of columns
				int nCol = fromResults.getMetaData().getColumnCount();

				// Insert in batches
				toPreparedStatement = toConnection.prepareStatement(JDBCUtil.generateInsertSQL(fromResults, toTableName));
				int batchCount = 0;
				int nRecord = 0;
				LOGGER.info(jobName + ": " + "Inserting data into the table " + toTableName + ".");
				toConnection.setAutoCommit(false);
				while (fromResults.next()) {
					for (int c = 1; c <= nCol; c++) {
						toPreparedStatement.setObject(c, fromResults.getObject(c));
					}
					toPreparedStatement.addBatch();
					batchCount++;
					nRecord++;
					if (batchCount % BATCH_SIZE == 0) {
						toPreparedStatement.executeBatch();
						toConnection.commit();
						LOGGER.info(jobName + ": " + "Total records inserted into the table " + toTableName + " thus far: " + nRecord);
						batchCount = 0;
					}
				}

				// Execute the last batch
				toPreparedStatement.executeBatch();
				toConnection.commit();
				LOGGER.info(jobName + ": " + "Inserted data into the table " + toTableName + ". Total number of records: " + nRecord);
				LOGGER.info(jobName + ": " + "Completed the ETL routine for the " + jobName + " job.");
				LOGGER.info(jobName + ": " + "Elapsed time " + QuartzUtility.minutesSinceStartTime(startTime) + " minutes.");
			} catch (SQLException e) {
				
				// Roll back in the case of an error (auto-commit disabled)
				toConnection.rollback();
				String toDatabaseExceptionMessage = "A SQL exception occured while refreshing the to-database. ";
				LOGGER.error(jobName + ": " + toDatabaseExceptionMessage + terminationMessage);
				throw new JobExecutionException(toDatabaseExceptionMessage, e);
			} finally {
				closeToConnections();
			}

		} catch (SQLException e) {
			String fromDatabaseExceptionMessage = "A SQL exception occured while querying the from-database. ";
			LOGGER.error(jobName + ": " + fromDatabaseExceptionMessage + terminationMessage);
			throw new JobExecutionException(fromDatabaseExceptionMessage, e);
		} finally {
			closeFromConnections();
		}

		////////////////////
		// Store outputs
		////////////////////
		// No outputs to store here
	}

	private void closeFromConnections() {
		try {
			if (fromResults != null) fromResults.close();
			if (fromStatement != null) fromStatement.close();
			if (fromConnection != null) fromConnection.close();
			LOGGER.info(jobName + ": " + "Closed all connections to " + fromConnectionURL + ".");
		} catch (SQLException e) {
			LOGGER.error(jobName + ": " + "Failed to close all connections. ", e);
		}
	}

	private void closeToConnections() {
		try {
			if (toPreparedStatement != null) toPreparedStatement.close();
			if (toTables != null) toTables.close();
			if (toStatement != null) toStatement.close();
			if (toConnection != null) toConnection.close();
			LOGGER.info(jobName + ": " + "Closed all connections to " + toConnectionURL + ".");
		} catch (SQLException e) {
			LOGGER.error(jobName + ": " + "Failed to close all connections. ", e);
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn(jobName + ": " + "The " + jobName + " job was interrupted. Will attempt to close all connections and terminate the job.");
		closeToConnections();
		closeFromConnections();
	}

}
