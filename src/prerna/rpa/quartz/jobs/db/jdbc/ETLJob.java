package prerna.rpa.quartz.jobs.db.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import prerna.util.Constants;

public class ETLJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(ETLJob.class.getName());
	
	private static final String NEW_LINE = System.lineSeparator();

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
	
	private static final int BATCH_SIZE = 1024;
	private static final int MOD_SIZE = 100000;

	// Closable objects
	private Connection fromConnection;
	private Statement fromStatement;
	private ResultSet fromResults;
	private Connection toConnection;
	private Statement toStatement;
	private ResultSet toTables;
	private PreparedStatement toPreparedStatement;

	private String jobName;
	private String terminationMessage;

	private String fromConnectionURL;
	private String toConnectionURL;
	
	private volatile boolean fromClosed = false;
	private volatile boolean toClosed = false;
	private volatile boolean interrupted = false;
	
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
		String toDriver = dataMap.getString(IN_TO_DRIVER_KEY);
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

		// Extract then load
		long startTime = System.currentTimeMillis();

		// Extract (from-database)
		try {
			extract(fromUsername, fromPassword, fromSQLExecute, fromSQLQuery);
			
			// Load (to-database)
			load(toUsername, toPassword, toTableName);
			LOGGER.info(jobName + ": " + "Completed the ETL routine for the " + jobName + " job.");
			LOGGER.info(jobName + ": " + "Elapsed time " + RPAUtil.minutesSinceStartTime(startTime) + " minutes.");
		} catch (SQLException e) {
			
			// Unless the exception occurred due to an interruption, raise it again
			if (!interrupted) {
				String fromDatabaseExceptionMessage = "A SQL exception occurred while querying the from-database. ";
				LOGGER.error(jobName + ": " + fromDatabaseExceptionMessage + terminationMessage);
				LOGGER.error(Constants.STACKTRACE, e);
				throw new JobExecutionException(fromDatabaseExceptionMessage);
			}
		} finally {
			closeFromConnections();
		}

		////////////////////
		// Store outputs
		////////////////////
		// No outputs to store here
	}

	private void extract(String fromUsername, String fromPassword, String fromSQLExecute, String fromSQLQuery) throws SQLException {
		fromConnection = DriverManager.getConnection(fromConnectionURL, fromUsername, fromPassword);
		fromStatement = fromConnection.createStatement();
		
		// If there is an execute SQL to run, then execute it
		if (fromSQLExecute.length() > 0) {
			LOGGER.info(jobName + ": " + "Running the following SQL statement:" + NEW_LINE + StringUtils.abbreviate(fromSQLExecute, 1000));
			
			// execute
			fromStatement.execute(fromSQLExecute);
			LOGGER.info(jobName + ": " + "Succesfully executed the SQL statement. ");
		}
		
		// Grab results using the from SQL query
		LOGGER.info(jobName + ": " + "Running the following SQL statement:" + NEW_LINE + StringUtils.abbreviate(fromSQLQuery, 1000));

		// query
		fromResults = fromStatement.executeQuery(fromSQLQuery);
		LOGGER.info(jobName + ": " + "Successfully retrieved results from the SQL query. ");
	}
	
	private void load(String toUsername, String toPassword, String toTableName) throws JobExecutionException {
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
			LOGGER.info(jobName + ": " + "Creating the table " + toTableName + " using the following SQL:" + NEW_LINE + toTableCreateSQL);
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
					batchCount = 0;
				}
				if (nRecord % MOD_SIZE == 0) {
					LOGGER.info(jobName + ": " + "Total records inserted into the table " + toTableName + " thus far: " + nRecord);
				}
			}
	
			// Execute the last batch
			toPreparedStatement.executeBatch();
			toConnection.commit();
			LOGGER.info(jobName + ": " + "Inserted data into the table " + toTableName + ". Total number of records: " + nRecord);
		} catch (SQLException e) {
			
			// Unless the exception occurred due to an interruption, raise it again
			if (!interrupted) {
				String toDatabaseExceptionMessage = "A SQL exception occurred while refreshing the to-database. ";
				LOGGER.error(jobName + ": " + toDatabaseExceptionMessage + terminationMessage);
				LOGGER.error(Constants.STACKTRACE, e);
				rollback();
				throw new JobExecutionException(toDatabaseExceptionMessage);
			}
		} finally {
			closeToConnections();
		}
	}
	
	private void closeFromConnections() {
		if (!fromClosed) {
			fromClosed = true;
			try {
				if (fromResults != null) fromResults.close();
				if (fromStatement != null) fromStatement.close();
				if (fromConnection != null) fromConnection.close();
				LOGGER.info(jobName + ": " + "Closed all connections to " + fromConnectionURL + ".");
			} catch (SQLException e) {
				LOGGER.error(Constants.STACKTRACE, e);
				LOGGER.error(jobName + ": " + "Failed to close all connections. ");
			}
		}
	}
	
	private void closeToConnections() {
		if (!toClosed) {
			toClosed = true;
			try {
				if (toPreparedStatement != null) toPreparedStatement.close();
				if (toTables != null) toTables.close();
				if (toStatement != null) toStatement.close();
				if (toConnection != null) toConnection.close();
				LOGGER.info(jobName + ": " + "Closed all connections to " + toConnectionURL + ".");
			} catch (SQLException e) {
				LOGGER.error(Constants.STACKTRACE, e);
				LOGGER.error(jobName + ": " + "Failed to close all connections. ");
			}	
		}
	}
	
	private void rollback() {
		
		// Roll back in the case of an error (auto-commit disabled)
		LOGGER.info(jobName + ": Attempting to rollback commits made to the to-database. ");
		try {
			if (toConnection!= null) toConnection.rollback();
		} catch (SQLException eRollback) {
			LOGGER.error(Constants.STACKTRACE, eRollback);
			LOGGER.warn(jobName + ": Failed to rollback commits made to the to-database. ");
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		interrupted = true;
		LOGGER.warn(jobName + ": " + "The " + jobName + " job was interrupted. Will attempt to close all connections and terminate the job.");
		rollback();
		closeToConnections();
		closeFromConnections();
	}

}
