package prerna.rpa.quartz.jobs.jdbc.maria;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import prerna.rpa.quartz.CommonDataKeys;
import prerna.rpa.quartz.QuartzUtility;
import prerna.rpa.quartz.jobs.jdbc.JDBCUtil;
import prerna.rpa.quartz.jobs.jdbc.maria.GenerateHashedPrimkeyJob;

public class GenerateHashedPrimkeyJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(GenerateHashedPrimkeyJob.class.getName());

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
	public static final String TABLE_NAME_KEY = GenerateHashedPrimkeyJob.class + ".tableName";

	/**
	 * {@code String} - Optional comma-separated string of column names to include
	 * in the hash. If not specified, then this job will use all columns when
	 * generating the hash.
	 */
	public static final String HASH_COLUMNS_KEY = GenerateHashedPrimkeyJob.class + ".hashColumns";
	
	/** {@code String} */
	public static final String PRIMKEY_NAME_KEY = GenerateHashedPrimkeyJob.class + ".primkeyName";
	
	/** {@code int} */
	public static final String PRIMKEY_LENGTH_KEY = GenerateHashedPrimkeyJob.class + ".primkeyLength";

	// Closable objects
	private Connection connection;
	private Statement statement;
	private ResultSet tables;
	private ResultSet columns;

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

		// This job will only work for MariaDB connections
		String driver = dataMap.getString(DRIVER_KEY);
		if (!driver.contains("mariadb")) {
			String notMariaDBMessage = "This job only works for MariaDB connections. ";
			LOGGER.error(jobName + ": " + notMariaDBMessage + terminationMessage);
			throw new JobExecutionException(notMariaDBMessage);
		}
		connectionURL = dataMap.getString(CONNECTION_URL_KEY);
		String username = dataMap.getString(USERNAME_KEY);
		String password = dataMap.getString(PASSWORD_KEY);
		String tableName = dataMap.getString(TABLE_NAME_KEY);

		// Since this is optional, if the dataMap doesn't assign a key set as an empty
		// string
		String hashColsString = dataMap.containsKey(HASH_COLUMNS_KEY) ? dataMap.getString(HASH_COLUMNS_KEY) : "";
		String primkeyName = dataMap.getString(PRIMKEY_NAME_KEY);
		int primkeyLength = dataMap.getInt(PRIMKEY_LENGTH_KEY);

		////////////////////
		// Do work
		////////////////////
		JDBCUtil.loadDriver(driver);

		// Generate the hashed primkey
		long startTime = System.currentTimeMillis();
		try {
			connection = DriverManager.getConnection(connectionURL, username, password);
			statement = connection.createStatement();

			// Throw an exception if the table doesn't exist
			DatabaseMetaData metaData = connection.getMetaData();
			tables = metaData.getTables(null, null, tableName, null);
			boolean tableExists = tables.next();
			if (!tableExists) {
				String tableNotExistsMessage = "The specified table, " + tableName + ", does not exist. ";
				LOGGER.error(jobName + ": " + tableNotExistsMessage + terminationMessage);
				throw new JobExecutionException(tableNotExistsMessage);
			}

			// Get all the column names for the table
			List<String> allCols = new ArrayList<String>();
			// Does preserve ordinal position
			columns = metaData.getColumns(null, null, tableName, null);
			while (columns.next()) {
				allCols.add(columns.getString("COLUMN_NAME"));
			}
			
			// Throw an exception if the primkey already exists
			if (allCols.contains(primkeyName)) {
				String existingPrimkeyColMessage = "The column " + primkeyName + " already exists in the table " + tableName + ". ";
				LOGGER.error(jobName + ": " + existingPrimkeyColMessage + terminationMessage);
				throw new JobExecutionException(existingPrimkeyColMessage);
			}
			
			// Decide which columns will be used in the hash
			List<String> hashCols = new ArrayList<String>();

			// If the column names are given, then pull from the input
			// Else, include all the columns in the hash
			if (hashColsString.length() > 0) {
				for (String colName : hashColsString.split(",")) {
					hashCols.add(colName);
				}
			} else {
				hashCols = allCols;
			}
			
			// Add a column for the primary key
			String sqlAddPrimkey = "ALTER TABLE " + tableName + " ADD " + primkeyName + " VARCHAR(" + primkeyLength	+ ");";
			LOGGER.info(jobName + ": " + "Running the following SQL statement:\n" + sqlAddPrimkey);
			statement.execute(sqlAddPrimkey);
			LOGGER.info(jobName + ": " + "Successfully added the column " + primkeyName + " to the table " + tableName + ".");

			// Utilize MariaDB's built-in SHA256 hasher to simplify things
			StringBuilder generateHashedPrimkeySQL = new StringBuilder();
			generateHashedPrimkeySQL.append("UPDATE ");
			generateHashedPrimkeySQL.append(tableName);
			generateHashedPrimkeySQL.append(" SET ");
			generateHashedPrimkeySQL.append(primkeyName);

			// Left takes the left n characters of the hash
			// SHA2 performs an SHA256 hash on the string
			// CONCAT groups together all the desired columns for the hash
			generateHashedPrimkeySQL.append(" = LEFT(SHA2(CONCAT(\n");
			int nCol = hashCols.size();
			for (int i = 0; i < nCol; i++) {
				if (i > 0) generateHashedPrimkeySQL.append(", \n");
				
				// Must not be null for CONCAT to work
				// Replace all null and empty string with null
				// otherwise | a |   | b | will hash the same as | a | b |   |
				// I've seen that actually happen
				// Also some of the columns may not be strings, so cast first
				generateHashedPrimkeySQL.append("IFNULL(NULLIF(CAST(");
				generateHashedPrimkeySQL.append(hashCols.get(i));
				generateHashedPrimkeySQL.append(" AS CHAR CHARACTER SET utf8), ''), 'null')");
			}
			generateHashedPrimkeySQL.append("\n), 256), " + primkeyLength + ");");

			// Populate the primkey column with the hash
			LOGGER.info(jobName + ": " + "Running the following SQL statement:\n" + generateHashedPrimkeySQL.toString());
			statement.executeUpdate(generateHashedPrimkeySQL.toString());
			LOGGER.info(jobName + ": " + "Successfully populated the hashed column " + primkeyName + " in the table " + tableName + ".");
			
			// Alter table to make this the primary key
			String makePrimkeySQL = "ALTER TABLE " + tableName + " ADD PRIMARY KEY (" + primkeyName + ");";
			LOGGER.info(jobName + ": " + "Running the following SQL statement:\n" + makePrimkeySQL);
			try {
				statement.execute(makePrimkeySQL);
				LOGGER.info(jobName + ": " + "Successfully made " + primkeyName + " the primary key of the table " + tableName + ".");
			} catch (SQLException e) {
				LOGGER.warn(jobName + ": " + "Failed to make the column " + primkeyName + " a primary key. Will still create an index on this column.", e);
				
				// Most of the time the primary key fails because of duplicate entries
				// While we can't make the primary key in this case (b/c it needs to be unique)
				// we can still create an index on this column
				String addIndexSQL = "CREATE INDEX index_" + primkeyName + " ON " + tableName + " (" + primkeyName + ");";
				LOGGER.info(jobName + ": " + "Running the following SQL statement:\n" + addIndexSQL);
				statement.execute(addIndexSQL);
				LOGGER.info(jobName + ": " + "Successfully added an index on " + primkeyName + ".");
			}
			
			// Complete
			LOGGER.info(jobName + ": " + "Completed hashed primary key generation for the " + jobName + " job.");
			LOGGER.info(jobName + ": " + "Elapsed time " + QuartzUtility.secondsSinceStartTime(startTime) + " seconds.");
		} catch (SQLException e) {
			String primkeyExceptionMessage = "A SQL exception occured while generating the hashed primary key for the table " + tableName + ". ";
			LOGGER.error(jobName + ": " + primkeyExceptionMessage + terminationMessage);
			throw new JobExecutionException(primkeyExceptionMessage, e);
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
			if (columns != null) columns.close();
			if (tables != null) tables.close();
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
