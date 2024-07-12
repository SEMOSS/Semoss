package prerna.rpa.quartz.jobs.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.rpa.RPAUtil;
import prerna.rpa.db.jdbc.JDBCUtil;
import prerna.rpa.db.jedis.JedisStore;
import prerna.rpa.quartz.CommonDataKeys;
import prerna.util.Constants;
import redis.clients.jedis.Jedis;

public class JedisToJDBCJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(JedisToJDBCJob.class.getName());
	
	private static final String NEW_LINE = System.lineSeparator();

	/** {@code String} */
	public static final String IN_JEDIS_HASH_KEY = CommonDataKeys.JEDIS_HASH;

	/** {@code String} */
	public static final String IN_TABLE_NAME_KEY = JedisToJDBCJob.class + ".tableName";
	
	/**
	 * {@code String[]} - The first header corresponds to the field of the Jedis
	 * hash, the rest are assigned to the semicolon-delimited csv values from the
	 * value corresponding to this field.
	 */
	public static final String IN_COLUMN_HEADERS_KEY = JedisToJDBCJob.class + ".columnHeaders";

	/** {@code String[]} */
	public static final String IN_COLUMN_TYPES_KEY = JedisToJDBCJob.class + ".columnTypes";
	
	/** {@code String} */
	public static final String IN_JDBC_DRIVER_KEY = CommonDataKeys.JDBC_DRIVER;

	/** {@code String} */
	public static final String IN_JDBC_CONNECTION_URL_KEY = CommonDataKeys.JDBC_CONNECTION_URL;

	/** {@code String} */
	public static final String IN_JDBC_USERNAME_KEY = CommonDataKeys.JDBC_USERNAME;

	/** {@code String} */
	public static final String IN_JDBC_PASSWORD_KEY = CommonDataKeys.JDBC_PASSWORD;

	private static final int BATCH_SIZE = 1024;
	private static final int MOD_SIZE = 100000;
	
	// Closable objects
	private Connection connection;
	private Statement statement;
	private ResultSet tables;
	private PreparedStatement preparedStatement;
	
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
		
		String jedisHash = dataMap.getString(IN_JEDIS_HASH_KEY);
		String tableName = dataMap.getString(IN_TABLE_NAME_KEY);
		String[] columnHeaders = (String[]) dataMap.get(IN_COLUMN_HEADERS_KEY);
		String[] columnTypes = (String[]) dataMap.get(IN_COLUMN_TYPES_KEY);
		
		String driver = dataMap.getString(IN_JDBC_DRIVER_KEY);
		connectionURL = dataMap.getString(IN_JDBC_CONNECTION_URL_KEY);
		String username = dataMap.getString(IN_JDBC_USERNAME_KEY);
		String password = dataMap.getString(IN_JDBC_PASSWORD_KEY);
		
		////////////////////
		// Do work
		////////////////////
		JDBCUtil.loadDriver(driver);
		
		// Load the data into the JDBC connection	
		long startTime = System.currentTimeMillis();
		try {
			connection = DriverManager.getConnection(connectionURL, username, password);
			statement = connection.createStatement();
			
			// Check whether the to table exists
			DatabaseMetaData toMetaData = connection.getMetaData();
			tables = toMetaData.getTables(null, null, tableName, null);
			boolean tableExists = tables.next();

			// If the to table exists, then drop
			if (tableExists) {
				statement.execute("DROP TABLE " + tableName + ";");
				LOGGER.info(jobName + ": " + "Dropped the table " + tableName + ".");
			}
			
			// Create the table
			String tableCreateSQL = JDBCUtil.generateCreateTableSQL(columnHeaders, columnTypes, tableName);
			LOGGER.info(jobName + ": " + "Creating the table " + tableName + " using the following SQL:" + NEW_LINE + tableCreateSQL);
			statement.execute(tableCreateSQL);
			LOGGER.info(jobName + ": " + "Created the table " + tableName + ".");		
			
			// Get the number of columns
			int nCol = columnHeaders.length;
			
			// Insert in batches
			preparedStatement = connection.prepareStatement(JDBCUtil.generateInsertSQL(columnHeaders, tableName));
			int batchCount = 0;
			int nRecord = 0;
			LOGGER.info(jobName + ": " + "Inserting data into the table " + tableName + ".");
			connection.setAutoCommit(false);
			try (Jedis jedis = JedisStore.getInstance().getResource()) {
				Set<String> ids = jedis.hkeys(jedisHash);
				for (String id : ids) {
					
					// Allows commas in csv data, so long as they are surrounded in quotes
					String[] row = jedis.hget(jedisHash, id).split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
					
					// Set the id first
					preparedStatement.setObject(1, id);
					
					// Then set attributes
					// Subtract 1 from nCol b/c we have already set the id
					for (int c = 0; c < nCol - 1; c++) {
						
						// Start from 2nd index in prepared statement b/c we have already set the id
						preparedStatement.setObject(c + 2, row[c].replace("\"", ""));
					}
					preparedStatement.addBatch();
					batchCount++;
					nRecord++;
					if (batchCount % BATCH_SIZE == 0) {
						preparedStatement.executeBatch();
						connection.commit();
						batchCount = 0;
					}
					if (nRecord % MOD_SIZE == 0) {
						LOGGER.info(jobName + ": " + "Total records inserted into the table " + tableName + " thus far: " + nRecord);
					}
				}
			}
			
			// Execute the last batch
			preparedStatement.executeBatch();
			connection.commit();
			LOGGER.info(jobName + ": " + "Inserted data into the table " + tableName + ". Total number of records: " + nRecord);
			LOGGER.info(jobName + ": " + "Completed the ETL routine for the " + jobName + " job.");
			LOGGER.info(jobName + ": " + "Elapsed time " + RPAUtil.minutesSinceStartTime(startTime) + " minutes.");
		} catch (SQLException e) {
			
			// Unless the exception occurred due to an interruption, raise it again
			if (!interrupted) {
				String jdbcExceptionMessage = "A SQL exception occurred while loading data from Redis into the given JDBC connection. ";
				LOGGER.error(jobName + ": " + jdbcExceptionMessage + terminationMessage);
				LOGGER.error(Constants.STACKTRACE, e);
				throw new JobExecutionException(jdbcExceptionMessage);
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
				if (preparedStatement != null) preparedStatement.close();
				if (tables != null) tables.close();
				if (statement != null) statement.close();
				if (connection != null) connection.close();
				LOGGER.info(jobName + ": " + "Closed all connections to " + connectionURL + ".");
			} catch (SQLException e) {
				LOGGER.error(jobName + ": " + "Failed to close all connections. ", e);
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
