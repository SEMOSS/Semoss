package prerna.sablecc2.reactor.scheduler;

import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BIGINT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BIT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BLOB_DATA;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BOOLEAN;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BOOL_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.BOOL_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CALENDAR;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CALENDAR_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CHECKIN_INTERVAL;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.CRON_EXPRESSION;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.DEC_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.DEC_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.DESCRIPTION;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.END_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.ENTRY_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.EXECUTION_DELTA;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.EXECUTION_END;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.EXECUTION_START;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.EXEC_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.FIRED_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IMAGE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.INSTANCE_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.INTEGER;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.INT_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.INT_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IS_DURABLE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IS_NONCONCURRENT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.IS_UPDATE_DATA;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_CATEGORY;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_CLASS_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_DATA;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_GROUP;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_TAG;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.JOB_TAGS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.LAST_CHECKIN_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.LOCK_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.LONG_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.LONG_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.MISFIRE_INSTR;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.NEXT_FIRE_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.NOT_NULL;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.NUMERIC_13_4;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PARAMETERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE_PARAMETERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PREV_FIRE_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.PRIORITY;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_BLOB_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_CALENDARS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_CRON_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_FIRED_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_JOB_DETAILS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_LOCKS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_PAUSED_TRIGGER_GRPS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_SCHEDULER_STATE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_SIMPLE_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_SIMPROP_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.QRTZ_TRIGGERS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.REPEAT_COUNT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.REPEAT_INTERVAL;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.REQUESTS_RECOVERY;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SCHED_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SCHED_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMALLINT;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMSS_AUDIT_TRAIL;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMSS_EXECUTION;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMSS_JOB_RECIPES;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SMSS_JOB_TAGS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.START_TIME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.STATE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.STR_PROP_1;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.STR_PROP_2;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.STR_PROP_3;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.SUCCESS;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TIMESTAMP;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TIMES_TRIGGERED;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TIME_ZONE_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_GROUP;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_NAME;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_ON_LOAD;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_STATE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.TRIGGER_TYPE;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.USER_ID;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_120;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_16;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_200;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_250;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_255;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_40;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_512;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_8;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_80;
import static prerna.sablecc2.reactor.scheduler.SchedulerConstants.VARCHAR_95;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronExpression;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RDBMSUtility;

public class SchedulerH2DatabaseUtility {
	
	private static final Logger logger = LogManager.getLogger(SchedulerH2DatabaseUtility.class);
	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/**
	 * SELECT SMSS_JOB_RECIPES.USER_ID, SMSS_JOB_RECIPES.JOB_ID, SMSS_JOB_RECIPES.JOB_NAME, SMSS_JOB_RECIPES.JOB_GROUP, SMSS_JOB_RECIPES.CRON_EXPRESSION,
	 * SMSS_JOB_RECIPES.PIXEL_RECIPE, SMSS_JOB_RECIPES.PIXEL_RECIPE_PARAMETERS, SMSS_JOB_RECIPES.PARAMETERS, QRTZ_TRIGGERS.NEXT_FIRE_TIME, QRTZ_TRIGGERS.PREV_FIRE_TIME
	 * FROM SMSS_JOB_RECIPES LEFT OUTER JOIN QRTZ_TRIGGERS ON SMSS_JOB_RECIPES.JOB_NAME = QRTZ_TRIGGERS.JOB_NAME AND SMSS_JOB_RECIPES.JOB_GROUP = QRTZ_TRIGGERS.JOB_GROUP
	 */
	private static final String BASE_JOB_DETAILS_QUERY = "SELECT SMSS_JOB_RECIPES.USER_ID, "
			+ "SMSS_JOB_RECIPES.JOB_ID, "
			+ "SMSS_JOB_RECIPES.JOB_NAME, "
			+ "SMSS_JOB_RECIPES.JOB_GROUP, "
			+ "SMSS_JOB_RECIPES.CRON_EXPRESSION, "
			+ "SMSS_JOB_RECIPES.PIXEL_RECIPE, "
			+ "SMSS_JOB_RECIPES.PIXEL_RECIPE_PARAMETERS, "
			+ "SMSS_JOB_RECIPES.PARAMETERS, "
			+ "QRTZ_TRIGGERS.NEXT_FIRE_TIME, "
			+ "QRTZ_TRIGGERS.PREV_FIRE_TIME, "
			+ "QRTZ_TRIGGERS.TRIGGER_STATE";

	private static final String JOIN_JOB_DETAILS_QUERY = "LEFT OUTER JOIN QRTZ_TRIGGERS ON "
			+ "SMSS_JOB_RECIPES.JOB_ID = QRTZ_TRIGGERS.JOB_NAME "
			+ "AND SMSS_JOB_RECIPES.JOB_GROUP = QRTZ_TRIGGERS.JOB_GROUP ";

	static RDBMSNativeEngine schedulerDb;
	static AbstractSqlQueryUtil queryUtil;

	private SchedulerH2DatabaseUtility() {
		throw new IllegalStateException("Utility class");
	}

	public static void startServer() throws IOException, SQLException {
		schedulerDb = (RDBMSNativeEngine) Utility.getEngine(Constants.SCHEDULER_DB);
		schedulerDb.getConnection();
		queryUtil = schedulerDb.getQueryUtil();
		
		SchedulerOwlCreator owlCreator = new SchedulerOwlCreator(schedulerDb);
		if (owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}

		initialize();
		
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		try {
			if(!scheduler.isStarted()) {
				logger.info("Scheduler is not active. Starting up scheduler...");
				scheduler.start();
			}
		} catch (SchedulerException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	public static void initialize() throws SQLException {
		String schema = schedulerDb.getSchema();
		Connection connection = schedulerDb.getConnection();

		createQuartzTables(connection, schema);
		createSemossTables(connection, schema);
		addAllPrimaryKeys();
		addAllForeignKeys();
	}

	public static Connection connectToScheduler() {
		Connection connection = null;

		try {
			schedulerDb = (RDBMSNativeEngine) Utility.getEngine(Constants.SCHEDULER_DB);
			connection = schedulerDb.getConnection();
		} catch (SQLException se) {
			logger.error(Constants.STACKTRACE, se);
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
		}

		if (connection == null) {
			throw new NullPointerException("Connection wasn't able to be created.");
		}

		return connection;
	}
	
	public static RDBMSNativeEngine getSchedulerDB() {
		return schedulerDb;
	}
	
	public static AbstractSqlQueryUtil getQueryUtil() {
		return schedulerDb.getQueryUtil();
	}

	public static void closeConnection(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	public static boolean insertIntoExecutionTable(String execId, String jobId, String jobGroup) {
		Connection connection = connectToScheduler();
		try (PreparedStatement statement = connection
				.prepareStatement("INSERT INTO SMSS_EXECUTION (EXEC_ID, JOB_ID, JOB_GROUP) VALUES (?,?,?)")) {
			statement.setString(1, execId);
			statement.setString(2, jobId);
			statement.setString(3, jobGroup);
			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}

		return true;
	}
	
	public static String[] executionIdExists(String execId) {
		Connection connection = connectToScheduler();
		ResultSet rs = null;
		try (PreparedStatement statement = connection
				.prepareStatement("SELECT JOB_ID, JOB_GROUP FROM SMSS_EXECUTION WHERE EXEC_ID = ?")) {
			statement.setString(1, execId);
			rs = statement.executeQuery();
			if(rs.next()) {
				String jobId = rs.getString(1);
				String jobGroup = rs.getString(2);
				return new String[] {jobId , jobGroup};
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return null;
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return null;
	}
	
	public static boolean removeExecutionId(String execId) {
		Connection connection = connectToScheduler();
		try (PreparedStatement statement = connection
				.prepareStatement("DELETE FROM SMSS_EXECUTION WHERE EXEC_ID = ?")) {
			statement.setString(1, execId);
			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}

		return true;
	}

	public static boolean insertIntoAuditTrailTable(String jobId, String jobGroup, Long start, Long end, boolean success) {
		Connection connection = connectToScheduler();

		Timestamp startTimeStamp = new Timestamp(start);
		Timestamp endTimeStamp = new Timestamp(end);

		try (PreparedStatement statement = connection
				.prepareStatement("INSERT INTO SMSS_AUDIT_TRAIL (JOB_ID, JOB_GROUP, EXECUTION_START, EXECUTION_END, EXECUTION_DELTA, SUCCESS) VALUES (?,?,?,?,?,?)")) {
			statement.setString(1, jobId);
			statement.setString(2, jobGroup);
			statement.setTimestamp(3, startTimeStamp);
			statement.setTimestamp(4, endTimeStamp);
			statement.setString(5, String.valueOf(end - start));
			statement.setBoolean(6, success);
			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}

		return true;
	}

	public static boolean insertIntoJobRecipesTable(String userId, String jobId, String jobName, String jobGroup, String cronExpression, String recipe, String recipeParameters,
			String jobCategory, boolean triggerOnLoad, String parameters, List<String> jobTags ) {
		
		Connection connection = connectToScheduler();
		try (PreparedStatement statement = connection
				.prepareStatement("INSERT INTO SMSS_JOB_RECIPES (USER_ID, JOB_ID, JOB_NAME, JOB_GROUP, CRON_EXPRESSION, PIXEL_RECIPE, PIXEL_RECIPE_PARAMETERS, JOB_CATEGORY, TRIGGER_ON_LOAD, PARAMETERS) VALUES (?,?,?,?,?,?,?,?,?,?)")) {
			statement.setString(1, userId);
			statement.setString(2, jobId);
			statement.setString(3, jobName);
			statement.setString(4, jobGroup);
			statement.setString(5, cronExpression);
			statement.setString(8, jobCategory);
			statement.setBoolean(9, triggerOnLoad);

			if(queryUtil.allowBlobJavaObject()) {
				statement.setBlob(6, stringToBlob(connection, recipe));
				if(recipeParameters == null || recipeParameters.isEmpty()) {
					statement.setNull(7, java.sql.Types.BLOB);
				} else {
					statement.setBlob(7, stringToBlob(connection, recipeParameters));
				}
				statement.setBlob(10, stringToBlob(connection, parameters));
			} else {
				statement.setString(6, recipe);
				if(recipeParameters == null || recipeParameters.isEmpty()) {
					statement.setNull(7, java.sql.Types.BLOB);
				} else {
					statement.setString(7, recipeParameters);
				}
				statement.setString(10, parameters);
			}


			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}


		return updateJobTags(jobId, jobTags);
	}

	/**
	 * Update the job tags for a specific job
	 * @param jobId
	 * @param jobTags
	 * @return
	 */
	public static boolean updateJobTags( String jobId, List<String> jobTags) {
		Connection connection = connectToScheduler();

		// first we delete old tags
		try (PreparedStatement statement = connection.prepareStatement("DELETE FROM SMSS_JOB_TAGS WHERE JOB_ID=?")) {
			statement.setString(1, jobId);
			statement.execute();
		} catch( SQLException e) {
			logger.error(Constants.STACKTRACE, e );
			return false;
		}


		if(jobTags == null) {
			return true;
		}

		// bulk insert for the job tags
		try (PreparedStatement statement = connection.prepareStatement("INSERT INTO SMSS_JOB_TAGS (JOB_ID, JOB_TAG) VALUES (?,?)")) {
			for(String jobTag : jobTags) {
				statement.setString(1, jobId);
				statement.setString(2, jobTag);
				statement.addBatch();
			}
			statement.executeBatch();
		} catch( SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}
		return true;
	}
	
	public static boolean updateJobRecipesTable(String userId, String jobId, String jobName, String jobGroup, String cronExpression, String recipe, String recipeParameters,
			String jobCategory, boolean triggerOnLoad, String parameters, String existingJobName, String existingJobGroup, List<String> jobTags) {
		
		Connection connection = connectToScheduler();
		try (PreparedStatement statement = connection
				.prepareStatement("UPDATE SMSS_JOB_RECIPES SET USER_ID = ?, JOB_NAME = ?, JOB_GROUP = ?, CRON_EXPRESSION = ?, PIXEL_RECIPE = ?, "
						+ "PIXEL_RECIPE_PARAMETERS = ?, JOB_CATEGORY = ?, TRIGGER_ON_LOAD = ?, PARAMETERS = ? WHERE JOB_ID = ? AND JOB_GROUP = ?")) {
			statement.setString(1, userId);
			statement.setString(2, jobName);
			statement.setString(3, jobGroup);
			statement.setString(4, cronExpression);
			statement.setString(7, jobCategory);
			statement.setBoolean(8, triggerOnLoad);

			if(queryUtil.allowBlobJavaObject()) {
				statement.setBlob(5, stringToBlob(connection, recipe));
				if(recipeParameters == null || recipeParameters.isEmpty()) {
					statement.setNull(6, java.sql.Types.BLOB);
				} else {
					statement.setBlob(6, stringToBlob(connection, recipeParameters));
				}
				statement.setBlob(9, stringToBlob(connection, parameters));
			} else {
				statement.setString(5, recipe);
				if(recipeParameters == null || recipeParameters.isEmpty()) {
					statement.setNull(6, java.sql.Types.BLOB);
				} else {
					statement.setString(6, recipeParameters);
				}
				statement.setString(9, parameters);
			}
			
			// where clause filters
			statement.setString(10, jobId);
			statement.setString(11, existingJobGroup);
			
			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}

		return updateJobTags(jobId, jobTags);
	}

	public static boolean removeFromJobRecipesTable(String jobId , String jobGroup) {
		Connection connection = connectToScheduler();
		try (PreparedStatement statement = connection
				.prepareStatement("DELETE FROM SMSS_JOB_RECIPES WHERE JOB_ID =? AND JOB_GROUP=?")) {
			statement.setString(1, jobId);
			statement.setString(2, jobGroup);

			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}

		return true;
	}

	public static boolean existsInJobRecipesTable(String jobId, String jobGroup) {
		Connection connection = connectToScheduler();
		try (PreparedStatement statement = connection
				.prepareStatement("SELECT COUNT(JOB_ID) FROM SMSS_JOB_RECIPES WHERE JOB_ID =? AND JOB_GROUP=?");) {
			statement.setString(1, jobId);
			statement.setString(2, jobGroup);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					int count = result.getInt(1);
					if (count == 0) {
						return false;
					}
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}

		return true;
	}

//	public static boolean existsInAuditTrailTable(String jobName, String jobGroup) {
//		Connection connection = connectToScheduler();
//		try (PreparedStatement statement = connection
//				.prepareStatement("SELECT COUNT(JOB_NAME) FROM SMSS_AUDIT_TRAIL WHERE JOB_NAME=? AND JOB_GROUP=?")) {
//			statement.setString(1, jobName);
//			statement.setString(2, jobGroup);
//			try (ResultSet result = statement.executeQuery()) {
//				while (result.next()) {
//					int count = result.getInt(1);
//					if (count == 0) {
//						return false;
//					}
//				}
//			}
//		} catch (SQLException e) {
//			logger.error(Constants.STACKTRACE, e);
//			return false;
//		}
//
//		return true;
//	}

	public static Map<String, Map<String, String>> retrieveJobsForApp(String appId, List<String> jobTags ) {
		Connection connection = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();

		try (PreparedStatement statement = connection
				.prepareStatement(createJobQuery("WHERE SMSS_JOB_RECIPES.JOB_GROUP=?",jobTags))) {
			statement.setString(1, appId);

			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return jobMap;
	}

	public static Map<String, Map<String, String>> retrieveUsersJobsForApp(String appId, String userId, List<String> jobTags) {
		Connection connection = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		try (PreparedStatement statement = connection
				.prepareStatement(createJobQuery(" WHERE SMSS_JOB_RECIPES.USER_ID=? AND SMSS_JOB_RECIPES.JOB_GROUP=?",jobTags))) {
			statement.setString(1, userId);
			statement.setString(2, appId);

			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return jobMap;
	}

	public static Map<String, Map<String, String>> retrieveUsersJobs(String userId, List<String> jobTags) {
		Connection connection = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		try (PreparedStatement statement = connection
				.prepareStatement(createJobQuery(" WHERE SMSS_JOB_RECIPES.USER_ID=?",jobTags))) {
			statement.setString(1, userId);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return jobMap;
	}

	/**
	 * Query generation helper
	 * @param where
	 * @param jobTags
	 * @return
	 */
	public static String createJobQuery(String where, List<String> jobTags ) {
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append(BASE_JOB_DETAILS_QUERY);
		// add the job tags
		// this depends on how group_concat is defined based on the rdbms type
		queryBuilder.append(", (SELECT ")
			.append(queryUtil.processGroupByFunction("JOB_TAG", ",", true))
			.append(" FROM SMSS_JOB_TAGS WHERE SMSS_JOB_TAGS.JOB_ID=SMSS_JOB_RECIPES.JOB_ID) AS JOB_TAGS ");

		if(jobTags == null) {
			queryBuilder.append( "FROM SMSS_JOB_RECIPES ");
		} else {
			queryBuilder.append( "FROM SMSS_JOB_TAGS,SMSS_JOB_RECIPES ");
		}

		queryBuilder.append( JOIN_JOB_DETAILS_QUERY );

		if( where != null ) {
			queryBuilder.append(' ');
			queryBuilder.append(where);
		}
		if( jobTags != null) {
			if(where != null) {
				queryBuilder.append(" AND ");
			} else {
				queryBuilder.append(" WHERE ");
			}

			Iterator<String> i = jobTags.iterator();
			while( i.hasNext() ) {
				queryBuilder.append( String.format(" '%s' IN (SELECT SMSS_JOB_TAGS.JOB_TAG FROM SMSS_JOB_TAGS WHERE SMSS_JOB_TAGS.JOB_ID=SMSS_JOB_RECIPES.JOB_ID)", i.next()));
				if( i.hasNext() ) {
					queryBuilder.append( " OR " );
				}
			}
		}
		
//		System.out.println(queryBuilder.toString());
		return queryBuilder.toString();
	}

	public static Map<String, Map<String, String>> retrieveAllJobs(List<String> jobTags) {
		Connection connection = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		String query = createJobQuery(null, jobTags);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return jobMap;
	}
	
	private static void fillJobDetailsMap(Map<String, Map<String, String>> jobMap, ResultSet result) throws SQLException {
		Map<String, String> jobDetailsMap = new HashMap<>();

		String userId = result.getString(USER_ID);
		String jobId  = result.getString(JOB_ID);
		String jobTags  = result.getString(JOB_TAGS);
		String jobName = result.getString(JOB_NAME);
		String jobGroup = result.getString(JOB_GROUP);
		String cronExpression = result.getString(CRON_EXPRESSION);
		String recipe = null;
		String recipeParameters = null;
		String parameters = null;
		BigInteger prevExecTime = null;
		BigInteger nextExecTime = null;
		BigDecimal pExecTimeD = result.getBigDecimal(PREV_FIRE_TIME);
		BigDecimal nExecTimeD = result.getBigDecimal(NEXT_FIRE_TIME);
		String tiggerState = result.getString(TRIGGER_STATE);
		if(pExecTimeD != null) {
			prevExecTime = pExecTimeD.toBigInteger();
		}
		if(nExecTimeD != null) {
			nextExecTime = nExecTimeD.toBigInteger();
		}
		if(queryUtil.allowBlobJavaObject()) {
			try {
				recipe = RDBMSUtility.flushBlobToString(result.getBlob(PIXEL_RECIPE));
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			try {
				recipeParameters = RDBMSUtility.flushBlobToString(result.getBlob(PIXEL_RECIPE_PARAMETERS));
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			try {
				parameters = RDBMSUtility.flushBlobToString(result.getBlob(PARAMETERS));
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		} else {
			recipe = result.getString(PIXEL_RECIPE);
			recipeParameters = result.getString(PIXEL_RECIPE_PARAMETERS);
			parameters = result.getString(PARAMETERS);
		}
		
		jobDetailsMap.put(USER_ID, userId);
		jobDetailsMap.put(ReactorKeysEnum.JOB_ID.getKey(), jobId);
		jobDetailsMap.put(ReactorKeysEnum.JOB_GROUP.getKey(), jobGroup );
		jobDetailsMap.put(ReactorKeysEnum.JOB_NAME.getKey(), jobName);
		jobDetailsMap.put(ReactorKeysEnum.JOB_TAGS.getKey(), jobTags );
		jobDetailsMap.put(ReactorKeysEnum.CRON_EXPRESSION.getKey(), cronExpression);
		jobDetailsMap.put(ReactorKeysEnum.RECIPE.getKey(), recipe);
		jobDetailsMap.put(ReactorKeysEnum.RECIPE_PARAMETERS.getKey(), recipeParameters);
		jobDetailsMap.put(ScheduleJobReactor.PARAMETERS, parameters);
		// add next fire time
		if(nextExecTime != null && !tiggerState.equals("PAUSED")) {
			Instant instant = Instant.ofEpochMilli(nextExecTime.longValue());
			DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			jobDetailsMap.put(NEXT_FIRE_TIME, fmt.format(instant.atZone(ZoneId.systemDefault())));
		} else {
			jobDetailsMap.put(NEXT_FIRE_TIME, "INACTIVE");
		}
		if(prevExecTime != null) {
			if(BigInteger.valueOf(-1).equals(prevExecTime)) {
				jobDetailsMap.put(PREV_FIRE_TIME, "N/A");
			} else {
				Instant instant = Instant.ofEpochMilli(prevExecTime.longValue());
				DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				jobDetailsMap.put(PREV_FIRE_TIME, fmt.format(instant.atZone(ZoneId.systemDefault())));
			}
		} else {
			jobDetailsMap.put(PREV_FIRE_TIME, "INACTIVE");
		}

		// add to the job map
		JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
		jobMap.put(jobKey.toString(), jobDetailsMap);
	}

	public static void executeAllTriggerOnLoads() {
		if(ClusterUtil.IS_CLUSTERED_SCHEDULER) {
			return;
		}
		
		Connection connection = connectToScheduler();
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		ResultSet result = null;

		try (PreparedStatement preparedStatement = connection
				.prepareStatement("SELECT * FROM SMSS_JOB_RECIPES WHERE TRIGGER_ON_LOAD=?")) {
			preparedStatement.setBoolean(1, true);
			result = preparedStatement.executeQuery();

			while (result.next()) {
				String jobId= result.getString(JOB_ID);
				String jobName = result.getString(JOB_NAME);
				String jobGroup = result.getString(JOB_GROUP);
				JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
				logger.info("Triggering job on startup " + jobName);
				scheduler.triggerJob(jobKey);
				
			}

			logger.info("All trigger on load jobs executed successfully");
		} catch (SQLException sqe) {
			logger.error(Constants.STACKTRACE, sqe);
		} catch (SchedulerException se) {
			logger.error(Constants.STACKTRACE, se);
		} finally {
			try {
				if (result != null) {
					result.close();
				}
			} catch (SQLException sqe) {
				logger.error(Constants.STACKTRACE, sqe);
			}
		}
	}

	private static Blob stringToBlob(Connection connection, String blobInput) {
		Blob blob = null;

		try {
			blob = connection.createBlob();
			blob.setBytes(1, blobInput.getBytes());
		} catch (SQLException se) {
			logger.error("Failed to convert string to blob...");
			logger.error(Constants.STACKTRACE, se);
		}

		return blob;
	}

	public static void startScheduler(Scheduler scheduler) {
		try {
			logger.info("Scheduler starting up...");
			if(!scheduler.isStarted()) {
				scheduler.start();
			}
			logger.info("Scheduler started at " + new Date());
		} catch (SchedulerException se) {
			logger.error("Failed to start scheduler...");
			logger.error(Constants.STACKTRACE, se);
		}
	}

	public static void validateInput(String jobName, String jobGroup, String cronExpression) {
		if (jobName == null || jobName.length() <= 0) {
			throw new IllegalArgumentException("Must provide job name");
		}

		if (jobGroup == null || jobGroup.length() <= 0) {
			throw new IllegalArgumentException("Must provide job group");
		}

		if (!CronExpression.isValidExpression(cronExpression)) {
			throw new IllegalArgumentException("Must provide a valid cron expression!");
		}
	}

	public static String validateAndDecodeRecipe(String recipe) {
		if (recipe == null || recipe.length() <= 0) {
			throw new IllegalArgumentException("Must provide a recipe");
		}

		try {
			recipe = URLDecoder.decode(recipe, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new IllegalArgumentException("Must be able to decode recipe");
		}

		return recipe;
	}
	
	public static String validateAndDecodeRecipeParameters(String recipeParameters) {
		if(recipeParameters == null || recipeParameters.isEmpty()) {
			return null;
		}
		try {
			recipeParameters = URLDecoder.decode(recipeParameters, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new IllegalArgumentException("Must be able to decode recipe");
		}

		return recipeParameters;
	}

	private static void createQuartzTables(Connection connection, String schema) {
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowBooleanDataType = queryUtil.allowBooleanDataType();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();

		String[] colNames = null;
		String[] types = null;
		Object[] constraints = null;

		try {
			// QRTZ_CALENDARS
			colNames = new String[] { SCHED_NAME, CALENDAR_NAME, CALENDAR };
			types = new String[] { VARCHAR_120, VARCHAR_200, IMAGE };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
 			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL };
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_CALENDARS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_CALENDARS, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_CALENDARS, colNames, types, constraints));
				}
			}
	
			// QRTZ_CRON_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, CRON_EXPRESSION, TIME_ZONE_ID };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_120, VARCHAR_80 };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_CRON_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_CRON_TRIGGERS, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_CRON_TRIGGERS, colNames, types, constraints));
				}
			}
	
			// QRTZ_FIRED_TRIGGERS
			colNames = new String[] { SCHED_NAME, ENTRY_ID, TRIGGER_NAME, TRIGGER_GROUP, INSTANCE_NAME, FIRED_TIME,
					SCHED_TIME, PRIORITY, STATE, JOB_NAME, JOB_GROUP, IS_NONCONCURRENT, REQUESTS_RECOVERY };
			types = new String[] { VARCHAR_120, VARCHAR_95, VARCHAR_200, VARCHAR_200, VARCHAR_200, BIGINT, BIGINT, INTEGER,
					VARCHAR_16, VARCHAR_200, VARCHAR_200, BOOLEAN, BOOLEAN };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL,
					NOT_NULL, null, null, null, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_FIRED_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_FIRED_TRIGGERS, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_FIRED_TRIGGERS, colNames, types, constraints));
				}
			}
	
			// QRTZ_PAUSED_TRIGGER_GRPS
			colNames = new String[] { SCHED_NAME, TRIGGER_GROUP };
			types = new String[] { VARCHAR_120, VARCHAR_200 };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_PAUSED_TRIGGER_GRPS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_PAUSED_TRIGGER_GRPS, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_PAUSED_TRIGGER_GRPS, colNames,
							types, constraints));
				}
			}
	
			// QRTZ_SCHEDULER_STATE
			colNames = new String[] { SCHED_NAME, INSTANCE_NAME, LAST_CHECKIN_TIME, CHECKIN_INTERVAL };
			types = new String[] { VARCHAR_120, VARCHAR_200, BIGINT, BIGINT };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SCHEDULER_STATE, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SCHEDULER_STATE, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_SCHEDULER_STATE, colNames, types, constraints));
				}
			}
	
			// QRTZ_LOCKS
			colNames = new String[] { SCHED_NAME, LOCK_NAME };
			types = new String[] { VARCHAR_120, VARCHAR_40 };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(
						queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_LOCKS, colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_LOCKS, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_LOCKS, colNames, types, constraints));
				}
			}
	
			// QRTZ_JOB_DETAILS
			colNames = new String[] { SCHED_NAME, JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE,
					IS_NONCONCURRENT, IS_UPDATE_DATA, REQUESTS_RECOVERY, JOB_DATA };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_250, VARCHAR_250, BOOLEAN, BOOLEAN,
					BOOLEAN, BOOLEAN, IMAGE };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL,
					NOT_NULL, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_JOB_DETAILS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_JOB_DETAILS, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_JOB_DETAILS, colNames, types, constraints));
				}
			}
	
			// QRTZ_SIMPLE_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, REPEAT_COUNT, REPEAT_INTERVAL,
					TIMES_TRIGGERED };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, BIGINT, BIGINT, BIGINT };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SIMPLE_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SIMPLE_TRIGGERS, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_SIMPLE_TRIGGERS, colNames, types, constraints));
				}
			}
	
			// QRTZ_SIMPROP_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, STR_PROP_1, STR_PROP_2, STR_PROP_3,
					INT_PROP_1, INT_PROP_2, LONG_PROP_1, LONG_PROP_2, DEC_PROP_1, DEC_PROP_2, BOOL_PROP_1, BOOL_PROP_2 };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_512, VARCHAR_512, VARCHAR_512, INTEGER,
					INTEGER, BIGINT, BIGINT, NUMERIC_13_4, NUMERIC_13_4, BOOLEAN, BOOLEAN };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null, null, null, null, null, null,
					null, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SIMPROP_TRIGGERS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SIMPROP_TRIGGERS, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_SIMPROP_TRIGGERS, colNames,
							types, constraints));
				}
			}
	
			// QRTZ_BLOB_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, BLOB_DATA };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, IMAGE };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_BLOB_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_BLOB_TRIGGERS, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_BLOB_TRIGGERS, colNames, types, constraints));
				}
			}
	
			// QRTZ_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, JOB_NAME, JOB_GROUP, DESCRIPTION,
					NEXT_FIRE_TIME, PREV_FIRE_TIME, PRIORITY, TRIGGER_STATE, TRIGGER_TYPE, START_TIME, END_TIME,
					CALENDAR_NAME, MISFIRE_INSTR, JOB_DATA };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_200, VARCHAR_200, VARCHAR_250, BIGINT,
					BIGINT, INTEGER, VARCHAR_16, VARCHAR_8, BIGINT, BIGINT, VARCHAR_200, SMALLINT, IMAGE };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null, NOT_NULL,
					NOT_NULL, NOT_NULL, null, null, null, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(
						queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_TRIGGERS, colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_TRIGGERS, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_TRIGGERS, colNames, types, constraints));
				}
			}
		} catch (SQLException se) {
			logger.error(Constants.STACKTRACE, se);
		}
	}

	private static void createSemossTables(Connection connection, String schema) {
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowBooleanDataType = queryUtil.allowBooleanDataType();
		boolean allowBlobDataType = queryUtil.allowBlobDataType();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		String dateTimeType = queryUtil.getDateWithTimeDataType();
		final String BLOB_DATATYPE_NAME = queryUtil.getBlobDataTypeName();
		
		String[] colNames = null;
		String[] types = null;
		Object[] constraints = null;

		try {
			// SMSS_JOB_RECIPES
			colNames = new String[] { USER_ID, JOB_ID, JOB_NAME, JOB_GROUP, CRON_EXPRESSION, PIXEL_RECIPE, PIXEL_RECIPE_PARAMETERS, JOB_CATEGORY, TRIGGER_ON_LOAD, PARAMETERS };
			types = new String[] { VARCHAR_120, VARCHAR_200,VARCHAR_200, VARCHAR_200, VARCHAR_250, BLOB_DATATYPE_NAME, BLOB_DATATYPE_NAME, VARCHAR_200, BOOLEAN, BLOB_DATATYPE_NAME };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null, null, null };

			if (allowIfExistsTable) {
				schedulerDb.insertData(
						queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_JOB_RECIPES, colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_JOB_RECIPES, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(SMSS_JOB_RECIPES, colNames, types, constraints));
				}
			}

			// ADDED 2020-08-21
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// since we added the pixel recipe parameters at a later point...
				if(!queryUtil.getTableColumns(connection, SMSS_JOB_RECIPES, schema).contains(PIXEL_RECIPE_PARAMETERS)) {
					// alter table to add the column
					schedulerDb.insertData(queryUtil.alterTableAddColumn(SMSS_JOB_RECIPES, PIXEL_RECIPE_PARAMETERS, BLOB_DATATYPE_NAME));
				}
			}

			// ADDED 2020-11-30
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// need to add new JobId
				if(!queryUtil.getTableColumns(connection, SMSS_JOB_RECIPES, schema).contains(JOB_ID)) {
					// alter table to add the column
					schedulerDb.execUpdateAndRetrieveStatement(queryUtil.alterTableAddColumnWithDefault("SMSS_JOB_RECIPES", "JOB_ID", "VARCHAR(200)", "PLACEHOLDER"), true);
					// make the JOB_ID the JOB_NAME for LEGACY recipes
					schedulerDb.execUpdateAndRetrieveStatement("UPDATE SMSS_JOB_RECIPES SET JOB_ID=JOB_NAME", true);
					// add constraints on job id column
					schedulerDb.execUpdateAndRetrieveStatement(queryUtil.modColumnNotNull("SMSS_JOB_RECIPES", "JOB_ID", "VARCHAR(2000)"), true);
					schedulerDb.execUpdateAndRetrieveStatement("ALTER TABLE SMSS_JOB_RECIPES ADD CONSTRAINT SMSS_JOB_RECIPES_PK PRIMARY KEY (JOB_ID)", true);
				}
			}
			
			// SMSS_JOB_TAGS
			colNames = new String[]{JOB_ID, JOB_TAG};
			types = new String[]{VARCHAR_200, VARCHAR_200};
			constraints = new String[] { NOT_NULL, NOT_NULL };
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_JOB_TAGS, colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_JOB_TAGS, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(SMSS_JOB_TAGS, colNames, types, constraints));
				}
			}

			// SMSS_AUDIT_TRAIL
			colNames = new String[] { JOB_ID, JOB_GROUP, EXECUTION_START, EXECUTION_END, EXECUTION_DELTA, SUCCESS };
			types = new String[] { VARCHAR_200, VARCHAR_200, TIMESTAMP, TIMESTAMP, VARCHAR_255, BOOLEAN };
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			if(!dateTimeType.equals(TIMESTAMP)) { types = cleanUpDataType(types, TIMESTAMP, dateTimeType); };
			constraints = new String[] { NOT_NULL, NOT_NULL, null, null, null, null, null };
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_AUDIT_TRAIL, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_AUDIT_TRAIL, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(SMSS_AUDIT_TRAIL, colNames, types, constraints));
				}
			}

			// ADDED 2020-11-30
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// need to add new JobId
				if(!queryUtil.getTableColumns(connection, SMSS_AUDIT_TRAIL, schema).contains(JOB_ID)) {
					// change JOB_NAME to JOB_ID
					schedulerDb.execUpdateAndRetrieveStatement(queryUtil.modColumnName("SMSS_AUDIT_TRAIL", "JOB_NAME", "JOB_ID"), true);
				}
			}

			// SMSS_EXECUTION_SCHEDULE
			colNames = new String[] { EXEC_ID, JOB_ID, JOB_GROUP};
			types = new String[] { VARCHAR_200, VARCHAR_200, VARCHAR_200};
			if(!allowBooleanDataType) { types = cleanUpBooleans(types); };
			if(!dateTimeType.equals(TIMESTAMP)) { types = cleanUpDataType(types, TIMESTAMP, dateTimeType); };
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExists(SMSS_EXECUTION, colNames, types));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_EXECUTION, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTable(SMSS_EXECUTION, colNames, types));
				}
			}

			// ADDED 2020-11-30
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// need to add new JobId
				if(!queryUtil.getTableColumns(connection, SMSS_EXECUTION, schema).contains(JOB_ID)) {
					// change JOB_NAME to JOB_ID
					schedulerDb.execUpdateAndRetrieveStatement(queryUtil.modColumnName("SMSS_EXECUTION", "JOB_NAME", "JOB_ID"), true);
				}
			}
		} catch (SQLException se) {
			logger.error(Constants.STACKTRACE, se);
		}
	}
	
	/**
	 * Clean up boolean for bit data type
	 * @param arrays
	 * @return
	 */
	private static String[] cleanUpBooleans(String[] arrays) {
		for(int i = 0; i < arrays.length; i++) {
			if(arrays[i].equals(BOOLEAN)) {
				arrays[i] = BIT;
			}
		}
		return arrays;
	}

	/**
	 * Clean up blob data types
	 * @param arrays
	 * @return
	 */
	private static String[] cleanUpDataType(String[] arrays, String value, String replacement) {
		for(int i = 0; i < arrays.length; i++) {
			if(arrays[i].equals(value)) {
				arrays[i] = replacement;
			}
		}
		return arrays;
	}
	
	private static void addAllPrimaryKeys() {
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		if(queryUtil.allowIfExistsAddConstraint()) {
			String query1 = "ALTER TABLE QRTZ_CALENDARS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_CALENDARS PRIMARY KEY ( SCHED_NAME, CALENDAR_NAME);";
			String query2 = "ALTER TABLE QRTZ_CRON_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_CRON_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query3 = "ALTER TABLE QRTZ_FIRED_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_FIRED_TRIGGERS PRIMARY KEY ( SCHED_NAME, ENTRY_ID );";
			String query4 = "ALTER TABLE QRTZ_PAUSED_TRIGGER_GRPS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_PAUSED_TRIGGER_GRPS PRIMARY KEY ( SCHED_NAME, TRIGGER_GROUP );";
			String query5 = "ALTER TABLE QRTZ_SCHEDULER_STATE ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_SCHEDULER_STATE PRIMARY KEY ( SCHED_NAME, INSTANCE_NAME );";
			String query6 = "ALTER TABLE QRTZ_LOCKS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_LOCKS PRIMARY KEY ( SCHED_NAME, LOCK_NAME );";
			String query7 = "ALTER TABLE QRTZ_JOB_DETAILS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_JOB_DETAILS PRIMARY KEY ( SCHED_NAME, JOB_NAME, JOB_GROUP );";
			String query8 = "ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_SIMPLE_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query9 = "ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_SIMPROP_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query10 = "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";

			try {
				schedulerDb.insertData(query1);
				schedulerDb.insertData(query2);
				schedulerDb.insertData(query3);
				schedulerDb.insertData(query4);
				schedulerDb.insertData(query5);
				schedulerDb.insertData(query6);
				schedulerDb.insertData(query7);
				schedulerDb.insertData(query8);
				schedulerDb.insertData(query9);
				schedulerDb.insertData(query10);
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
		} else {
			String query1 = "ALTER TABLE QRTZ_CALENDARS ADD CONSTRAINT PK_QRTZ_CALENDARS PRIMARY KEY ( SCHED_NAME, CALENDAR_NAME);";
			String query2 = "ALTER TABLE QRTZ_CRON_TRIGGERS ADD CONSTRAINT PK_QRTZ_CRON_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query3 = "ALTER TABLE QRTZ_FIRED_TRIGGERS ADD CONSTRAINT PK_QRTZ_FIRED_TRIGGERS PRIMARY KEY ( SCHED_NAME, ENTRY_ID );";
			String query4 = "ALTER TABLE QRTZ_PAUSED_TRIGGER_GRPS ADD CONSTRAINT PK_QRTZ_PAUSED_TRIGGER_GRPS PRIMARY KEY ( SCHED_NAME, TRIGGER_GROUP );";
			String query5 = "ALTER TABLE QRTZ_SCHEDULER_STATE ADD CONSTRAINT PK_QRTZ_SCHEDULER_STATE PRIMARY KEY ( SCHED_NAME, INSTANCE_NAME );";
			String query6 = "ALTER TABLE QRTZ_LOCKS ADD CONSTRAINT PK_QRTZ_LOCKS PRIMARY KEY ( SCHED_NAME, LOCK_NAME );";
			String query7 = "ALTER TABLE QRTZ_JOB_DETAILS ADD CONSTRAINT PK_QRTZ_JOB_DETAILS PRIMARY KEY ( SCHED_NAME, JOB_NAME, JOB_GROUP );";
			String query8 = "ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD CONSTRAINT PK_QRTZ_SIMPLE_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query9 = "ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD CONSTRAINT PK_QRTZ_SIMPROP_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query10 = "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT PK_QRTZ_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";

			try {
				schedulerDb.insertData(query1);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query2);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query3);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query4);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query5);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query6);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query7);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query8);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query9);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query10);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
		}
	}

	private static void addAllForeignKeys() {
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		if(queryUtil.allowIfExistsAddConstraint()) {
			String query1 = "ALTER TABLE QRTZ_CRON_TRIGGERS ADD CONSTRAINT IF NOT EXISTS FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query2 = "ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD CONSTRAINT IF NOT EXISTS FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query3 = "ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD CONSTRAINT IF NOT EXISTS FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query4 = "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT IF NOT EXISTS FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS FOREIGN KEY ( SCHED_NAME, JOB_NAME, JOB_GROUP ) REFERENCES QRTZ_JOB_DETAILS ( SCHED_NAME, JOB_NAME, JOB_GROUP );";
	
			try {
				schedulerDb.insertData(query1);
				schedulerDb.insertData(query2);
				schedulerDb.insertData(query3);
				schedulerDb.insertData(query4);
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
		} else {
			String query1 = "ALTER TABLE QRTZ_CRON_TRIGGERS ADD CONSTRAINT FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query2 = "ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD CONSTRAINT FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query3 = "ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD CONSTRAINT FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query4 = "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS FOREIGN KEY ( SCHED_NAME, JOB_NAME, JOB_GROUP ) REFERENCES QRTZ_JOB_DETAILS ( SCHED_NAME, JOB_NAME, JOB_GROUP );";

			try {
				schedulerDb.insertData(query1);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query2);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query3);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
			try {
				schedulerDb.insertData(query4);
			} catch (SQLException se) {
//				logger.error(Constants.STACKTRACE, se);
			}
		}
	}
}
