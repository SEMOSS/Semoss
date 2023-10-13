package prerna.reactor.scheduler;

import static prerna.reactor.scheduler.SchedulerConstants.BIGINT;
import static prerna.reactor.scheduler.SchedulerConstants.BLOB_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.BOOL_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.BOOL_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.CALENDAR;
import static prerna.reactor.scheduler.SchedulerConstants.CALENDAR_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.CHECKIN_INTERVAL;
import static prerna.reactor.scheduler.SchedulerConstants.CRON_EXPRESSION;
import static prerna.reactor.scheduler.SchedulerConstants.CRON_TIMEZONE;
import static prerna.reactor.scheduler.SchedulerConstants.DEC_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.DEC_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.DESCRIPTION;
import static prerna.reactor.scheduler.SchedulerConstants.END_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.ENTRY_ID;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_DELTA;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_END;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_START;
import static prerna.reactor.scheduler.SchedulerConstants.EXEC_ID;
import static prerna.reactor.scheduler.SchedulerConstants.FIRED_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.INSTANCE_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.INTEGER;
import static prerna.reactor.scheduler.SchedulerConstants.INT_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.INT_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.IS_DURABLE;
import static prerna.reactor.scheduler.SchedulerConstants.IS_LATEST;
import static prerna.reactor.scheduler.SchedulerConstants.IS_NONCONCURRENT;
import static prerna.reactor.scheduler.SchedulerConstants.IS_UPDATE_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_CATEGORY;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_CLASS_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_GROUP;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_ID;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_TAG;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_TAGS;
import static prerna.reactor.scheduler.SchedulerConstants.LAST_CHECKIN_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.LOCK_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.LONG_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.LONG_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.MISFIRE_INSTR;
import static prerna.reactor.scheduler.SchedulerConstants.NEXT_FIRE_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.NOT_NULL;
import static prerna.reactor.scheduler.SchedulerConstants.NUMERIC_13_4;
import static prerna.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE;
import static prerna.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE_PARAMETERS;
import static prerna.reactor.scheduler.SchedulerConstants.PREV_FIRE_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.PRIORITY;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_BLOB_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_CALENDARS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_CRON_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_FIRED_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_JOB_DETAILS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_LOCKS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_PAUSED_TRIGGER_GRPS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SCHEDULER_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SIMPLE_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SIMPROP_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.REPEAT_COUNT;
import static prerna.reactor.scheduler.SchedulerConstants.REPEAT_INTERVAL;
import static prerna.reactor.scheduler.SchedulerConstants.REQUESTS_RECOVERY;
import static prerna.reactor.scheduler.SchedulerConstants.SCHEDULER_OUTPUT;
import static prerna.reactor.scheduler.SchedulerConstants.SCHED_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.SCHED_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.SMALLINT;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_AUDIT_TRAIL;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_EXECUTION;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_JOB_RECIPES;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_JOB_TAGS;
import static prerna.reactor.scheduler.SchedulerConstants.START_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.STATE;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_3;
import static prerna.reactor.scheduler.SchedulerConstants.SUCCESS;
import static prerna.reactor.scheduler.SchedulerConstants.TIMESTAMP;
import static prerna.reactor.scheduler.SchedulerConstants.TIMES_TRIGGERED;
import static prerna.reactor.scheduler.SchedulerConstants.TIME_ZONE_ID;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_GROUP;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_ON_LOAD;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_TYPE;
import static prerna.reactor.scheduler.SchedulerConstants.UI_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.USER_ID;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_120;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_16;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_200;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_250;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_255;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_40;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_512;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_8;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_80;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_95;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronExpression;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class SchedulerDatabaseUtility {
	
	private static final Logger logger = LogManager.getLogger(SchedulerDatabaseUtility.class);
	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/**
	 * SELECT SMSS_JOB_RECIPES.USER_ID, SMSS_JOB_RECIPES.JOB_ID, SMSS_JOB_RECIPES.JOB_NAME, SMSS_JOB_RECIPES.JOB_GROUP, SMSS_JOB_RECIPES.CRON_EXPRESSION,
	 * SMSS_JOB_RECIPES.PIXEL_RECIPE, SMSS_JOB_RECIPES.PIXEL_RECIPE_PARAMETERS, SMSS_JOB_RECIPES.UI_STATE, QRTZ_TRIGGERS.NEXT_FIRE_TIME, QRTZ_TRIGGERS.PREV_FIRE_TIME
	 * FROM SMSS_JOB_RECIPES LEFT OUTER JOIN QRTZ_TRIGGERS ON SMSS_JOB_RECIPES.JOB_NAME = QRTZ_TRIGGERS.JOB_NAME AND SMSS_JOB_RECIPES.JOB_GROUP = QRTZ_TRIGGERS.JOB_GROUP
	 */
	private static final String BASE_JOB_DETAILS_QUERY = "SELECT SMSS_JOB_RECIPES.USER_ID, "
			+ "SMSS_JOB_RECIPES.JOB_ID, "
			+ "SMSS_JOB_RECIPES.JOB_NAME, "
			+ "SMSS_JOB_RECIPES.JOB_GROUP, "
			+ "SMSS_JOB_RECIPES.CRON_EXPRESSION, "
			+ "SMSS_JOB_RECIPES.CRON_TIMEZONE, "
			+ "SMSS_JOB_RECIPES.PIXEL_RECIPE, "
			+ "SMSS_JOB_RECIPES.PIXEL_RECIPE_PARAMETERS, "
			+ "SMSS_JOB_RECIPES.UI_STATE, "
			+ "QRTZ_TRIGGERS.NEXT_FIRE_TIME, "
			// fetching the execution start time value from audit trail table 
			+ "SMSS_AUDIT_TRAIL.EXECUTION_START, "
			+ "QRTZ_TRIGGERS.TRIGGER_STATE";

	private static final String JOIN_JOB_DETAILS_QUERY = "LEFT OUTER JOIN QRTZ_TRIGGERS ON "
			+ "SMSS_JOB_RECIPES.JOB_ID = QRTZ_TRIGGERS.JOB_NAME "
			+ "AND SMSS_JOB_RECIPES.JOB_GROUP = QRTZ_TRIGGERS.JOB_GROUP "
			// added join on audit trail table to fetch the previous run time based on is_latest 
			+ "LEFT OUTER JOIN SMSS_AUDIT_TRAIL ON SMSS_JOB_RECIPES.JOB_ID = SMSS_AUDIT_TRAIL.JOB_ID "
			+ "AND SMSS_AUDIT_TRAIL.IS_LATEST=? ";

	static RDBMSNativeEngine schedulerDb;
	static AbstractSqlQueryUtil queryUtil;

	private SchedulerDatabaseUtility() {
		throw new IllegalStateException("Utility class");
	}

	public static void startServer() throws Exception {
		schedulerDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SCHEDULER_DB);
		Connection conn = schedulerDb.getConnection();
		try {
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
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}

	public static void initialize() throws SQLException {
		String database = schedulerDb.getDatabase();
		String schema = schedulerDb.getSchema();
		Connection conn = schedulerDb.getConnection();
		try {
			createQuartzTables(conn, database, schema);
			createSemossTables(conn, database, schema);
			addAllPrimaryKeys(conn, database, schema);
			addAllForeignKeys(conn, database, schema);
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}

	public static Connection connectToScheduler() {
		Connection connection = null;

		try {
			schedulerDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SCHEDULER_DB);
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
	
	/**
	 * 
	 * @param execId
	 * @param jobId
	 * @param jobGroup
	 * @return
	 */
	public static boolean insertIntoExecutionTable(String execId, String jobId, String jobGroup) {
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn
				.prepareStatement("INSERT INTO SMSS_EXECUTION (EXEC_ID, JOB_ID, JOB_GROUP) VALUES (?,?,?)")) {
			statement.setString(1, execId);
			statement.setString(2, jobId);
			statement.setString(3, jobGroup);
			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return true;
	}
	
	/**
	 * 
	 * @param execId
	 * @return
	 */
	public static String[] executionIdExists(String execId) {
		Connection conn = connectToScheduler();
		ResultSet rs = null;
		try (PreparedStatement statement = conn
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
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return null;
	}
	
	/**
	 * 
	 * @param execId
	 * @return
	 */
	public static boolean removeExecutionId(String execId) {
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn
				.prepareStatement("DELETE FROM SMSS_EXECUTION WHERE EXEC_ID = ?")) {
			statement.setString(1, execId);
			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return true;
	}

	/**
	 * 
	 * @param jobId
	 * @param jobGroup
	 * @param start
	 * @param end
	 * @param success
	 * @param schedulerOutput
	 * @return
	 */
	public static boolean insertIntoAuditTrailTable(String jobId, String jobGroup, Long start, Long end, boolean success, String schedulerOutput) {
		Connection conn = connectToScheduler();
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();

		Timestamp startTimeStamp = new Timestamp(start);
		Timestamp endTimeStamp = new Timestamp(end);
		
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		// update is_latest to false for all the existing records of this job id
		try {
			try(PreparedStatement updateAuditTrailStatement = conn
					.prepareStatement("UPDATE SMSS_AUDIT_TRAIL SET IS_LATEST=? WHERE JOB_ID=?")){
				updateAuditTrailStatement.setBoolean(1, false);
				updateAuditTrailStatement.setString(2, jobId);
				updateAuditTrailStatement.executeUpdate();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
				return false;
			}
			// now insert the new record with is_latest as true
			try (PreparedStatement statement = conn
					.prepareStatement("INSERT INTO SMSS_AUDIT_TRAIL (JOB_ID, JOB_GROUP, EXECUTION_START, EXECUTION_END, EXECUTION_DELTA, SUCCESS, IS_LATEST, SCHEDULER_OUTPUT) VALUES (?,?,?,?,?,?,?,?)")) {
				int index = 1;
				statement.setString(index++, jobId);
				statement.setString(index++, jobGroup);
				statement.setTimestamp(index++, startTimeStamp, cal);
				statement.setTimestamp(index++, endTimeStamp, cal);
				statement.setString(index++, String.valueOf(end - start));
				statement.setBoolean(index++, success);
				statement.setBoolean(index++, true);
				queryUtil.handleInsertionOfClob(conn, statement, schedulerOutput, index++, gson);
				statement.executeUpdate();
			} catch (UnsupportedEncodingException | SQLException e) {
				logger.error(Constants.STACKTRACE, e);
				return false;
			}
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return true;
	}

	/**
	 * 
	 * @param userId
	 * @param jobId
	 * @param jobName
	 * @param jobGroup
	 * @param cronExpression
	 * @param cronTimezone
	 * @param recipe
	 * @param recipeParameters
	 * @param jobCategory
	 * @param triggerOnLoad
	 * @param uiState
	 * @param jobTags
	 * @return
	 */
	public static boolean insertIntoJobRecipesTable(String userId, String jobId, 
			String jobName, String jobGroup, 
			String cronExpression, TimeZone cronTimeZone,
			String recipe, String recipeParameters,
			String jobCategory, boolean triggerOnLoad, String uiState, List<String> jobTags ) {
		
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn
				.prepareStatement("INSERT INTO SMSS_JOB_RECIPES (USER_ID, JOB_ID, JOB_NAME, JOB_GROUP, CRON_EXPRESSION, CRON_TIMEZONE, PIXEL_RECIPE, PIXEL_RECIPE_PARAMETERS, JOB_CATEGORY, TRIGGER_ON_LOAD, UI_STATE) VALUES (?,?,?,?,?,?,?,?,?,?,?)")) {
			int index = 1;
			statement.setString(index++, userId);
			statement.setString(index++, jobId);
			statement.setString(index++, jobName);
			statement.setString(index++, jobGroup);
			statement.setString(index++, cronExpression);
			statement.setString(index++, cronTimeZone.getDisplayName());
			queryUtil.handleInsertionOfBlob(conn, statement, recipe, index++);
			queryUtil.handleInsertionOfBlob(conn, statement, recipeParameters, index++);
			statement.setString(index++, jobCategory);
			statement.setBoolean(index++, triggerOnLoad);
			queryUtil.handleInsertionOfBlob(conn, statement, uiState, index++);
			
			statement.executeUpdate();
		} catch (SQLException | UnsupportedEncodingException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
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
		Connection conn = connectToScheduler();

		try {
			// first we delete old tags
			try (PreparedStatement statement = conn.prepareStatement("DELETE FROM SMSS_JOB_TAGS WHERE JOB_ID=?")) {
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
			try (PreparedStatement statement = conn.prepareStatement("INSERT INTO SMSS_JOB_TAGS (JOB_ID, JOB_TAG) VALUES (?,?)")) {
				for(String jobTag : jobTags) {
					statement.setString(1, jobId);
					statement.setString(2, jobTag.trim());
					statement.addBatch();
				}
				statement.executeBatch();
			} catch( SQLException e) {
				logger.error(Constants.STACKTRACE, e);
				return false;
			}
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return true;
	}
	
	/**
	 * 
	 * @param userId
	 * @param jobId
	 * @param jobName
	 * @param jobGroup
	 * @param cronExpression
	 * @param cronTimeZone
	 * @param recipe
	 * @param recipeParameters
	 * @param jobCategory
	 * @param triggerOnLoad
	 * @param uiState
	 * @param existingJobName
	 * @param existingJobGroup
	 * @param jobTags
	 * @return
	 */
	public static boolean updateJobRecipesTable(String userId, String jobId, 
			String jobName, String jobGroup, 
			String cronExpression, TimeZone cronTimeZone, 
			String recipe, String recipeParameters,
			String jobCategory, boolean triggerOnLoad, 
			String uiState, String existingJobName, String existingJobGroup, 
			List<String> jobTags) {
		
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn
				.prepareStatement("UPDATE SMSS_JOB_RECIPES SET USER_ID = ?, JOB_NAME = ?, JOB_GROUP = ?, CRON_EXPRESSION = ?, CRON_TIMEZONE= ?, PIXEL_RECIPE = ?, "
						+ "PIXEL_RECIPE_PARAMETERS = ?, JOB_CATEGORY = ?, TRIGGER_ON_LOAD = ?, UI_STATE = ? WHERE JOB_ID = ? AND JOB_GROUP = ?")) {
			int index = 1;
			statement.setString(index++, userId);
			statement.setString(index++, jobName);
			statement.setString(index++, jobGroup);
			statement.setString(index++, cronExpression);
			statement.setString(index++, cronTimeZone.getDisplayName());
			queryUtil.handleInsertionOfBlob(conn, statement, recipe, index++);
			queryUtil.handleInsertionOfBlob(conn, statement, recipeParameters, index++);
			statement.setString(index++, jobCategory);
			statement.setBoolean(index++, triggerOnLoad);
			queryUtil.handleInsertionOfBlob(conn, statement, uiState, index++);

			// where clause filters
			statement.setString(index++, jobId);
			statement.setString(index++, existingJobGroup);
			
			statement.executeUpdate();
		} catch (SQLException | UnsupportedEncodingException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return updateJobTags(jobId, jobTags);
	}

	/**
	 * 
	 * @param jobId
	 * @param jobGroup
	 * @return
	 */
	public static boolean removeFromJobRecipesTable(String jobId , String jobGroup) {
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn
				.prepareStatement("DELETE FROM SMSS_JOB_RECIPES WHERE JOB_ID =? AND JOB_GROUP=?")) {
			statement.setString(1, jobId);
			statement.setString(2, jobGroup);

			statement.executeUpdate();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return true;
	}

	public static boolean existsInJobRecipesTable(String jobId, String jobGroup) {
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn
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
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
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

	/**
	 * 
	 * @param appId
	 * @param jobTags
	 * @return
	 */
	public static Map<String, Map<String, String>> retrieveJobsForProject(String appId, List<String> jobTags ) {
		Connection conn = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();

		try (PreparedStatement statement = conn
				.prepareStatement(createJobQuery("WHERE SMSS_JOB_RECIPES.JOB_GROUP=?",jobTags))) {
			// always have the is_latest value
			statement.setBoolean(1, true);
			statement.setString(2, appId);

			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return jobMap;
	}

	/**
	 * 
	 * @param appId
	 * @param userId
	 * @param jobTags
	 * @return
	 */
	public static Map<String, Map<String, String>> retrieveUsersJobsForProject(String appId, String userId, List<String> jobTags) {
		Connection conn = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		try (PreparedStatement statement = conn
				.prepareStatement(createJobQuery(" WHERE SMSS_JOB_RECIPES.USER_ID=? AND SMSS_JOB_RECIPES.JOB_GROUP=?",jobTags))) {
			// always have the is_latest value
			statement.setBoolean(1, true);
			statement.setString(2, userId);
			statement.setString(3, appId);

			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return jobMap;
	}

	/**
	 * 
	 * @param userId
	 * @param jobTags
	 * @return
	 */
	public static Map<String, Map<String, String>> retrieveUsersJobs(String userId, List<String> jobTags) {
		Connection conn = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		try (PreparedStatement statement = conn
				.prepareStatement(createJobQuery(" WHERE SMSS_JOB_RECIPES.USER_ID=?",jobTags))) {
			// always have the is_latest value
			statement.setBoolean(1, true);
			statement.setString(2, userId);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
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

	/**
	 * 
	 * @param jobTags
	 * @return
	 */
	public static Map<String, Map<String, String>> retrieveAllJobs(List<String> jobTags) {
		Connection conn = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		String query = createJobQuery(null, jobTags);
		try (PreparedStatement statement = conn.prepareStatement(query)) {
			// always have the is_latest value
			statement.setBoolean(1, true);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
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
		String cronTimeZone = result.getString(CRON_TIMEZONE);
		String recipe = null;
		String recipeParameters = null;
		String uiState = null;
		BigInteger nextExecTime = null;
		BigDecimal nExecTimeD = result.getBigDecimal(NEXT_FIRE_TIME);
		String tiggerState = result.getString(TRIGGER_STATE);
		Timestamp previousRun = result.getTimestamp(EXECUTION_START);
		
		if(nExecTimeD != null) {
			nextExecTime = nExecTimeD.toBigInteger();
		}
		
		try {
			recipe = queryUtil.handleBlobRetrieval(result, PIXEL_RECIPE);
		} catch (SQLException | IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		try {
			recipeParameters = queryUtil.handleBlobRetrieval(result, PIXEL_RECIPE_PARAMETERS);
		} catch (SQLException | IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		try {
			uiState = queryUtil.handleBlobRetrieval(result, UI_STATE);
		} catch (SQLException | IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		jobDetailsMap.put(USER_ID, userId);
		jobDetailsMap.put(ReactorKeysEnum.JOB_ID.getKey(), jobId);
		jobDetailsMap.put(ReactorKeysEnum.JOB_GROUP.getKey(), jobGroup );
		jobDetailsMap.put(ReactorKeysEnum.JOB_NAME.getKey(), jobName);
		jobDetailsMap.put(ReactorKeysEnum.JOB_TAGS.getKey(), jobTags );
		jobDetailsMap.put(ReactorKeysEnum.CRON_EXPRESSION.getKey(), cronExpression);
		jobDetailsMap.put(ReactorKeysEnum.CRON_TZ.getKey(), cronTimeZone);
		jobDetailsMap.put(ReactorKeysEnum.RECIPE.getKey(), recipe);
		jobDetailsMap.put(ReactorKeysEnum.RECIPE_PARAMETERS.getKey(), recipeParameters);
		jobDetailsMap.put(ScheduleJobReactor.UI_STATE, uiState);
		// setting the prev_fire_time fom the smss audit table
		if(previousRun != null) {
			jobDetailsMap.put(PREV_FIRE_TIME, previousRun.toString());			
		} else {
			jobDetailsMap.put(PREV_FIRE_TIME, "N/A");
		}

		// add next fire time
		if(nextExecTime != null && !tiggerState.equals("PAUSED")) {
			if(nextExecTime.intValue() == -1) {
				jobDetailsMap.put(NEXT_FIRE_TIME, "EXECUTING");
			} else {
				Instant instant = Instant.ofEpochMilli(nextExecTime.longValue());
				DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				jobDetailsMap.put(NEXT_FIRE_TIME, fmt.format(instant.atZone(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()).toZoneId())));
			}
		} else {
			jobDetailsMap.put(NEXT_FIRE_TIME, "INACTIVE");
		}
		// add to the job map
		JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
		jobMap.put(jobKey.toString(), jobDetailsMap);
	}

	/**
	 * 
	 */
	public static void executeAllTriggerOnLoads() {
		if(ClusterUtil.IS_CLUSTERED_SCHEDULER) {
			return;
		}
		
		Connection conn = connectToScheduler();
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		ResultSet result = null;

		try (PreparedStatement preparedStatement = conn
				.prepareStatement("SELECT * FROM SMSS_JOB_RECIPES WHERE TRIGGER_ON_LOAD=?")) {
			preparedStatement.setBoolean(1, true);
			result = preparedStatement.executeQuery();

			while (result.next()) {
				String jobId= result.getString(JOB_ID);
				String jobName = result.getString(JOB_NAME);
				String jobGroup = result.getString(JOB_GROUP);
				JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
				logger.info("Triggering job on startup " + Utility.cleanLogString(jobName));
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
			if(schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * 
	 * @param scheduler
	 */
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

	/**
	 * 
	 * @param jobName
	 * @param jobGroup
	 * @param cronExpression
	 */
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

	/**
	 * 
	 * @param recipe
	 * @return
	 */
	public static String validateAndDecodeRecipe(String recipe) {
		if (recipe == null || recipe.length() <= 0) {
			throw new IllegalArgumentException("Must provide a recipe");
		}

		return Utility.decodeURIComponent(recipe);
	}
	
	/**
	 * 
	 * @param recipeParameters
	 * @return
	 */
	public static String validateAndDecodeRecipeParameters(String recipeParameters) {
		if(recipeParameters == null || recipeParameters.isEmpty()) {
			return null;
		}
		
		return Utility.decodeURIComponent(recipeParameters);
	}

	/**
	 * 
	 * @param connection
	 * @param database
	 * @param schema
	 */
	private static void createQuartzTables(Connection connection, String database, String schema) {
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		final String BOOLEAN_DATATYPE = queryUtil.getBooleanDataTypeName();
		final String IMAGE_DATATYPE = queryUtil.getImageDataTypeName();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		
		String[] colNames = null;
		String[] types = null;
		Object[] constraints = null;

		try {
			// QRTZ_CALENDARS
			colNames = new String[] { SCHED_NAME, CALENDAR_NAME, CALENDAR };
			types = new String[] { VARCHAR_120, VARCHAR_200, IMAGE_DATATYPE };
 			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL };
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_CALENDARS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_CALENDARS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_CALENDARS, colNames, types, constraints));
				}
			}
	
			// QRTZ_CRON_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, CRON_EXPRESSION, TIME_ZONE_ID };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_120, VARCHAR_80 };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_CRON_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_CRON_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_CRON_TRIGGERS, colNames, types, constraints));
				}
			}
	
			// QRTZ_FIRED_TRIGGERS
			colNames = new String[] { SCHED_NAME, ENTRY_ID, TRIGGER_NAME, TRIGGER_GROUP, INSTANCE_NAME, FIRED_TIME,
					SCHED_TIME, PRIORITY, STATE, JOB_NAME, JOB_GROUP, IS_NONCONCURRENT, REQUESTS_RECOVERY };
			types = new String[] { VARCHAR_120, VARCHAR_95, VARCHAR_200, VARCHAR_200, VARCHAR_200, BIGINT, BIGINT, INTEGER,
					VARCHAR_16, VARCHAR_200, VARCHAR_200, BOOLEAN_DATATYPE, BOOLEAN_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL,
					NOT_NULL, null, null, null, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_FIRED_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_FIRED_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_FIRED_TRIGGERS, colNames, types, constraints));
				}
			}
	
			// QRTZ_PAUSED_TRIGGER_GRPS
			colNames = new String[] { SCHED_NAME, TRIGGER_GROUP };
			types = new String[] { VARCHAR_120, VARCHAR_200 };
			constraints = new String[] { NOT_NULL, NOT_NULL };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_PAUSED_TRIGGER_GRPS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_PAUSED_TRIGGER_GRPS, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_PAUSED_TRIGGER_GRPS, colNames,
							types, constraints));
				}
			}
	
			// QRTZ_SCHEDULER_STATE
			colNames = new String[] { SCHED_NAME, INSTANCE_NAME, LAST_CHECKIN_TIME, CHECKIN_INTERVAL };
			types = new String[] { VARCHAR_120, VARCHAR_200, BIGINT, BIGINT };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SCHEDULER_STATE, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SCHEDULER_STATE, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_SCHEDULER_STATE, colNames, types, constraints));
				}
			}
	
			// QRTZ_LOCKS
			colNames = new String[] { SCHED_NAME, LOCK_NAME };
			types = new String[] { VARCHAR_120, VARCHAR_40 };
			constraints = new String[] { NOT_NULL, NOT_NULL };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(
						queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_LOCKS, colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_LOCKS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_LOCKS, colNames, types, constraints));
				}
			}
	
			// QRTZ_JOB_DETAILS
			colNames = new String[] { SCHED_NAME, JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE,
					IS_NONCONCURRENT, IS_UPDATE_DATA, REQUESTS_RECOVERY, JOB_DATA };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_250, VARCHAR_250, BOOLEAN_DATATYPE, BOOLEAN_DATATYPE,
					BOOLEAN_DATATYPE, BOOLEAN_DATATYPE, IMAGE_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL,
					NOT_NULL, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_JOB_DETAILS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_JOB_DETAILS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_JOB_DETAILS, colNames, types, constraints));
				}
			}
	
			// QRTZ_SIMPLE_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, REPEAT_COUNT, REPEAT_INTERVAL,
					TIMES_TRIGGERED };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, BIGINT, BIGINT, BIGINT };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SIMPLE_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SIMPLE_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_SIMPLE_TRIGGERS, colNames, types, constraints));
				}
			}
	
			// QRTZ_SIMPROP_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, STR_PROP_1, STR_PROP_2, STR_PROP_3,
					INT_PROP_1, INT_PROP_2, LONG_PROP_1, LONG_PROP_2, DEC_PROP_1, DEC_PROP_2, BOOL_PROP_1, BOOL_PROP_2 };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_512, VARCHAR_512, VARCHAR_512, INTEGER,
					INTEGER, BIGINT, BIGINT, NUMERIC_13_4, NUMERIC_13_4, BOOLEAN_DATATYPE, BOOLEAN_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null, null, null, null, null, null,
					null, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SIMPROP_TRIGGERS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SIMPROP_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_SIMPROP_TRIGGERS, colNames,
							types, constraints));
				}
			}
	
			// QRTZ_BLOB_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, BLOB_DATA };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, IMAGE_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_BLOB_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_BLOB_TRIGGERS, database, schema)) {
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
					BIGINT, INTEGER, VARCHAR_16, VARCHAR_8, BIGINT, BIGINT, VARCHAR_200, SMALLINT, IMAGE_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null, NOT_NULL,
					NOT_NULL, NOT_NULL, null, null, null, null };
	
			if (allowIfExistsTable) {
				schedulerDb.insertData(
						queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_TRIGGERS, colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_TRIGGERS, colNames, types, constraints));
				}
			}
		} catch (SQLException se) {
			logger.error(Constants.STACKTRACE, se);
		}
	}

	/**
	 * 
	 * @param connection
	 * @param database
	 * @param schema
	 */
	private static void createSemossTables(Connection connection, String database, String schema) {
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowBlobDataType = queryUtil.allowBlobDataType();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		String dateTimeType = queryUtil.getDateWithTimeDataType();
		final String BLOB_DATATYPE = queryUtil.getBlobDataTypeName();
		final String BOOLEAN_DATATYPE = queryUtil.getBooleanDataTypeName();
		final String CLOB_DATATYPE = queryUtil.getClobDataTypeName(); 
		String[] colNames = null;
		String[] types = null;
		Object[] constraints = null;

		try {
			// SMSS_JOB_RECIPES
			colNames = new String[] { USER_ID, JOB_ID, JOB_NAME, JOB_GROUP, CRON_EXPRESSION, CRON_TIMEZONE, PIXEL_RECIPE, PIXEL_RECIPE_PARAMETERS, 
					JOB_CATEGORY, TRIGGER_ON_LOAD, UI_STATE };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_200, VARCHAR_250, VARCHAR_120, BLOB_DATATYPE, BLOB_DATATYPE, 
					VARCHAR_200, BOOLEAN_DATATYPE, BLOB_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null, null, null, null };

			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_JOB_RECIPES, colNames, types, constraints);
				logger.info("Running sql: " + sql);
				schedulerDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_JOB_RECIPES, database, schema)) {
					// make the table
					String sql = queryUtil.createTableWithCustomConstraints(SMSS_JOB_RECIPES, colNames, types, constraints);
					logger.info("Running sql: " + sql);
					schedulerDb.insertData(sql);
				}
			}
			
			// ADDED 2021-02-19
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// since we added the pixel recipe parameters at a later point...
				if(!queryUtil.getTableColumns(connection, SMSS_JOB_RECIPES, database, schema).contains(UI_STATE)) {
					// alter table to add the column
					String sql = queryUtil.alterTableAddColumn(SMSS_JOB_RECIPES, UI_STATE, BLOB_DATATYPE);
					logger.info("Running sql: " + sql);
					schedulerDb.insertData(sql);
					// set it to the value of the previous name "PARAMETER"
					sql = "UPDATE " + SMSS_JOB_RECIPES + " SET " + UI_STATE + "=PARAMETERS";
					logger.info("Running sql: " + sql);
					schedulerDb.insertData(sql);
					// now delete
					sql = queryUtil.alterTableDropColumn(SMSS_JOB_RECIPES, "PARAMETERS");
					logger.info("Running sql: " + sql);
					schedulerDb.removeData(sql);
				}
			}

			// ADDED 2020-08-21
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// since we added the pixel recipe parameters at a later point...
				if(!queryUtil.getTableColumns(connection, SMSS_JOB_RECIPES, database, schema).contains(PIXEL_RECIPE_PARAMETERS)) {
					// alter table to add the column
					String sql = queryUtil.alterTableAddColumn(SMSS_JOB_RECIPES, PIXEL_RECIPE_PARAMETERS, BLOB_DATATYPE);
					logger.info("Running sql: " + sql);
					schedulerDb.insertData(sql);
				}
			}

			// ADDED 2020-11-30
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// need to add new JobId
				if(!queryUtil.getTableColumns(connection, SMSS_JOB_RECIPES, database, schema).contains(JOB_ID)) {
					// alter table to add the column
					String sql = queryUtil.alterTableAddColumnWithDefault("SMSS_JOB_RECIPES", "JOB_ID", "VARCHAR(200)", "PLACEHOLDER");
					logger.info("Running sql: " + sql);
					schedulerDb.execUpdateAndRetrieveStatement(sql, true);
					// make the JOB_ID the JOB_NAME for LEGACY recipes
					sql = "UPDATE SMSS_JOB_RECIPES SET JOB_ID=JOB_NAME";
					logger.info("Running sql: " + sql);
					schedulerDb.execUpdateAndRetrieveStatement(sql, true);
					// make column not null
					sql = queryUtil.modColumnNotNull("SMSS_JOB_RECIPES", "JOB_ID", "VARCHAR(2000)");
					logger.info("Running sql: " + sql);
					schedulerDb.execUpdateAndRetrieveStatement(sql, true);
					// add PK constraints on job id column
					sql = "ALTER TABLE SMSS_JOB_RECIPES ADD CONSTRAINT SMSS_JOB_RECIPES_PK PRIMARY KEY (JOB_ID)";
					logger.info("Running sql: " + sql);
					schedulerDb.execUpdateAndRetrieveStatement(sql, true);
				}
			}
			
			// 2023-04-01 just check all the columns we defined are actually there
			{
				// just check all the columns are there
				List<String> allCols = queryUtil.getTableColumns(connection, SMSS_JOB_RECIPES, database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if(!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						String addColumnSql = queryUtil.alterTableAddColumn(SMSS_JOB_RECIPES, col, types[i]);
						logger.info("Running sql: " + addColumnSql);
						schedulerDb.insertData(addColumnSql);
					}
				}
			}
			
			// SMSS_JOB_TAGS
			colNames = new String[]{JOB_ID, JOB_TAG};
			types = new String[]{VARCHAR_200, VARCHAR_200};
			constraints = new String[] { NOT_NULL, NOT_NULL };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_JOB_TAGS, colNames, types, constraints);
				logger.info("Running sql: " + sql);
				schedulerDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_JOB_TAGS, database, schema)) {
					// make the table
					String sql = queryUtil.createTableWithCustomConstraints(SMSS_JOB_TAGS, colNames, types, constraints);
					logger.info("Running sql: " + sql);
					schedulerDb.insertData(sql);
				}
			}
			
			// 2023-04-01 just check all the columns we defined are actually there
			{
				// just check all the columns are there
				List<String> allCols = queryUtil.getTableColumns(connection, SMSS_JOB_TAGS, database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if(!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						String addColumnSql = queryUtil.alterTableAddColumn(SMSS_JOB_TAGS, col, types[i]);
						logger.info("Running sql: " + addColumnSql);
						schedulerDb.insertData(addColumnSql);
					}
				}
			}

			// SMSS_AUDIT_TRAIL
			// adding is_latest flag to mark the latest record
			colNames = new String[] { JOB_ID, JOB_GROUP, EXECUTION_START, EXECUTION_END, EXECUTION_DELTA, SUCCESS, IS_LATEST, SCHEDULER_OUTPUT};
			types = new String[] { VARCHAR_200, VARCHAR_200, TIMESTAMP, TIMESTAMP, VARCHAR_255, BOOLEAN_DATATYPE, BOOLEAN_DATATYPE, CLOB_DATATYPE};
			if(!dateTimeType.equals(TIMESTAMP)) { types = cleanUpDataType(types, TIMESTAMP, dateTimeType); };
			constraints = new String[] { NOT_NULL, NOT_NULL, null, null, null, null, null, null };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_AUDIT_TRAIL, colNames, types, constraints);
				logger.info("Running sql: " + sql);
				schedulerDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_AUDIT_TRAIL, database, schema)) {
					// make the table
					String sql = queryUtil.createTableWithCustomConstraints(SMSS_AUDIT_TRAIL, colNames, types, constraints);
					logger.info("Running sql: " + sql);
					schedulerDb.insertData(sql);
				}
			}
			
			// ADDED 2020-11-30
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// need to add new JobId
				if(!queryUtil.getTableColumns(connection, SMSS_AUDIT_TRAIL, database, schema).contains(JOB_ID)) {
					// change JOB_NAME to JOB_ID
					String sql = queryUtil.modColumnName("SMSS_AUDIT_TRAIL", "JOB_NAME", "JOB_ID");
					logger.info("Running sql: " + sql);
					schedulerDb.execUpdateAndRetrieveStatement(sql, true);
				}
			}
			// ADDED 2021-02-11
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// adding the column is_latest 
				if(!queryUtil.getTableColumns(connection, SMSS_AUDIT_TRAIL, database, schema).contains(IS_LATEST)) {
					schedulerDb.execUpdateAndRetrieveStatement(queryUtil.alterTableAddColumn(SMSS_AUDIT_TRAIL, IS_LATEST, BOOLEAN_DATATYPE), true);
					// being lazy - just update all the existing ones to be is_latest false
					// in theory should go and find the last instance of each job id and update...
					try(PreparedStatement updateAuditTrailStatement = connection
								.prepareStatement("UPDATE SMSS_AUDIT_TRAIL SET IS_LATEST=?")){
						
						updateAuditTrailStatement.setBoolean(1, false);
						updateAuditTrailStatement.executeUpdate();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			// 2023-04-01 just check all the columns we defined are actually there
			{
				// just check all the columns are there
				List<String> allCols = queryUtil.getTableColumns(connection, SMSS_AUDIT_TRAIL, database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if(!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						String addColumnSql = queryUtil.alterTableAddColumn(SMSS_AUDIT_TRAIL, col, types[i]);
						logger.info("Running sql: " + addColumnSql);
						schedulerDb.insertData(addColumnSql);
					}
				}
			}
			// 2023-04-03 changing from BLOB to CLOB
			{
				// just check all the columns are there
				try {
					String[] nameAndType = queryUtil.getColumnDetails(connection, SMSS_AUDIT_TRAIL, SCHEDULER_OUTPUT, database, schema);
					if(nameAndType != null) {
						String name = nameAndType[0];
						String type = nameAndType[1];
						if(!CLOB_DATATYPE.equalsIgnoreCase(type)) {
							// add one more check
							if( !(CLOB_DATATYPE.matches("(?i)varchar\\(.*\\)") && type.equalsIgnoreCase("varchar")) ) {
								// we alter
								String sql = queryUtil.modColumnType(SMSS_AUDIT_TRAIL, SCHEDULER_OUTPUT, CLOB_DATATYPE);
								logger.info("Running sql: " + sql);
								schedulerDb.insertData(sql);
							}
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			
			
			// SMSS_EXECUTION_SCHEDULE
			colNames = new String[] { EXEC_ID, JOB_ID, JOB_GROUP};
			types = new String[] { VARCHAR_200, VARCHAR_200, VARCHAR_200};
			if(!dateTimeType.equals(TIMESTAMP)) { types = cleanUpDataType(types, TIMESTAMP, dateTimeType); };
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExists(SMSS_EXECUTION, colNames, types));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_EXECUTION, database, schema)) {
					// make the table
					String sql = queryUtil.createTable(SMSS_EXECUTION, colNames, types);
					logger.info("Running sql: " + sql);
					schedulerDb.insertData(sql);
				}
			}

			// ADDED 2020-11-30
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			// TODO: CAN DELETE THIS AFTER A FEW VERSIONS
			{
				// need to add new JobId
				if(!queryUtil.getTableColumns(connection, SMSS_EXECUTION, database, schema).contains(JOB_ID)) {
					// change JOB_NAME to JOB_ID
					String sql = queryUtil.modColumnName("SMSS_EXECUTION", "JOB_NAME", "JOB_ID");
					logger.info("Running sql: " + sql);
					schedulerDb.execUpdateAndRetrieveStatement(sql, true);
				}
			}
		} catch (SQLException se) {
			logger.error(Constants.STACKTRACE, se);
		}
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
	
	private static void addAllPrimaryKeys(Connection conn, String database, String schema) {
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
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_CALENDARS", "QRTZ_CALENDARS", database, schema)) {
					schedulerDb.insertData(query1);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_CRON_TRIGGERS", "QRTZ_CRON_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query2);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_FIRED_TRIGGERS", "QRTZ_FIRED_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query3);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_PAUSED_TRIGGER_GRPS", "QRTZ_PAUSED_TRIGGER_GRPS", database, schema)) {
					schedulerDb.insertData(query4);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_SCHEDULER_STATE", "QRTZ_SCHEDULER_STATE", database, schema)) {
					schedulerDb.insertData(query5);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_LOCKS", "QRTZ_LOCKS", database, schema)) {
					schedulerDb.insertData(query6);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_JOB_DETAILS", "QRTZ_JOB_DETAILS", database, schema)) {
					schedulerDb.insertData(query7);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_SIMPLE_TRIGGERS", "QRTZ_SIMPLE_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query8);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_SIMPROP_TRIGGERS", "QRTZ_SIMPROP_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query9);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.tableConstraintExists(conn, "PK_QRTZ_TRIGGERS", "QRTZ_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query10);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
		}
	}

	private static void addAllForeignKeys(Connection conn, String database, String schema) {
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
				if(!queryUtil.referentialConstraintExists(conn, "FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query1);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.referentialConstraintExists(conn, "FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query2);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.referentialConstraintExists(conn, "FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query3);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
			try {
				if(!queryUtil.referentialConstraintExists(conn, "FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS", database, schema)) {
					schedulerDb.insertData(query4);
				}
			} catch (SQLException se) {
				logger.error(Constants.STACKTRACE, se);
			}
		}
	}
	
	/**
	 * 
	 * @param type
	 * @return
	 */
	public static String getQuartzDelegateForRdbms(RdbmsTypeEnum type) {
		if(type == RdbmsTypeEnum.SQL_SERVER) 
		{
			return "org.quartz.impl.jdbcjobstore.MSSQLDelegate";
		}
		else if(type == RdbmsTypeEnum.POSTGRES) 
		{
			return "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate";
		}
		else if(type == RdbmsTypeEnum.ORACLE) 
		{
			return "org.quartz.impl.jdbcjobstore.oracle.OracleDelegate";
		}
		else 
		{
			return "org.quartz.impl.jdbcjobstore.StdJDBCDelegate";
		}
	}
}
