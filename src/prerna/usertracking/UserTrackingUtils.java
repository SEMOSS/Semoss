package prerna.usertracking;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;
import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class UserTrackingUtils {

	private static Logger classLogger = LogManager.getLogger(UserTrackingUtils.class);

	static IRDBMSEngine userTrackingDb;
	
	/**
	 * 
	 * @param queriedDatabaseIds
	 * @param insightId
	 * @param projectId
	 */
	public static void addEngineUsage(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		if (Utility.isUserTrackingEnabled()) {
			EngineUsageUtils.add(queriedDatabaseIds, insightId, projectId);
		}
	}
	
	/**
	 * 
	 * @param queriedDatabaseIds
	 * @param insightId
	 * @param projectId
	 */
	public static void updateEngineUsage(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		if (Utility.isUserTrackingEnabled()) {
			EngineUsageUtils.update(queriedDatabaseIds, insightId, insightId);
		}
	}
	
	/**
	 * 
	 * @param engineId
	 */
	public static void addEngineViews(String engineId) {
		if (Utility.isUserTrackingEnabled()) {
			EngineViewsUtils.add(engineId);
		}
	}

	/**
	 * 
	 * @param engineId
	 */
	public static void deleteEngine(String engineId) {
		if (Utility.isUserTrackingEnabled()) {
			doDeleteEngine(engineId);
		}
	}

	/**
	 * 
	 * @param projectId
	 */
	public static void deleteProject(String projectId) {
		if (Utility.isUserTrackingEnabled()) {
			doDeleteProject(projectId);
		}
	}

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 */
	public static void deleteInsight(String projectId, String insightId) {
		if (Utility.isUserTrackingEnabled()) {
			doDeleteInsight(projectId, insightId);
		}
	}
	
	/**
	 * 
	 * @param toRecipients
	 * @param ccRecipients
	 * @param bccRecipients
	 * @param from
	 * @param subject
	 * @param emailMessage
	 * @param isHtml
	 * @param attachments
	 * @param successful
	 */
	public static void trackEmail(String[] toRecipients, String[] ccRecipients, String[] bccRecipients, 
			String from, String subject, String emailMessage, boolean isHtml, String[] attachments, boolean successful) {
		if (Utility.isUserTrackingEnabled()) {
			doTrackEmail(toRecipients, ccRecipients, bccRecipients, from, subject, emailMessage, isHtml, attachments, successful);
		}
	}
	
	/**
	 * 
	 * @param insightId
	 * @param userId
	 * @param origin
	 */
	public static void trackInsightOpen(String insightId, String userId, String origin) {
		if (Utility.isUserTrackingEnabled()) {
			doTrackInsightOpen(insightId, userId, origin);
		}
	}

	/**
	 * 
	 * @param insightId
	 * @param userId
	 * @param origin
	 */
	private static void doTrackInsightOpen(String insightId, String userId, String origin) {
		String query = "INSERT INTO INSIGHT_OPENS (INSIGHTID, USERID, OPENED_ON, ORIGIN) "
				+ "VALUES (?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, insightId);
			ps.setString(index++, userId);
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(index++, origin);

			// execute
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}		
	}

	/**
	 * 
	 * @param toRecipients
	 * @param ccRecipients
	 * @param bccRecipients
	 * @param from
	 * @param subject
	 * @param emailMessage
	 * @param isHtml
	 * @param attachments
	 * @param successful
	 */
	private static void doTrackEmail(String[] toRecipients, String[] ccRecipients, String[] bccRecipients, String from,
			String subject, String emailMessage, boolean isHtml, String[] attachments, boolean successful) {
		boolean allowClob = userTrackingDb.getQueryUtil().allowClobJavaObject();

		String query = "INSERT INTO EMAIL_TRACKING (ID, SENT_TIME, SUCCESSFUL, E_FROM, E_TO, E_CC, E_BCC, E_SUBJECT, BODY, ATTACHMENTS, IS_HTML) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, UUID.randomUUID().toString());
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.setBoolean(index++, successful);
			ps.setString(index++, from);
			
			if (toRecipients != null) {
				String toStr = Strings.join(toRecipients, ", ");
				if(allowClob) {
					Clob toclob = userTrackingDb.getConnection().createClob();
					toclob.setString(1, toStr);
					ps.setClob(index++, toclob);
				} else {
					ps.setString(index++, toStr);
				}
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			
			if (ccRecipients != null) {
				String ccStr = Strings.join(ccRecipients, ", ");
				if(allowClob) {
					Clob ccclob = userTrackingDb.getConnection().createClob();
					ccclob.setString(1, ccStr);
					ps.setClob(index++, ccclob);
				} else {
					ps.setString(index++, ccStr);
				}
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			
			if (bccRecipients != null) {
				String bccStr = Strings.join(bccRecipients, ", ");
				if(allowClob) {
					Clob bccclob = userTrackingDb.getConnection().createClob();
					bccclob.setString(1, bccStr);
					ps.setClob(index++, bccclob);
				} else {
					ps.setString(index++, bccStr);
				}
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			
			if (subject != null) {
				ps.setString(index++, subject);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}

			if (emailMessage != null) {
				if(allowClob) {
					Clob bodyClob = userTrackingDb.getConnection().createClob();
					bodyClob.setString(1, emailMessage);
					ps.setClob(index++, bodyClob);
				} else {
					ps.setString(index++, emailMessage);
				}
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			
			if (attachments != null) {
				String attachmentStr = Strings.join(attachments, ", ");
				if(allowClob) {
					Clob attachmentClob = userTrackingDb.getConnection().createClob();
					attachmentClob.setString(1, attachmentStr);
					ps.setClob(index++, attachmentClob);
				} else {
					ps.setString(index++, attachmentStr);
				}
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			
			ps.setBoolean(index++, isHtml);

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}		
	}

	/**
	 * 
	 * @param engineId
	 */
	private static void doDeleteEngine(String engineId) {
		String[] queries = { 
				"DELETE FROM ENGINE_USES where ENGINEID = ?",
				"DELETE FROM ENGINE_VIEWS where ENGINEID = ?",
				"DELETE FROM USER_CATALOG_VOTES WHERE ENGINEID = ?"
				};

		for (String query : queries) {
			doDeleteEngine(query, engineId);
		}
	}

	/**
	 * 
	 * @param query
	 * @param engineId
	 */
	private static void doDeleteEngine(String query, String engineId) {
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, engineId);

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * 
	 * @param projectId
	 */
	private static void doDeleteProject(String projectId) {
		String query = "DELETE FROM ENGINE_USES WHERE PROJECTID = ?";

		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, projectId);

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 */
	private static void doDeleteInsight(String projectId, String insightId) {
		String query = "DELETE FROM ENGINE_USES WHERE PROJECTID = ? AND INSIGHTID = ?";

		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, projectId);
			ps.setString(index++, insightId);

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param databaseId
	 * @param queryExecuted
	 * @param startTime
	 * @return
	 */
	public static void trackQueryExecution(User user, String databaseId, String queryExecuted, Timestamp startTime, Timestamp endTime, Long executionTime, boolean fail) {
		if (Utility.isUserTrackingEnabled()) {
			doTrackQueryExecution(user, databaseId, queryExecuted, startTime, endTime, executionTime, fail);
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param databaseId
	 * @param queryExecuted
	 * @param startTime
	 * @return
	 */
	private static void doTrackQueryExecution(User user, String databaseId, String queryExecuted, 
			Timestamp startTime, Timestamp endTime, Long executionTime, boolean failed) {
		String insertQuery = "INSERT INTO QUERY_TRACKING "
				+ "(ID, USERID, USERTYPE, DATABASEID, QUERY_EXECUTED, START_TIME, END_TIME, TOTAL_EXECUTION_TIME, FAILED_EXECUTION) "
				+ "VALUES(?,?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		String userId = null;
		String userType = null;
		String id = UUID.randomUUID().toString();
		if (user != null) {
			Pair<String, String> userIdType = User.getPrimaryUserIdAndTypePair(user);
			userId = userIdType.getValue0();
			userType = userIdType.getValue1();
		}
 		try {
			ps = userTrackingDb.getPreparedStatement(insertQuery);
			int index = 1;
			ps.setString(index++, id);
			ps.setString(index++, userId);
			ps.setString(index++, userType);
			ps.setString(index++, databaseId);
			userTrackingDb.getQueryUtil().handleInsertionOfClob(ps.getConnection(), ps, queryExecuted, index++, new Gson());
			ps.setTimestamp(index++, startTime);
			if(endTime == null) {
				ps.setNull(index++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(index++, endTime);
			}
			if(executionTime == null) {
				ps.setNull(index++, java.sql.Types.BIGINT);
			} else {
				ps.setLong(index++, executionTime);
			}
			ps.setBoolean(index++, failed);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}
	
	/**
	 * 
	 * @param sessionId
	 * @param ip
	 * @param user
	 * @param ap
	 */
	public static void registerLogin(String sessionId, String ip, User user, AuthProvider ap) {
		if (Utility.isUserTrackingEnabled()) {
			long start = System.currentTimeMillis();
			IUserTracking ut = UserTrackingFactory.getUserTrackingConnector();

			if (ut == null) {
				classLogger.error("Could not find user tracker. User Session/IP Data will not be saved.");
			} else {
				try {
					ut.registerLogin(sessionId, ip, user, ap);
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}

			long end = System.currentTimeMillis();
			classLogger.info("User Tracking took: {} ms", (end - start));
		}
	}

	/**
	 * 
	 * @param sessionId
	 */
	public static void registerLogout(String sessionId) {
		if (Utility.isUserTrackingEnabled()) {
			IUserTracking ut = UserTrackingFactory.getUserTrackingConnector();
			if (ut == null) {
				throw new IllegalArgumentException("Could not find user tracker.");
			}
			ut.registerLogout(sessionId);
		}
	}

	// End of User tracking methods

	// ENGINE STUFF BELOW

	/**
	 * 
	 * @throws Exception 
	 */
	public static void initUserTrackerDatabase() throws Exception {
		userTrackingDb = (IRDBMSEngine) Utility.getDatabase(Constants.USER_TRACKING_DB);
		UserTrackingOwlCreator utoc = new UserTrackingOwlCreator(userTrackingDb);
		if (utoc.needsRemake()) {
			utoc.remakeOwl();
		}

		Connection conn = null;
		try {
			conn = userTrackingDb.makeConnection();
			executeInitUserTracker(userTrackingDb, conn, utoc.getDBSchema());
			if(!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, conn, null, null);
		}
	}

	/**
	 * 
	 * @param engine
	 * @param conn
	 * @param columnNamesAndTypes
	 * @throws SQLException
	 */
	private static void executeInitUserTracker(
			IRDBMSEngine engine, 
			Connection conn,
			List<Pair<String, List<Pair<String, String>>>> dbSchema) throws SQLException {

		String database = engine.getDatabase();
		String schema = engine.getSchema();

		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();

		for (Pair<String, List<Pair<String, String>>> tableSchema : dbSchema) {
			String tableName = tableSchema.getValue0();
			String[] colNames = tableSchema.getValue1().stream().map(Pair::getValue0).toArray(String[]::new);
			String[] types = tableSchema.getValue1().stream().map(Pair::getValue1).toArray(String[]::new);
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists(tableName, colNames, types);
				executeSql(conn, sql);
			} else {
				if (!queryUtil.tableExists(engine, tableName, database, schema)) {
					String sql = queryUtil.createTable(tableName, colNames, types);
					executeSql(conn, sql);
				}
			}
			
			List<String> allCols = queryUtil.getTableColumns(conn, tableName, database, schema);
			for (int i = 0; i < colNames.length; i++) {
				String col = colNames[i];
				if(!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
					String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
					executeSql(conn, addColumnSql);
				}
			}
		}
	}

	/**
	 * 
	 * @param conn
	 * @param sql
	 * @throws SQLException
	 */
	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			classLogger.info("Running sql " + sql);
			stmt.execute(sql);
		}
	}

}
