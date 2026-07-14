/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class UserTrackingUtils {

	private static Logger classLogger = LogManager.getLogger(UserTrackingUtils.class);

	/**
	 * Records that the given databases were queried within an insight. No-op when
	 * user tracking is disabled.
	 *
	 * @param queriedDatabaseIds the ids of the databases/engines that were queried
	 * @param insightId          the insight the queries ran in
	 * @param projectId          the project owning the insight
	 */
	public static void addEngineUsage(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		EngineUsageUtils.add(queriedDatabaseIds, insightId, projectId);
	}

	/**
	 * Reconciles the databases recorded as used by an insight: newly used ones are
	 * added, still-used ones are refreshed, and ones no longer used are removed.
	 * No-op when user tracking is disabled.
	 *
	 * @param queriedDatabaseIds the ids of the databases/engines currently in use
	 * @param insightId          the insight the queries ran in
	 * @param projectId          the project owning the insight
	 */
	public static void updateEngineUsage(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		EngineUsageUtils.update(queriedDatabaseIds, insightId, insightId);
	}

	/**
	 * Removes all user-tracking rows for an engine (usage, views, and votes). No-op
	 * when user tracking is disabled.
	 *
	 * @param engineId the id of the engine being deleted
	 */
	public static void deleteEngine(String engineId) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		String[] queries = { "DELETE FROM ENGINE_USES where ENGINEID = ?",
				"DELETE FROM ENGINE_VIEWS where ENGINEID = ?", "DELETE FROM USER_CATALOG_VOTES WHERE ENGINEID = ?" };

		for (String query : queries) {
			doDeleteEngine(query, engineId);
		}
	}

	/**
	 * Removes engine-usage rows recorded for a project. No-op when user tracking is
	 * disabled.
	 *
	 * @param projectId the id of the project being deleted
	 */
	public static void deleteProject(String projectId) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
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
			classLogger.error("Failed to delete user-tracking engine usage for project {}", projectId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * Removes engine-usage rows recorded for a specific insight within a project.
	 * No-op when user tracking is disabled.
	 *
	 * @param projectId the project owning the insight
	 * @param insightId the id of the insight being deleted
	 */
	public static void deleteInsight(String projectId, String insightId) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
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
			classLogger.error("Failed to delete user-tracking engine usage for project {} insight {}", projectId,
					insightId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * Records an email send attempt in the email tracking table. No-op when user
	 * tracking is disabled.
	 *
	 * @param toRecipients  the primary recipients
	 * @param ccRecipients  the cc recipients
	 * @param bccRecipients the bcc recipients
	 * @param from          the sender address
	 * @param subject       the email subject
	 * @param emailMessage  the email body
	 * @param isHtml        whether the body is HTML
	 * @param attachments   the attachment names/paths
	 * @param successful    whether the send succeeded
	 */
	public static void trackEmail(String[] toRecipients, String[] ccRecipients, String[] bccRecipients, String from,
			String subject, String emailMessage, boolean isHtml, String[] attachments, boolean successful) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
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
				String toStr = String.join(", ", toRecipients);
				if (allowClob) {
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
				String ccStr = String.join(", ", ccRecipients);
				if (allowClob) {
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
				String bccStr = String.join(", ", bccRecipients);
				if (allowClob) {
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
				if (allowClob) {
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
				String attachmentStr = String.join(", ", attachments);
				if (allowClob) {
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
			classLogger.error("Failed to track email from {} (subject {}, successful={})", from, subject, successful,
					e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * Records that a user opened an insight. No-op when user tracking is disabled.
	 *
	 * @param insightId the id of the opened insight
	 * @param userId    the id of the user who opened it
	 * @param origin    where the open originated from
	 */
	public static void trackInsightOpen(String insightId, String userId, String origin) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		String query = "INSERT INTO INSIGHT_OPENS (INSIGHTID, USERID, OPENED_ON, ORIGIN) " + "VALUES (?, ?, ?, ?)";
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
			classLogger.error("Failed to track insight open for insight {} by user {} (origin {})", insightId, userId,
					origin, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * Executes a single engine-scoped delete statement, binding the engine id as
	 * the sole parameter.
	 *
	 * @param query    the parameterized delete statement to run
	 * @param engineId the engine id to bind
	 */
	private static void doDeleteEngine(String query, String engineId) {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
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
			classLogger.error("Failed to delete user-tracking rows for engine {} [query: {}]", engineId, query, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * Records a single query execution against a database, including its timing and
	 * whether it failed. No-op when user tracking is disabled.
	 *
	 * @param user          the user who ran the query; may be {@code null}
	 * @param databaseId    the database the query ran against
	 * @param queryExecuted the executed query text
	 * @param startTime     when execution started
	 * @param endTime       when execution finished; may be {@code null}
	 * @param executionTime total execution time in milliseconds; may be
	 *                      {@code null}
	 * @param failed        whether the query failed
	 */
	public static void trackQueryExecution(User user, String databaseId, String queryExecuted, Timestamp startTime,
			Timestamp endTime, Long executionTime, boolean failed) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
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
			userTrackingDb.getQueryUtil().handleInsertionOfClob(ps.getConnection(), ps, queryExecuted, index++,
					new Gson());
			ps.setTimestamp(index++, startTime);
			if (endTime == null) {
				ps.setNull(index++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(index++, endTime);
			}
			if (executionTime == null) {
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
			classLogger.error("Failed to track query execution against database {} for user {} (failed={})", databaseId,
					userId, failed, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * Registers a user login/session with the configured user tracker. No-op when
	 * user tracking is disabled; logs and skips if no tracker is available.
	 *
	 * @param sessionId the HTTP session id
	 * @param ip        the originating client IP
	 * @param user      the user logging in
	 * @param ap        the auth provider the login was performed against
	 */
	public static void registerLogin(String sessionId, String ip, User user, AuthProvider ap) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		long start = System.currentTimeMillis();
		IUserTracking ut = UserTrackingFactory.getUserTrackingConnector();

		if (ut == null) {
			classLogger.error("Could not find user tracker. User Session/IP Data will not be saved.");
		} else {
			try {
				ut.registerLogin(sessionId, ip, user, ap);
			} catch (Exception e) {
				classLogger.error("Failed to register login user tracking for session {}", sessionId, e);
			}
		}

		long end = System.currentTimeMillis();
		classLogger.info("registerLogin user tracking took {} ms", (end - start));
	}

	/**
	 * Marks the given session as ended with the configured user tracker. No-op when
	 * user tracking is disabled.
	 *
	 * @param sessionId the HTTP session id that ended
	 */
	public static void registerLogout(String sessionId) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		IUserTracking ut = UserTrackingFactory.getUserTrackingConnector();
		if (ut == null) {
			throw new IllegalArgumentException("Could not find user tracker.");
		}
		ut.registerLogout(sessionId);
	}

	// End of User tracking methods

	// ENGINE STUFF BELOW

	/**
	 * Ensures the user-tracking database is initialized: remakes the OWL when
	 * needed and creates or alters the tracking tables to match the expected
	 * schema.
	 *
	 * @throws Exception if the schema cannot be created or updated
	 */
	public static void initUserTrackerDatabase() throws Exception {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		UserTrackingOwlCreator utoc = new UserTrackingOwlCreator(userTrackingDb.getQueryUtil());
		if (utoc.needsRemake(userTrackingDb)) {
			utoc.remakeOwl(userTrackingDb);
		}

		Connection conn = null;
		try {
			conn = userTrackingDb.getConnection();
			executeInitUserTracker(userTrackingDb, conn, utoc.getDBSchema());
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, conn, null, null);
		}
	}

	/**
	 * Creates each user-tracking table that does not yet exist and adds any missing
	 * columns to existing tables, per the provided schema definition.
	 *
	 * @param engine   the user-tracking database engine
	 * @param conn     an open connection to that engine
	 * @param dbSchema the table-to-(column, type) schema to reconcile against
	 * @throws SQLException if a DDL statement fails
	 */
	private static void executeInitUserTracker(IRDBMSEngine engine, Connection conn,
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
				if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
					String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
					executeSql(conn, addColumnSql);
				}
			}
		}
	}

	/**
	 * Executes a single SQL/DDL statement on the given connection.
	 *
	 * @param conn the connection to execute against
	 * @param sql  the statement to run
	 * @throws SQLException if the statement fails
	 */
	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			classLogger.info("Running sql {}", sql);
			stmt.execute(sql);
		}
	}

}
