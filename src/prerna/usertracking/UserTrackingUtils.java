package prerna.usertracking;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.OwlSeparatePixelFromConceptual;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class UserTrackingUtils {

	private static Logger logger = LogManager.getLogger(UserTrackingUtils.class);

	static IRDBMSEngine userTrackingDb;
	
	public static void addEngineUsage(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		if (Utility.isUserTrackingEnabled()) {
			EngineUsageUtils.add(queriedDatabaseIds, insightId, projectId);
		}
	}
	
	public static void updateEngineUsage(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		if (Utility.isUserTrackingEnabled()) {
			EngineUsageUtils.update(queriedDatabaseIds, insightId, insightId);
		}
	}
	
	public static void addEngineViews(String databaseId) {
		if (Utility.isUserTrackingEnabled()) {
			EngineViewsUtils.add(databaseId);
		}
	}

	public static void deleteDatabase(String databaseId) {
		if (Utility.isUserTrackingEnabled()) {
			doDeleteDatabase(databaseId);
		}
	}

	public static void deleteProject(String projectId) {
		if (Utility.isUserTrackingEnabled()) {
			doDeleteProject(projectId);
		}
	}

	public static void deleteInsight(String projectId, String insightId) {
		if (Utility.isUserTrackingEnabled()) {
			doDeleteInsight(projectId, insightId);
		}
	}
	
	public static void trackEmail(String[] toRecipients, String[] ccRecipients, String[] bccRecipients, 
			String from, String subject, String emailMessage, boolean isHtml, String[] attachments, boolean successful) {
		if (Utility.isUserTrackingEnabled()) {
			doTrackEmail(toRecipients, ccRecipients, bccRecipients, from, subject, emailMessage, isHtml, attachments, successful);
		}
	}
	
	public static void trackInsightOpen(String insightId, String userId, String origin) {
		if (Utility.isUserTrackingEnabled()) {
			doTrackInsightOpen(insightId, userId, origin);
		}
	}

	private static void doTrackInsightOpen(String insightId, String userId, String origin) {
		String query = "INSERT INTO INSIGHT_OPENS (INSIGHTID, USERID, OPENED_ON, ORIGIN) "
				+ "VALUES (?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, insightId);
			ps.setString(index++, userId);
			ps.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
			ps.setString(index++, origin);

			// execute
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(userTrackingDb, null, ps, null);
		}		
		
	}

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
			ps.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
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
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(userTrackingDb, null, ps, null);
		}		
	}

	private static void doDeleteDatabase(String databaseId) {
		String[] queries = { 
				"DELETE FROM ENGINE_USES where ENGINEID = ?",
				"DELETE FROM ENGINE_VIEWS where ENGINEID = ?",
				"DELETE FROM USER_CATALOG_VOTES WHERE ENGINEID = ?"
				};

		for (String query : queries) {
			doDeleteDatabase(query, databaseId);
		}
	}

	private static void doDeleteDatabase(String query, String databaseId) {
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, databaseId);

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(userTrackingDb, null, ps, null);
		}
	}

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
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(userTrackingDb, null, ps, null);
		}
	}

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
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(userTrackingDb, null, ps, null);
		}
	}

	// Start of user tracking methods

	public static void registerLogin(String sessionId, String ip, User user, AuthProvider ap) {
		if (Utility.isUserTrackingEnabled()) {
			long start = System.currentTimeMillis();
			IUserTracking ut = UserTrackingFactory.getUserTrackingConnector();

			if (ut == null) {
				logger.error("Could not find user tracker. User Session/IP Data will not be saved.");
			} else {
				try {
					ut.registerLogin(sessionId, ip, user, ap);
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}

			long end = System.currentTimeMillis();
			logger.info("User Tracking took: {} ms", (end - start));
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

	// DATABASE STUFF BELOW

	/**
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void initUserTrackerDatabase() throws SQLException, IOException {
		userTrackingDb = (IRDBMSEngine) Utility.getDatabase(Constants.USER_TRACKING_DB);
		UserTrackingOwlCreator utoc = new UserTrackingOwlCreator(userTrackingDb);

		if (utoc.needsRemake()) {
			utoc.remakeOwl();
		}

		OwlSeparatePixelFromConceptual.fixOwl(userTrackingDb.getProp());

		Connection conn = null;
		try {
			conn = userTrackingDb.makeConnection();
			executeInitUserTracker(userTrackingDb, conn, utoc.getDBSchema());
		} finally {
			closeResources(userTrackingDb, conn, null, null);
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
			String tableName = tableSchema.getLeft();
			String[] colNames = tableSchema.getRight().stream().map(Pair::getLeft).toArray(String[]::new);
			String[] types = tableSchema.getRight().stream().map(Pair::getRight).toArray(String[]::new);
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
	 * @param engine
	 * @param conn
	 * @param stmt
	 * @param rs
	 */
	private static void closeResources(IRDBMSEngine engine, Connection conn, Statement stmt, ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		try {
			if (engine != null && engine.isConnectionPooling()) {
				if(conn != null) {
					conn.close();
				} else if(stmt != null) {
					stmt.getConnection().close();
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			logger.info("Running sql " + sql);
			stmt.execute(sql);
		}
	}

}
