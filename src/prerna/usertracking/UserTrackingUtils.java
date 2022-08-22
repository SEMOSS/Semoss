package prerna.usertracking;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.OwlSeparatePixelFromConceptual;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class UserTrackingUtils {
	
	private static Logger logger = LogManager.getLogger(UserTrackingUtils.class);
	
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
	
	// DATABASE STUFF BELOW
	
	/**
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void initUserTrackerDatabase() throws SQLException, IOException {
		IRDBMSEngine engine = (IRDBMSEngine) Utility.getEngine(Constants.USER_TRACKING_DB);
		UserTrackingOwlCreator utoc = new UserTrackingOwlCreator(engine);
		
		if (utoc.needsRemake()) {
			utoc.remakeOwl();
		}
		
		OwlSeparatePixelFromConceptual.fixOwl(engine.getProp());
		
		Connection conn = null;
		
		try {
			conn = engine.makeConnection();
			executeInitUserTracker(engine, conn, utoc.getColumnNamesAndTypes());
		} finally {
			closeResources(engine, conn, null, null);
		}
	}
	
	/**
	 * 
	 * @param engine
	 * @param conn
	 * @param columnNamesAndTypes
	 * @throws SQLException
	 */
	private static void executeInitUserTracker(IRDBMSEngine engine, Connection conn, List<Pair<String, String>> columnNamesAndTypes) throws SQLException {
		
		String database = engine.getDatabase();
		String schema = engine.getSchema();
		
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		
		String[] colNames = columnNamesAndTypes.stream()
				.map(Pair::getLeft)
				.toArray(String[]::new);
		
		String[] types = columnNamesAndTypes.stream()
				.map(Pair::getRight)
				.toArray(String[]::new);
		
		if (allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("USER_TRACKING", colNames, types));
		} else {
			if (!queryUtil.tableExists(engine, "USER_TRACKING", database, schema)) {
				executeSql(conn, queryUtil.createTable("USER_TRACKING", colNames, types));
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
			if(engine != null && engine.isConnectionPooling() && conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()){
			stmt.execute(sql);
		}
	}

}
