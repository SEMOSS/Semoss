package prerna.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.om.Insight;
import prerna.reactor.VectorStorage;

public class LogManagerUtil {
	
	private static final Logger classLogger = LogManager.getLogger(VectorStorage.class);
	
	private static final String TABLE_NAME = "system_logs"; 
	private static final String DATABASE_ENGINE_ID = "eb98274a-1e5c-46fb-9423-ce43bb595dad";
	
	public static void saveLogsToDataBase(Insight insight, Exception exp, String serviceName) {
		
		Insight insightOne = insight;
		String user = insightOne.getUserId();
		
		String errorMessage = null;
		String stackTrace = null;
		Connection conn = null;
		PreparedStatement ps = null;
		IDatabaseEngine database = Utility.getDatabase(DATABASE_ENGINE_ID); 
		IRDBMSEngine rdbmsEng = (IRDBMSEngine) database;
		try {	
			errorMessage = exp.toString();
			stackTrace = getStackTrace(exp);
			if (database == null) {
				throw new IllegalArgumentException("Must define the database to pull data from");
			}
			conn = rdbmsEng.getConnection();
			
			String psString = "INSERT INTO " +
					TABLE_NAME +
					"(timestamp, userid, error_message, stack_trace, service_name) "+
					"VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?)";
			
			ps = conn.prepareStatement(psString);
			int index = 1;
			ps.setString(index++, user);
			ps.setString(index++, errorMessage);
			ps.setString(index++, stackTrace);
			ps.setString(index++, serviceName);
			ps.executeUpdate();			
			
		} catch(SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);			
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(rdbmsEng, conn, null, null);
		}
		
	}
	
	public static String getStackTrace(Exception e) {
		StringBuilder stackTrace = new StringBuilder();
		for (StackTraceElement element : e.getStackTrace()) {
		stackTrace.append(element.toString()).append("\n");
		}
		return stackTrace.toString();
		}


}
