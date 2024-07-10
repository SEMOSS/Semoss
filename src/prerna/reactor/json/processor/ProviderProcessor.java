package prerna.reactor.json.processor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class ProviderProcessor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(ProviderProcessor.class);

	@Override
	public NounMetadata execute() {
		System.out.println("Process a provider");
		
		final String CONNECTION_URL = "connURL";
		final String USER = "username";
		final String PASS = "password";
		final String DRIVER = "driver";
		
//		try {
//			Class.forName(DRIVER);
//		} catch (ClassNotFoundException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
		
		Connection conn = null;
		Statement stmt = null;
		try {
//			conn = DriverManager.getConnection(CONNECTION_URL, USER, PASS);
//			stmt = conn.createStatement();
			List<String> sqlStrings = (List<String>) this.curRow.get(0);
			for(int i = 0; i < sqlStrings.size(); i++) {
//				stmt.execute(sqlStrings.get(i));
//				System.out.println("Provider sql query (" + i +") = " + sqlStrings.get(i));
			}
		} 
//		catch (SQLException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		} 
		finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return new NounMetadata("success", PixelDataType.CONST_STRING);
	}

}
