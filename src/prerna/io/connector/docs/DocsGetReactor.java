package prerna.io.connector.docs;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class DocsGetReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);
	static RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);

	@Override
	public NounMetadata execute() {
		Connection conn = null;
		List<DocsDetails> resultList = new ArrayList<DocsDetails>();
		try {
			conn = securityDb.makeConnection();
			String query = "select * from GOOGLE_DOCS_PROFILE";
			try ( Statement  stmt = conn.createStatement()) {
				ResultSet rs = stmt.executeQuery(query);
				while (rs.next()) {
					DocsDetails docsDetails = new DocsDetails();
					docsDetails.setName(rs.getString("NAME"));
					docsDetails.setDatecreated(rs.getTimestamp("DATECREATED"));
					docsDetails.setUseremail(rs.getString("USERID"));
					resultList.add(docsDetails);
				}
			}
			return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			String error = "Error in the reactor DocsGetReactor: " + e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
		finally {
			if(securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

}
