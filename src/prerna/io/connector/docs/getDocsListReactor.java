package prerna.io.connector.docs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class getDocsListReactor extends AbstractReactor{
	
	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);
	static RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
	
	public getDocsListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ACCOUNTNAME.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String accname = this.keyValue.get(this.keysToGet[0]);
		List<String> docList = new ArrayList<>();
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			String query = " select TITLE from GOOGLE_DOCS_PROFILE where USERNAME = ? ";
			try (PreparedStatement ps = conn.prepareStatement(query)) {
				ps.setString(1, accname);
				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					docList.add(rs.getString("TITLE"));
				}
			}
			HashMap<String, Object> res = new HashMap<>();
			res.put("Name", accname);
			res.put("List", docList);
			return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in getting the List of documents";
			return new NounMetadata(error + ":" + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
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
