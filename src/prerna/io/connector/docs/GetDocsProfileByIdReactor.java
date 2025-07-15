package prerna.io.connector.docs;

import java.sql.*;
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

public class GetDocsProfileByIdReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);
	static RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
	
	public GetDocsProfileByIdReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[0]);
		String ID = "id";
		String DOCID = "docid";
		Connection conn = null;
		List<String> docProfile = new ArrayList<>();
		try {
			conn = securityDb.makeConnection();
			String query = " select DOCID,NAME,DATECREATED,USERNAME from GOOGLE_DOCS_PROFILE where ID = ? ";
			try (PreparedStatement ps = conn.prepareStatement(query)) {
				ps.setString(1, id);
				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					docProfile.add(rs.getString("DOCID"));
					docProfile.add(rs.getString("NAME"));
					docProfile.add(rs.getString("DATECREATED"));
					docProfile.add(rs.getString("USERNAME"));
				}
			}
			HashMap<String, Object> res = new HashMap<>();
			res.put(ID, id);
			res.put(DOCID, docProfile);
			return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in getting the user details";
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

	@Override
	public String getReactorDescription() {
		return "This reactor returns the details of the document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Id" + ReactorKeysEnum.ID.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
