package prerna.io.connector.docs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

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

public class GetdocsIdReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);
	static RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);

	public GetdocsIdReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[0]);
		String ID = "id";
		String DOCID = "docid";
		List<String> docids = new ArrayList<>();
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			String query = " select DOCID from GOOGLE_DOCS_PROFILE where ID= ? ";
			try (PreparedStatement ps = conn.prepareStatement(query)) {
				ps.setString(1, id);
				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					docids.add(rs.getString("DOCID"));
				}
			}
			HashMap<String, Object> res = new HashMap<>();
			res.put(ID, id);
			res.put(DOCID, docids);
			return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in getting the user ids";
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
		return "This reactor returns the document id.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Id" + ReactorKeysEnum.ID.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
