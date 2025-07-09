package prerna.io.connector.docs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleDocsDeleteReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);
	static RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);

	public GoogleDocsDeleteReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String name = this.keyValue.get(this.keysToGet[0]);
		String id = getID(name);
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			String deletequery = "DELETE FROM GOOGLE_DOCS_PROFILE WHERE ID = ? ";
			try (PreparedStatement ps = conn.prepareStatement(deletequery)) {
				ps.setString(1, id);
				int rowaffected = ps.executeUpdate();
				if (rowaffected > 0) {
					String message = "Row with id " + id + " succesfully deleted";
					return new NounMetadata(message, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
				} else {
					String message = "Error in deleting the row";
					return new NounMetadata(message, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in getting the user ids";
			return new NounMetadata(error + ":" + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	public static String getID(String name) {
		Connection conn = null;
		String id = null;
		try {
			conn = securityDb.makeConnection();
			String query = "select ID from GOOGLE_DOCS_PROFILE where NAME = ?";
			try (PreparedStatement ps = conn.prepareStatement(query)) {
				ps.setString(1, name);
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					id = rs.getString("ID");
				}
			}
			throw new Exception("Service Account not found");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return id;
	}

	@Override
	public String getReactorDescription() {
		return "This reactor delete the row from the googledocsprofile.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Id" + ReactorKeysEnum.ID.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
