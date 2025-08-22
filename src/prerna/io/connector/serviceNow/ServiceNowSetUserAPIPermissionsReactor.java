package prerna.io.connector.serviceNow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ServiceNowSetUserAPIPermissionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowSetUserAPIPermissionsReactor.class);
	private static final String TABLE = "USERAPIPERMISSION";
	public static final String SERVICENOW_UNIQUE_ID = "UUID";

	public ServiceNowSetUserAPIPermissionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.USERID.getKey(),
				ReactorKeysEnum.TYPE.getKey(), ReactorKeysEnum.API_ID.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String userId = this.keyValue.get(this.keysToGet[0]);
		String type = this.keyValue.get(this.keysToGet[1]);
		String apiId = this.keyValue.get(this.keysToGet[2]);
		UUID id = UUID.randomUUID();
		HashMap<Object, Object> responseMap = new HashMap<>();
		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);
			if (tableName == null) {
				responseMap.put("Data inserted successfully", false);
				responseMap.put("Error", "USERAPIPERMISSION table not found in database.");
				return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			IRDBMSEngine serviceNowDB = (RDBMSNativeEngine) database;

			HashMap<String, Object> insertResult = insertData(serviceNowDB, tableName, userId, id, type, apiId);
			if (Boolean.FALSE.equals(insertResult.get("Data inserted successfully"))) {
				String msg = (String) insertResult.get("Error");
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(msg));
			}

			String profileId = readData(serviceNowDB, tableName, userId, id, type, apiId);
			if (profileId != null && !profileId.isEmpty()) {
				responseMap.put("id", profileId);
				responseMap.put("success", true);
			} else {
				responseMap.put("id", null);
				responseMap.put("success", false);
			}
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in executing the reactor: " + e.getMessage();
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(error));
		}
	}

	private String readData(IRDBMSEngine serviceNowDB, String tableName, String userId, UUID id, String type,
			String apiId) {
		if (!isValidTableName(tableName)) {
			classLogger.error("Invalid table name");
			throw new IllegalArgumentException("Invalid table name");
		}

		String profileKey = null;
		String query = "SELECT " + SERVICENOW_UNIQUE_ID + " FROM " + tableName
				+ " WHERE USERID=? AND UUID=? AND TYPE=? AND API_ID=?";
		try (Connection conn = serviceNowDB.makeConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, userId);
			pstmt.setString(2, id.toString());
			pstmt.setString(3, type);
			pstmt.setString(4, apiId);

			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					profileKey = rs.getString(SERVICENOW_UNIQUE_ID);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return profileKey;
	}

	private HashMap<String, Object> insertData(IRDBMSEngine serviceNowDB, String tableName, String userId,UUID id,
			String type, String apiId) {
		HashMap<String, Object> map = new HashMap<>();
		boolean flag = false;

		if (!isValidTableName(tableName)) {
			map.put("Data inserted successfully", false);
			map.put("Error", "Invalid table name");
			return map;
		}

		// Check for duplicate USERID+UUID+TYPE+API_ID
		String checkQuery = "SELECT COUNT(*) AS CNT FROM " + tableName
				+ " WHERE USERID = ? AND UUID = ? AND TYPE = ? AND API_ID = ?";
		try (Connection conn = serviceNowDB.makeConnection();
				PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {
			checkStmt.setString(1, userId);
			checkStmt.setString(2, id.toString());
			checkStmt.setString(3, type);
			checkStmt.setString(4, apiId);

			try (ResultSet rs = checkStmt.executeQuery()) {
				if (rs.next() && rs.getInt("CNT") > 0) {
					String msg = "Error: Mapping for USERID '" + userId + "', UUID '" + id + "', TYPE '" + type
							+ "', API_ID '" + apiId + "' already exists.";
					map.put("Data inserted successfully", false);
					map.put("Error", msg);
					return map;
				}
			}

			String insertQuery = "INSERT INTO " + tableName + " (USERID, UUID, TYPE, API_ID) " + "VALUES (?, ?, ?, ?)";
			try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
				insertStmt.setString(1, userId);
				insertStmt.setString(2, id.toString());
				insertStmt.setString(3, type);
				insertStmt.setString(4, apiId);
				int rowsInserted = insertStmt.executeUpdate();
				flag = rowsInserted > 0;
				map.put("Data inserted successfully", flag);
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			map.put("Data inserted successfully", false);
			map.put("Error", e.getMessage());
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return map;
	}

	private boolean isValidTableName(String tableName) {
		return TABLE.equals(tableName);
	}

	private String getTableName(IDatabaseEngine database) {
		try {
			List<String> tables = database.getPixelConcepts();
			for (String tableName : tables) {
				if (TABLE.equals(tableName)) {
					return tableName;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return null;
	}
}
