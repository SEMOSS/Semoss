package prerna.io.connector.salesforce;

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

public class SetUserApiPermissionsReactor extends AbstractReactor {

	public static final String UNIQUE_ID = "ID";
	private static final String TABLE = "USERAPIPERMISSION";

	private static final Logger classLogger = LogManager.getLogger(SetUserApiPermissionsReactor.class);

	static IRDBMSEngine userApiPermissionDb;
	
	public SetUserApiPermissionsReactor() {
		this.keysToGet = new String[] { "userId", "apiId", ReactorKeysEnum.TYPE.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String userId = this.keyValue.get(this.keysToGet[0]);
		String apiId = this.keyValue.get(this.keysToGet[1]);
		String type = this.keyValue.get(this.keysToGet[2]);
		
		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);

			HashMap<Object, Object> responseMap = new HashMap<>();

			if (tableName == null) {
				responseMap.put("Data inserted successfully", false);
				responseMap.put("Error", "User Api Permission table not found in database");
				return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			UUID id = UUID.randomUUID();
			HashMap<String, Object> insertResult = insertData(userApiPermissionDb, tableName, id.toString(), userId, apiId, type);

			if (Boolean.FALSE.equals(insertResult.get("Data inserted successfully"))) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(insertResult.get("Error").toString()));
			}

			String profileId = readData(userApiPermissionDb, tableName, apiId);
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
			String error = "Error in executing the reactor " + e.getMessage();
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(error));
		}
	}

	private boolean isValidTableName(String tableName) {
		return TABLE.equals(tableName);
	}

	// centralized table name lookup
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
	
	private HashMap<String, Object> insertData(IRDBMSEngine userApiPermissionDb, String tableName, String id,
			String userId, String apiId, String type) {
		userApiPermissionDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
		HashMap<String, Object> map = new HashMap<>();
		boolean flag = false;

		try (Connection conn = userApiPermissionDb.makeConnection()) {
			// validate table name
			if (!isValidTableName(tableName)) {
				map.put("Data inserted successfully", false);
				map.put("Error", "Invalid table name");
				return map;
			}

			// check if API_ID already exists
			String checkQuery = "SELECT COUNT(*) FROM " + tableName + " WHERE API_ID=?";
			try (PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {
				checkStmt.setString(1, apiId);
				try (ResultSet rs = checkStmt.executeQuery()) {
					if (rs.next() && rs.getInt(1) > 0) {
						map.put("Data inserted successfully", false);
						map.put("Error", "Salesforce credentials already exist ");
						return map;
					}
				}
			}

			// doing the insert
			String insertQuery = "INSERT INTO " + tableName + " (ID, USERID, API_ID, TYPE) VALUES (?, ?, ?, ?)";
			try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
				insertStmt.setString(1, id);
				insertStmt.setString(2, userId);
				insertStmt.setString(3, apiId);
				insertStmt.setString(4, type);
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

	// to return primary key of the user after inserting data in DB
	private String readData(IRDBMSEngine userApiPermissionDb, String tableName, String apiId) {
		userApiPermissionDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
		String profileKey = null;

		if (!isValidTableName(tableName)) {
			classLogger.error("Invalid table name");
			throw new IllegalArgumentException("Invalid table name");
		}
		
		String query = "SELECT " + UNIQUE_ID + " FROM " + tableName + " WHERE API_ID=?";
		try (Connection conn = userApiPermissionDb.makeConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, apiId);

			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					profileKey = rs.getString(UNIQUE_ID);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return profileKey;
	}
	@Override
	public String getReactorDescription() {
		return "Inserts a row into USERAPIPERMISSION table with ID (UUID), USERID, API_ID and TYPE.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TYPE.getKey())) {
			return "Type of engine " + ReactorKeysEnum.TYPE.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
