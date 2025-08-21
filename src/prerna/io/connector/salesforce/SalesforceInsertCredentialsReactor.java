package prerna.io.connector.salesforce;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class SalesforceInsertCredentialsReactor extends AbstractReactor {

	public static final String SALESFORCE_UNIQUE_ID = "ID";
	private static final String TABLE = "SALESFORCE_CREDENTIALS";

	private static final Logger classLogger = LogManager.getLogger(SalesforceInsertCredentialsReactor.class);

	private static final String INSTANCE_URL = "instanceUrl";
	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String REDIRECT_URI = "redirectUri";
	private static final String KEY_NAME = "keyName";
	
	static IRDBMSEngine salesforceDb;

	public SalesforceInsertCredentialsReactor() {
		this.keysToGet = new String[] { INSTANCE_URL, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, KEY_NAME };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
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

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String instanceUrl = this.keyValue.get(this.keysToGet[0]);
		String clientId = this.keyValue.get(this.keysToGet[1]);
		String clientSecret = this.keyValue.get(this.keysToGet[2]);
		String redirectUri = this.keyValue.get(this.keysToGet[3]);
		String keyName = this.keyValue.get(this.keysToGet[4]);

		long now = System.currentTimeMillis();
		Timestamp dateCreated = new Timestamp(now);

		User user = this.insight.getUser();
		String createdBy = user.getPrimaryLoginToken().getUsername();

		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);

			HashMap<Object, Object> responseMap = new HashMap<>();

			if (tableName == null) {
				responseMap.put("Data inserted successfully", false);
				responseMap.put("Error", "Salesforce credentials table not found in database");
				return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			UUID id = UUID.randomUUID();
			HashMap<String, Object> insertResult = insertData(salesforceDb, tableName, id.toString(), instanceUrl,
					clientId, clientSecret, redirectUri, createdBy, dateCreated, keyName);

			if (Boolean.FALSE.equals(insertResult.get("Data inserted successfully"))) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(insertResult.get("Error").toString()));
			}

			String profileId = readData(salesforceDb, tableName, instanceUrl, clientId);
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

	// to insert a Salesforce user record
	private HashMap<String, Object> insertData(IRDBMSEngine salesforceDb, String tableName, String id,
			String instanceUrl, String clientId, String clientSecret, String redirectUri, String createdBy, Timestamp dateCreated, String keyName) {
		salesforceDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
		HashMap<String, Object> map = new HashMap<>();
		boolean flag = false;

		try (Connection conn = salesforceDb.makeConnection()) {
			// validate table name
			if (!isValidTableName(tableName)) {
				map.put("Data inserted successfully", false);
				map.put("Error", "Invalid table name");
				return map;
			}

			// check if INSTANCEURL and CLIENTID or KEYNAME already exists
			String checkQuery = "SELECT COUNT(*) FROM " + tableName + " WHERE INSTANCEURL=? AND CLIENTID=? OR KEYNAME=?";
			try (PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {
				checkStmt.setString(1, instanceUrl);
				checkStmt.setString(2, clientId);
				checkStmt.setString(3, keyName);
				try (ResultSet rs = checkStmt.executeQuery()) {
					if (rs.next() && rs.getInt(1) > 0) {
						map.put("Data inserted successfully", false);
						map.put("Error", "Salesforce credentials already exist ");
						return map;
					}
				}
			}

			// doing the insert
			String insertQuery = "INSERT INTO " + tableName
					+ " (ID, INSTANCEURL, CLIENTID, CLIENTSECRET, REDIRECTURI, CREATEDBY, DATECREATED, KEYNAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
			try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
				insertStmt.setString(1, id);
				insertStmt.setString(2, instanceUrl);
				insertStmt.setString(3, clientId);
				insertStmt.setString(4, clientSecret);
				insertStmt.setString(5, redirectUri);
				insertStmt.setString(6, createdBy);
				insertStmt.setTimestamp(7, dateCreated);
				insertStmt.setString(8, keyName);

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
	private String readData(IRDBMSEngine salesforceDb, String tableName, String instanceUrl, String clientId) {
		salesforceDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
		String profileKey = null;

		if (!isValidTableName(tableName)) {
			classLogger.error("Invalid table name");
			throw new IllegalArgumentException("Invalid table name");
		}
		
		String query = "SELECT " + SALESFORCE_UNIQUE_ID + " FROM " + tableName + " WHERE INSTANCEURL=? AND CLIENTID=?";
		try (Connection conn = salesforceDb.makeConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, instanceUrl);
			pstmt.setString(2, clientId);

			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					profileKey = rs.getString(SALESFORCE_UNIQUE_ID);
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
		return "Inserts a row into SALESFORCE_CREDENTIALS table. Duplicate (INSTANCEURL+CLIENTID) prevented.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(INSTANCE_URL)) {
			return "Salesforce instance URL for the user's org " + INSTANCE_URL;
		} else if (key.equals(CLIENT_ID)) {
			return "Salesforce application's client ID " + CLIENT_ID;
		} else if (key.equals(CLIENT_SECRET)) {
			return "Client secret for authentication " + CLIENT_SECRET;
		} else if (key.equals(REDIRECT_URI)) {
			return "Redirect Uri which is the Callback Url of my salesforce connected app " + REDIRECT_URI;
		} else if (key.equals(KEY_NAME)) {
			return "Key Name for users to identify the connection " + KEY_NAME;
		}
		return super.getDescriptionForKey(key);
	}
	
}
