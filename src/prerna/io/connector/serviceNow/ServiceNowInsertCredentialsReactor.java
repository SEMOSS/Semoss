package prerna.io.connector.serviceNow;

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

public class ServiceNowInsertCredentialsReactor extends AbstractReactor {
	
    private static final Logger classLogger = LogManager.getLogger(ServiceNowInsertCredentialsReactor.class);
    
    private static final String TABLE = "SERVICENOW_CREDENTIALS";
    public static final String SERVICENOW_UNIQUE_ID = "ID";

    private static final String INSTANCE_URL = "instanceUrl";
	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String REDIRECT_URI = "redirectUri";
	private static final String KEY_NAME = "keyName";
    
    public ServiceNowInsertCredentialsReactor() {
    	this.keysToGet = new String[] { INSTANCE_URL, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, KEY_NAME };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
    }

    // Centralized table name lookup
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

        HashMap<Object, Object> responseMap = new HashMap<>();
        try {
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            String tableName = getTableName(database);
            if (tableName == null) {
                responseMap.put("Data inserted successfully", false);
                responseMap.put("Error", "SERVICE_NOW table not found in database.");
                return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }

            IRDBMSEngine serviceNowDB = (RDBMSNativeEngine) database;

            UUID id = UUID.randomUUID();
            HashMap<String, Object> insertResult = insertData(serviceNowDB, tableName, id.toString(), instanceUrl,
				clientId, clientSecret, redirectUri, createdBy, dateCreated, keyName);

            if (Boolean.FALSE.equals(insertResult.get("Data inserted successfully"))) {
                String msg = (String) insertResult.get("Error");
                throw new SemossPixelException(NounMetadata.getErrorNounMessage(msg));
            }

            String profileId = readData(serviceNowDB, tableName, instanceUrl, clientId, clientSecret);
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

    private String readData(IRDBMSEngine serviceNowDB, String tableName, String instanceURL, String clientId, String clientSecret) {
        if (!isValidTableName(tableName)) {
            classLogger.error("Invalid table name");
            throw new IllegalArgumentException("Invalid table name");
        }

        String profileKey = null;
        String query = "SELECT " + SERVICENOW_UNIQUE_ID + " FROM " + tableName
            + " WHERE INSTANCEURL=? AND CLIENTID=? AND CLIENTSECRET=?";

        try (Connection conn = serviceNowDB.makeConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, instanceURL);
            pstmt.setString(2, clientId);
            pstmt.setString(3, clientSecret);

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

    private HashMap<String, Object> insertData(IRDBMSEngine serviceNowDB, String tableName, String id,
			String instanceUrl, String clientId, String clientSecret, String redirectUri, String createdBy, Timestamp dateCreated, String keyName) {
        HashMap<String, Object> map = new HashMap<>();
        boolean flag = false;

        if (!isValidTableName(tableName)) {
            map.put("Data inserted successfully", false);
            map.put("Error", "Invalid table name");
            return map;
        }

        // check if INSTANCEURL and CLIENTID or KEYNAME already exists
        String checkQuery = "SELECT COUNT(*) AS CNT FROM " + tableName + " WHERE INSTANCEURL=? AND CLIENTID=? OR KEYNAME=?";
        try (Connection conn = serviceNowDB.makeConnection();
             PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {

        	checkStmt.setString(1, instanceUrl);
			checkStmt.setString(2, clientId);
			checkStmt.setString(3, keyName);

            try (ResultSet rs = checkStmt.executeQuery()) {
                if (rs.next() && rs.getInt("CNT") > 0) {
                    String msg = "Error: KEY_NAME '" + keyName + "' already exists for this user.";
                    map.put("Data inserted successfully", false);
                    map.put("Error", msg);
                    return map;
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
            // Don't throw here, just return the map with error info
        }
        return map;
    }

    private boolean isValidTableName(String tableName) {
        return TABLE.equals(tableName);
    }

    @Override
    public String getReactorDescription() {
        return "This reactor is used for inserting ServiceNow Credentials";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(INSTANCE_URL)) {
            return "ServiceNow instance URL for API operations.";
        } else if (key.equals(CLIENT_ID)) {
            return "Client ID used for ServiceNow API authentication.";
        } else if (key.equals(CLIENT_SECRET)) {
            return "Client Secret used for ServiceNow API authentication.";
        } else if (key.equals(KEY_NAME)) {
            return "Unique key name to identify this ServiceNow credential entry.";
        }
        return super.getDescriptionForKey(key);
    }
}
