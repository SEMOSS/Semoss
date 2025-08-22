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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ServiceNowInsertCredentialsReactor extends AbstractReactor {
    private static final Logger classLogger = LogManager.getLogger(ServiceNowInsertCredentialsReactor.class);
    private static final String TABLE = "SERVICENOW";
    public static final String SERVICENOW_UNIQUE_ID = "ID";

    public ServiceNowInsertCredentialsReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.INSTANCE_URL.getKey(),
            ReactorKeysEnum.CLIENT_ID.getKey(),
            ReactorKeysEnum.CLIENT_SECRET.getKey(),
            ReactorKeysEnum.KEY_NAME.getKey(),
            ReactorKeysEnum.REDIRECT_URL.getKey(),
            ReactorKeysEnum.SERVICE_NOW_SCOPE.getKey(),
            ReactorKeysEnum.SERVICE_NOW_USERINFO_URL.getKey(),
            ReactorKeysEnum.SERVICE_NOW_CODE_CHALLENGE_METHOD.getKey(),
            ReactorKeysEnum.SERVICE_NOW_BEANPROPS.getKey(),
            ReactorKeysEnum.SERVICE_NOW_JSON_PATTERN.getKey(),
            ReactorKeysEnum.SERVICE_NOW_LOGIN_ALLOWED.getKey(),
            ReactorKeysEnum.SERVICE_NOW_AUTOADD.getKey(),
            ReactorKeysEnum.SERVICE_NOW_ACCESSKEY_KEYS_ALLOWED.getKey()
        };
        this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
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

        String instanceURL = this.keyValue.get(this.keysToGet[0]);
        String clientId = this.keyValue.get(this.keysToGet[1]);
        String clientSecret = this.keyValue.get(this.keysToGet[2]);
        String keyName = this.keyValue.get(this.keysToGet[3]);
        String redirectURL = this.keyValue.get(this.keysToGet[4]);
        String scope = this.keyValue.get(this.keysToGet[5]);
        String userinfoURL = this.keyValue.get(this.keysToGet[6]);
        String codeChMethod = this.keyValue.get(this.keysToGet[7]);
        String beanProps = this.keyValue.get(this.keysToGet[8]);
        String jsonPattern = this.keyValue.get(this.keysToGet[9]);
        String loginAllowed = this.keyValue.get(this.keysToGet[10]);
        String autoAdd = this.keyValue.get(this.keysToGet[11]);
        String accessKeysAllowed = this.keyValue.get(this.keysToGet[12]);

        Timestamp dateCreated = new Timestamp(System.currentTimeMillis());
        UUID id = UUID.randomUUID();
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

            HashMap<String, Object> insertResult = insertData(
                serviceNowDB, tableName, id, instanceURL, clientId, clientSecret, dateCreated, keyName, createdBy,
                redirectURL, scope, userinfoURL, codeChMethod, beanProps, jsonPattern, loginAllowed, autoAdd, accessKeysAllowed
            );

            if (Boolean.FALSE.equals(insertResult.get("Data inserted successfully"))) {
                String msg = (String) insertResult.get("Error");
                throw new SemossPixelException(NounMetadata.getErrorNounMessage(msg));
            }

            String profileId = readData(
                serviceNowDB, tableName, instanceURL, clientId, clientSecret, dateCreated, keyName, createdBy,
                redirectURL, scope, userinfoURL, codeChMethod, beanProps, jsonPattern, loginAllowed, autoAdd, accessKeysAllowed
            );
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

    private String readData(IRDBMSEngine serviceNowDB, String tableName, String instanceURL, String clientId, String clientSecret, Timestamp dateCreated, String keyName, String createdBy, String redirectURL, String scope, String userinfoURL, String codeChMethod, String beanProps, String jsonPattern, String loginAllowed, String autoAdd, String accessKeysAllowed) {
        if (!isValidTableName(tableName)) {
            classLogger.error("Invalid table name");
            throw new IllegalArgumentException("Invalid table name");
        }

        String profileKey = null;
        String query = "SELECT " + SERVICENOW_UNIQUE_ID + " FROM " + tableName
            + " WHERE KEY_NAME=? AND INSTANCE_URL=? AND CLIENT_ID=? AND CLIENT_SECRET=? AND CREATED_BY=? AND DATE_CREATED=?"
            + " AND REDIRECT_URL=? AND SCOPE=? AND USER_INFO_URL=? AND CODE_CHALLENGE_METHOD=? AND BEANPROPS=? AND JSONPATTERN=? AND LOGIN_APPLICABLE=? AND AUTO_ADD=? AND ACCESS_KEYS_ALLOWED=?";

        try (Connection conn = serviceNowDB.makeConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, keyName);
            pstmt.setString(2, instanceURL);
            pstmt.setString(3, clientId);
            pstmt.setString(4, clientSecret);
            pstmt.setString(5, createdBy);
            pstmt.setTimestamp(6, dateCreated);
            pstmt.setString(7, redirectURL);
            pstmt.setString(8, scope);
            pstmt.setString(9, userinfoURL);
            pstmt.setString(10, codeChMethod);
            pstmt.setString(11, beanProps);
            pstmt.setString(12, jsonPattern);
            pstmt.setString(13, loginAllowed);
            pstmt.setString(14, autoAdd);
            pstmt.setString(15, accessKeysAllowed);

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

    private HashMap<String, Object> insertData(IRDBMSEngine serviceNowDB, String tableName, UUID id, String instanceURL, String clientId, String clientSecret, Timestamp dateCreated, String keyName, String createdBy, String redirectURL, String scope, String userinfoURL, String codeChMethod, String beanProps, String jsonPattern, String loginAllowed, String autoAdd, String accessKeysAllowed) {
        HashMap<String, Object> map = new HashMap<>();
        boolean flag = false;

        if (!isValidTableName(tableName)) {
            map.put("Data inserted successfully", false);
            map.put("Error", "Invalid table name");
            return map;
        }

        String checkQuery = "SELECT COUNT(*) AS CNT FROM " + tableName + " WHERE KEY_NAME = ? AND CREATED_BY = ?";
        try (Connection conn = serviceNowDB.makeConnection();
             PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {

            checkStmt.setString(1, keyName);
            checkStmt.setString(2, createdBy);

            try (ResultSet rs = checkStmt.executeQuery()) {
                if (rs.next() && rs.getInt("CNT") > 0) {
                    String msg = "Error: KEY_NAME '" + keyName + "' already exists for this user.";
                    map.put("Data inserted successfully", false);
                    map.put("Error", msg);
                    return map;
                }
            }

            String insertQuery = "INSERT INTO " + tableName
                + " (KEY_NAME, ID, INSTANCE_URL, CLIENT_ID, CLIENT_SECRET, CREATED_BY, DATE_CREATED, REDIRECT_URL, SCOPE, USER_INFO_URL, CODE_CHALLENGE_METHOD, BEANPROPS, JSONPATTERN, LOGIN_APPLICABLE, AUTO_ADD, ACCESS_KEYS_ALLOWED) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
                insertStmt.setString(1, keyName);
                insertStmt.setString(2, id.toString());
                insertStmt.setString(3, instanceURL);
                insertStmt.setString(4, clientId);
                insertStmt.setString(5, clientSecret);
                insertStmt.setString(6, createdBy);
                insertStmt.setTimestamp(7, dateCreated);
                insertStmt.setString(8, redirectURL);
                insertStmt.setString(9, scope);
                insertStmt.setString(10, userinfoURL);
                insertStmt.setString(11, codeChMethod);
                insertStmt.setString(12, beanProps);
                insertStmt.setString(13, jsonPattern);
                insertStmt.setString(14, loginAllowed);
                insertStmt.setString(15, autoAdd);
                insertStmt.setString(16, accessKeysAllowed);

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
        if (key.equals(ReactorKeysEnum.INSTANCE_URL.getKey())) {
            return "ServiceNow instance URL for API operations.";
        } else if (key.equals(ReactorKeysEnum.CLIENT_ID.getKey())) {
            return "Client ID used for ServiceNow API authentication.";
        } else if (key.equals(ReactorKeysEnum.CLIENT_SECRET.getKey())) {
            return "Client Secret used for ServiceNow API authentication.";
        } else if (key.equals(ReactorKeysEnum.KEY_NAME.getKey())) {
            return "Unique key name to identify this ServiceNow credential entry.";
        }
        return super.getDescriptionForKey(key);
    }
}
