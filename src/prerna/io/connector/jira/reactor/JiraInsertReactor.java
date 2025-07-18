package prerna.io.connector.jira.reactor;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class JiraInsertReactor extends AbstractReactor {

    public static final String JIRA_UNIQUE_ID = "ID";
    private static final String TABLE = "JIRA_USER";
    private static final Logger classLogger = LogManager.getLogger(JiraInsertReactor.class);

    public JiraInsertReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.USERID.getKey(),
            ReactorKeysEnum.API_KEY.getKey(),
            ReactorKeysEnum.URL.getKey(),
            ReactorKeysEnum.KEY_NAME.getKey()
        };
        this.keyRequired = new int[] { 1, 1, 1, 1 };
    }

    // Centralized table name lookup
    private String getTableName(IDatabaseEngine database) {
        try {
            List<String> tables = database.getPixelConcepts();
            for (String tbl : tables) {
                if (TABLE.equals(tbl)) {
                    return tbl;
                }
            }
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        }
        return null;
    }

    @Override
    public NounMetadata execute() {
        this.organizeKeys();
        String userId = this.keyValue.get(this.keysToGet[0]);
        String apiToken = this.keyValue.get(this.keysToGet[1]);
        String url = this.keyValue.get(this.keysToGet[2]);
        String keyName = this.keyValue.get(this.keysToGet[3]);
        long now = System.currentTimeMillis();
        Timestamp date = new Timestamp(now);
        Timestamp lused = new Timestamp(now);
        HashMap<Object, Object> responseMap = new HashMap<>();
        User user = this.insight.getUser();
        String insightusername = user.getPrimaryLoginToken().getUsername();
        String profileId = null;
        String msg = null;

        try {
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            String tableName = getTableName(database);
            if (tableName == null) {
                msg = "Jira user table not found in database.";
                responseMap.put("Data inserted successfully", false);
                responseMap.put("Error", msg);
                return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }

            UUID id = UUID.randomUUID();
            HashMap<String, Object> insertResult = insertData(database, tableName, userId, apiToken, url, date, lused, insightusername, keyName, id);

            if (Boolean.FALSE.equals(insertResult.get("Data inserted successfully"))) {
                msg = (String) insertResult.get("Error");
                responseMap.put("Data inserted successfully", false);
                responseMap.put("Error", msg);
                return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }

            profileId = readData(database, tableName, userId, apiToken, url, date, lused, keyName);
            if (profileId != null && !profileId.isEmpty()) {
                responseMap.put("id", profileId);
                responseMap.put("Success", true);
            } else {
                responseMap.put("id", null);
                responseMap.put("Success", false);
            }
            return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            String error = "Error in executing the reactor";
            msg = e.getMessage();
            return new NounMetadata(error + ": " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        }
    }

    /**
     * To return primary key of the user after inserting data in DB
     */
    private String readData(IDatabaseEngine database, String tableName, String userId, String apiKey, String url, Timestamp date, Timestamp lused, String keyName) {
        String profileKey = null;
        ResultSet rs = null;
        try {
            String query = "SELECT " + JIRA_UNIQUE_ID + " FROM " + tableName + " WHERE API_KEY='" + apiKey
                    + "' AND URL='" + url + "' AND DATE_CREATED='" + date + "' AND DATE_LAST_USED='" + lused
                    + "' AND KEY_NAME='" + keyName + "'";
            HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
            Object rsObj = hashmap.get("RESULTSET_OBJECT");
            if (rsObj instanceof ResultSet) {
                rs = (ResultSet) rsObj;
                if (rs.next()) {
                    profileKey = rs.getString(JIRA_UNIQUE_ID);
                }
            }
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (Exception ex) {
                classLogger.error("Error closing ResultSet in readData", ex);
            }
        }
        return profileKey;
    }

    /**
     * To insert user data in DB, ensuring KEY_NAME is unique
     */
    private static HashMap<String, Object> insertData(IDatabaseEngine database, String tableName, String userId, String apiToken, String url, Timestamp dateCreated, Timestamp lastUsed, String insightusername, String keyName, UUID id) {
        HashMap<String, Object> map = new HashMap<>();
        boolean flag = false;
        ResultSet rs = null;
        try {
            // Check if KEY_NAME already exists
            String checkQuery = "SELECT COUNT(*) AS CNT FROM " + tableName + " WHERE KEY_NAME = '" + keyName + "'";
            HashMap<String, String> result = (HashMap<String, String>) database.execQuery(checkQuery);
            Object rsObj = result.get("RESULTSET_OBJECT");
            if (rsObj instanceof ResultSet) {
                rs = (ResultSet) rsObj;
                if (rs.next() && rs.getInt("CNT") > 0) {
                    String msg = "Error: KEY_NAME '" + keyName + "' already exists.";
                    map.put("Data inserted successfully", false);
                    map.put("Error", msg);
                    return map;
                }
            }
            if (rs != null) rs.close();

            String insertQuery = "INSERT INTO " + tableName
                + " (ID, API_KEY, USER_ID, URL, DATE_CREATED, DATE_LAST_USED, CREATED_BY, KEY_NAME) "
                + "VALUES ('" + id + "','" + apiToken + "','" + userId + "','" + url + "','" + dateCreated + "','" + lastUsed
                + "','" + insightusername + "','" + keyName + "')";
            database.insertData(insertQuery);
            flag = true;
            map.put("Data inserted successfully", flag);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            map.put("Data inserted successfully", flag);
            map.put("Error", e.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (Exception ex) {
                classLogger.error("Error closing ResultSet in insertData", ex);
            }
        }
        return map;
    }

    @Override
    public String getReactorDescription() {
        return "This reactor is used for inserting Jira data in DB of the user like Username, Apikey, URL and keyName";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.USERNAME.getKey())) {
            return "username(emaild) of the user who intends to perform Jira operations" + ReactorKeysEnum.USERNAME.getKey();
        } else if (key.equals(ReactorKeysEnum.API_KEY.getKey())) {
            return "Api key of the user which will be used for authentication for various Jira Operations" + ReactorKeysEnum.API_KEY.getKey();
        } else if (key.equals(ReactorKeysEnum.URL.getKey())) {
            return "Base URL using which url for create, delete, list issues etc will be created" + ReactorKeysEnum.URL.getKey();
        } else if (key.equals(ReactorKeysEnum.KEY_NAME.getKey())) {
            return "Key name for each entry to identify userid while performing different Jira operations" + ReactorKeysEnum.KEY_NAME.getKey();
        }
        return super.getDescriptionForKey(key);
    }
}
