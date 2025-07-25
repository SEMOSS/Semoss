package prerna.io.connector.jira;

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

public class JiraInsertApikeyReactor extends AbstractReactor {

	public static final String JIRA_UNIQUE_ID = "ID";
	private static final String TABLE = "JIRA_USER";
	private static final Logger classLogger = LogManager.getLogger(JiraInsertApikeyReactor.class);
	static IRDBMSEngine jiraDB;

	public JiraInsertApikeyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.USERID.getKey(), ReactorKeysEnum.API_KEY.getKey(),
				ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.KEY_NAME.getKey() , ReactorKeysEnum.PROJECT.getKey()};
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
		String userId = this.keyValue.get(this.keysToGet[0]);
		String apiToken = this.keyValue.get(this.keysToGet[1]);
		String url = this.keyValue.get(this.keysToGet[2]);
		String keyName = this.keyValue.get(this.keysToGet[3]);
		String project = this.keyValue.get(this.keysToGet[4]);
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
			HashMap<String, Object> insertResult = insertData(jiraDB, tableName, userId, apiToken, url, date, lused,
					insightusername, keyName, id, project);

			if (Boolean.FALSE.equals(insertResult.get("Data inserted successfully"))) {
				msg = (String) insertResult.get("Error");
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(msg));
			}

			profileId = readData(jiraDB, tableName, userId, apiToken, url, date, lused, keyName, project);
			if (profileId != null && !profileId.isEmpty()) {
				responseMap.put("id", profileId);
				responseMap.put("success", true);
			} else {
				responseMap.put("id", null);
				responseMap.put("Success", false);
			}
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in executing the reactor"+e.getMessage();
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(error));
		}
	}

	/**
	 * To return primary key of the user after inserting data in DB, now also using project.
	 */
	private String readData(IRDBMSEngine jiraDB, String tableName, String userId, String apiKey, String url,
	        Timestamp date, Timestamp lused, String keyName, String project) {
	    jiraDB = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
	    String profileKey = null;

	    if (!isValidTableName(tableName)) {
	        classLogger.error("Invalid table name");
	        throw new IllegalArgumentException("Invalid table name");
	    }

	    String query = "SELECT " + JIRA_UNIQUE_ID + " FROM " + tableName
	            + " WHERE USER_ID=? AND API_KEY=? AND URL=? AND DATE_CREATED=? AND DATE_LAST_USED=? AND KEY_NAME=? AND PROJECT=?";

	    try (Connection conn = jiraDB.makeConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
	        pstmt.setString(1, userId);
	        pstmt.setString(2, apiKey);
	        pstmt.setString(3, url);
	        pstmt.setTimestamp(4, date);
	        pstmt.setTimestamp(5, lused);
	        pstmt.setString(6, keyName);
	        pstmt.setString(7, project);

	        try (ResultSet rs = pstmt.executeQuery()) {
	            if (rs.next()) {
	                profileKey = rs.getString(JIRA_UNIQUE_ID);
	            }
	        }
	    } catch (Exception e) {
	        classLogger.error(Constants.STACKTRACE, e);
	        throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
	    }
	    return profileKey;
	}

	private boolean isValidTableName(String tableName) {
		return TABLE.equals(tableName);
	}

	/**
	 * To insert user data in DB, ensuring KEY_NAME is unique, and also storing project.
	 */
	private HashMap<String, Object> insertData(IRDBMSEngine jiraDB, String tableName, String userId, String apiToken,
	        String url, Timestamp dateCreated, Timestamp lastUsed, String insightusername, String keyName, UUID id, String project) {
	    jiraDB = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
	    HashMap<String, Object> map = new HashMap<>();
	    boolean flag = false;
	    ResultSet rs = null;

	    try (Connection conn = jiraDB.makeConnection()) {
	        // Validate table name
	        if (!isValidTableName(tableName)) {
	            map.put("Data inserted successfully", false);
	            map.put("Error", "Invalid table name");
	            return map;
	        }

	        // Check if KEY_NAME already exists for this user
	        String checkQuery = "SELECT COUNT(*) AS CNT FROM " + tableName + " WHERE KEY_NAME = ? AND USER_ID = ?";
	        try (PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {
	            checkStmt.setString(1, keyName);
	            checkStmt.setString(2, userId);
	            rs = checkStmt.executeQuery();
	            if (rs.next() && rs.getInt("CNT") > 0) {
	                String msg = "Error: KEY_NAME '" + keyName + "' already exists for this user.";
	                map.put("Data inserted successfully", false);
	                map.put("Error", msg);
	                return map;
	            }
	        } finally {
	            if (rs != null) {
	                try {
	                    rs.close();
	                } catch (Exception ex) {
	                    classLogger.error(Constants.STACKTRACE, ex);
	                    throw new SemossPixelException(NounMetadata.getErrorNounMessage(ex.getMessage()));
	                }
	            }
	        }

	        // Insert data using parameterized query, now including PROJECT
	        String insertQuery = "INSERT INTO " + tableName
	                + " (ID, API_KEY, USER_ID, URL, DATE_CREATED, DATE_LAST_USED, CREATED_BY, KEY_NAME, PROJECT) "
	                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
	        try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
	            insertStmt.setString(1, id.toString());
	            insertStmt.setString(2, apiToken);
	            insertStmt.setString(3, userId);
	            insertStmt.setString(4, url);
	            insertStmt.setTimestamp(5, dateCreated);
	            insertStmt.setTimestamp(6, lastUsed);
	            insertStmt.setString(7, insightusername);
	            insertStmt.setString(8, keyName);
	            insertStmt.setString(9, project);

	            int rowsInserted = insertStmt.executeUpdate();
	            flag = rowsInserted > 0;
	            map.put("Data inserted successfully", flag);
	        }

	    } catch (Exception e) {
	        classLogger.error(Constants.STACKTRACE, e);
	        map.put("Data inserted unsuccessfully", flag);
	        map.put("Error", e.getMessage());
	        throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
	    }
	    return map;
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used for inserting Jira data in DB of the user like Username, Apikey, URL and keyName.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.USERID.getKey())) {
			return "User ID (email) of the user who intends to perform Jira operations.";
		} else if (key.equals(ReactorKeysEnum.API_KEY.getKey())) {
			return "API key of the user which will be used for authentication for various Jira operations.";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "Base URL using which URLs for create, delete, list issues etc. will be created.";
		} else if (key.equals(ReactorKeysEnum.KEY_NAME.getKey())) {
			return "Key name for each entry to identify user ID while performing different Jira operations.";
		}
		return super.getDescriptionForKey(key);
	}
}
