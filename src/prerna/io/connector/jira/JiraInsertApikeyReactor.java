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

	private static final Logger classLogger = LogManager.getLogger(JiraInsertApikeyReactor.class);

	private static final String JIRA_UNIQUE_ID = "ID";
	private static final String TABLE = "JIRA_USER";
	private static final String USER_ID = "userid";
	private static final String KEY_NAME = "keyname";

	static IRDBMSEngine jiraDB;

	public JiraInsertApikeyReactor() {
		this.keysToGet = new String[] { USER_ID, ReactorKeysEnum.API_KEY.getKey(), ReactorKeysEnum.URL.getKey(),
				KEY_NAME, ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String userId = this.keyValue.get(USER_ID);
		String apiToken = this.keyValue.get(ReactorKeysEnum.API_KEY.getKey());
		String url = this.keyValue.get(ReactorKeysEnum.URL.getKey());
		String keyName = this.keyValue.get(KEY_NAME);
		String project = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());

		long now = System.currentTimeMillis();
		Timestamp date = new Timestamp(now);
		Timestamp lused = new Timestamp(now);

		User user = this.insight.getUser();
		String insightusername = user.getPrimaryLoginToken().getUsername();

		HashMap<Object, Object> responseMap = new HashMap<>();
		String profileId = null;
		String msg = null;

		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);
			if (tableName == null) {
				throw new SemossPixelException("Jira user table not found in database.");
			}

			UUID id = UUID.randomUUID();
			HashMap<String, Object> insertResult = insertData(jiraDB, tableName, userId, apiToken, url, date, lused, insightusername, keyName, id, project);

			if (insertResult.containsKey("Data inserted successfully") && (boolean) insertResult.get("Data inserted successfully")) {
				profileId = readData(jiraDB, tableName, userId, apiToken, url, date, lused, keyName, project);
				if (profileId != null && !profileId.isEmpty()) {
					responseMap.put("id", profileId);
					responseMap.put("success", true);
				} else {
					responseMap.put("id", null);
					responseMap.put("success", false);
				}
			} else {
				classLogger.error("Failed to insert data into database.");
				throw new SemossPixelException("Failed to insert data into database.");
			}
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while inserting API KEY details in JIRA DB. Error message: " + e.getMessage());
		}
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

		try (Connection conn = jiraDB.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
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

	private HashMap<String, Object> insertData(IRDBMSEngine jiraDB, String tableName, String userId, String apiToken,
			String url, Timestamp dateCreated, Timestamp lastUsed, String insightusername, String keyName, UUID id,
			String project) {
		jiraDB = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
		HashMap<String, Object> map = new HashMap<>();
		boolean flag = false;
		ResultSet rs = null;

		try (Connection conn = jiraDB.getConnection()) {
			if (!isValidTableName(tableName)) {
				throw new IllegalArgumentException("Invalid table name");
			}

			String checkQuery = "SELECT COUNT(*) AS CNT FROM " + tableName + " WHERE KEY_NAME = ? AND USER_ID = ?";
			try (PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {
				checkStmt.setString(1, keyName);
				checkStmt.setString(2, userId);
				rs = checkStmt.executeQuery();
				if (rs.next() && rs.getInt("CNT") > 0) {
					throw new SemossPixelException("A record with the same key name already exists for this user.");
				}
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (Exception ex) {
						classLogger.error(Constants.STACKTRACE, ex);
						throw new SemossPixelException(ex.getMessage());
					}
				}
			}

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
			throw new SemossPixelException("An error occurred while inserting data. Error message: " + e.getMessage());
		}
		return map;
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used for inserting Jira data in DB of the user like Username, Apikey, URL and keyName.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(USER_ID)) {
			return "User ID (email) of the user who intends to perform Jira operations.";
		} else if (key.equals(ReactorKeysEnum.API_KEY.getKey())) {
			return "The api key of the token created by user to interact with JIRA Dashboard";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "The Jira URL on which all projects are present and tickets can be created";
		} else if (key.equals(KEY_NAME)) {
			return "The keyname of the connection from DB through which details can be fetched of a user.";
		} else if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "Name of the project on JIRA Dashboard";
		}
		return super.getDescriptionForKey(key);
	}
}
