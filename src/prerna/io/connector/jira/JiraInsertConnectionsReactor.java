package prerna.io.connector.jira;

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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class JiraInsertConnectionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraInsertConnectionsReactor.class);

	private static final String TABLE = "JIRA_CONNECTIONS";
	private static final String JIRA_UNIQUE_ID = "ID";

	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String SCOPE = "scope";
	private static final String USER_PROFILE_URL = "userProfileUrl";

	public JiraInsertConnectionsReactor() {
		this.keysToGet = new String[] { CLIENT_ID, CLIENT_SECRET, SCOPE, USER_PROFILE_URL };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
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

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String clientId = this.keyValue.get(CLIENT_ID);
		if (clientId == null || clientId.trim().isEmpty()) {
			throw new SemossPixelException("Missing required key: " + CLIENT_ID);
		}
		String clientSecret = this.keyValue.get(CLIENT_SECRET);
		if (clientSecret == null || clientSecret.trim().isEmpty()) {
			throw new SemossPixelException("Missing required key: " + CLIENT_SECRET);
		}
		String scope = this.keyValue.get(SCOPE);
		if (scope == null || scope.trim().isEmpty()) {
			throw new SemossPixelException("Missing required key: " + SCOPE);
		}
		String userProfileUrl = this.keyValue.get(USER_PROFILE_URL);
		if (userProfileUrl == null || userProfileUrl.trim().isEmpty()) {
			userProfileUrl = "https://api.atlassian.com/me";
		}
		HashMap<String, Object> responseMap = new HashMap<>();
		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);
			if (tableName == null) {
				throw new SemossPixelException("Required table '" + TABLE + "' does not exist in the database.");
			}
			IRDBMSEngine jiraDB = (RDBMSNativeEngine) database;
			UUID id = UUID.randomUUID();
			HashMap<String, Object> insertResult = insertData(jiraDB, tableName, id.toString(), clientId, clientSecret,
					scope, userProfileUrl);
			boolean inserted = Boolean.TRUE.equals(insertResult.get("status"));
			if (!inserted) {
				throw new SemossPixelException("Failed to insert data");
			}
			String profileId = readData(jiraDB, tableName, clientId, clientSecret);
			if (profileId != null && !profileId.isEmpty()) {
				responseMap.put("id", profileId);
				responseMap.put("success", true);
			} else {
				responseMap.put("id", null);
				responseMap.put("success", false);
			}
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while inserting Jira connection: " + e.getMessage());
		}
	}

	private String readData(IRDBMSEngine jiraDB, String tableName, String clientId, String clientSecret) {
		if (!TABLE.equals(tableName)) {
			throw new IllegalArgumentException("Invalid table name");
		}
		String profileKey = null;
		String query = "SELECT " + JIRA_UNIQUE_ID + " FROM " + tableName + " WHERE CLIENTID=? AND CLIENTSECRET=?";
		try (Connection conn = jiraDB.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, clientId);
			pstmt.setString(2, clientSecret);
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					profileKey = rs.getString(JIRA_UNIQUE_ID);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while reading Jira connection: " + e.getMessage());
		}
		return profileKey;
	}

	private HashMap<String, Object> insertData(IRDBMSEngine jiraDB, String tableName, String id, String clientId,
			String clientSecret, String scope, String userProfileUrl) {
		HashMap<String, Object> map = new HashMap<>();
		if (!TABLE.equals(tableName)) {
			throw new IllegalArgumentException("Invalid table name");
		}
		String checkQuery = "SELECT COUNT(*) FROM " + tableName + " WHERE CLIENTID=?";
		try (Connection conn = jiraDB.getConnection();
				PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {
			checkStmt.setString(1, clientId);
			try (ResultSet rs = checkStmt.executeQuery()) {
				if (rs.next() && rs.getInt(1) > 0) {
					throw new SemossPixelException("A connection with CLIENTID '" + clientId + "' already exists.");
				}
			}

			String insertQuery = "INSERT INTO " + tableName
					+ " (ID, CLIENTID, CLIENTSECRET, SCOPE, USERPROFILEURL) VALUES (?, ?, ?, ?, ?)";
			try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
				insertStmt.setString(1, id);
				insertStmt.setString(2, clientId);
				insertStmt.setString(3, clientSecret);
				insertStmt.setString(4, scope);
				insertStmt.setString(5, userProfileUrl);
				int rowsInserted = insertStmt.executeUpdate();
				if (rowsInserted == 0) {
					throw new SemossPixelException("Failed to insert Jira connection.");
				} else {
					map.put("status", true);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while inserting Jira connection: " + e.getMessage());
		}
		return map;
	}

	@Override
	public String getReactorDescription() {
		return "Create a Jira OAuth connection profile in SEMOSS security storage. Use this once per Atlassian app to save clientId, clientSecret, scope, and optional user profile URL for later Jira API calls.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CLIENT_ID)) {
			return "Atlassian OAuth app Client ID.";
		} else if (key.equals(CLIENT_SECRET)) {
			return "Atlassian OAuth app Client Secret.";
		} else if (key.equals(SCOPE)) {
			return "OAuth scopes e.g. read:jira-work write:jira-work read:me offline_access";
		} else if (key.equals(USER_PROFILE_URL)) {
			return "Optional. Defaults to https://api.atlassian.com/me";
		}
		return super.getDescriptionForKey(key);
	}
}
