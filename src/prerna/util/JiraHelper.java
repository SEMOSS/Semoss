package prerna.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.Fields;
import prerna.engine.impl.model.IssueType;
import prerna.engine.impl.model.JiraRequestBodyModel;
import prerna.engine.impl.model.Project;
import prerna.reactor.model.JiraTicketDetails;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;

public class JiraHelper {

	public static final String JIRA_LIST_ISSUE_URL = "/rest/api/2/search?jql=project=";
	public static final String JIRA_UNIQUE_ID = "KEY_NAME";
	public static final String JIRA_CREATE_DELETE_ISSUE_URL = "/rest/api/2/issue";
	public static final String JIRA_GETALL_PROJECTS_URL = "/rest/api/2/project";
	private static final String TABLE = "JIRA_USER";

	private static final Logger classLogger = LogManager.getLogger(JiraHelper.class);

	public static IRDBMSEngine jiraDB;

	static {
		try {
			jiraDB = (IRDBMSEngine) Utility.getDatabase(Constants.SECURITY_DB);
		} catch (Exception e) {
			classLogger.error("Failed to initialize jiraDB", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Allows dependency injection for the Jira DB engine (for testing/mocking).
	 * 
	 * @param db The IRDBMSEngine instance to use.
	 */
	public static void setJiraDB(IRDBMSEngine db) {
		jiraDB = db;
	}

	/**
	 * Returns the JIRA_USER table name if it exists in the DB.
	 * 
	 * @return Table name or null if not found.
	 */
	private static String getTableName() {
		try {
			List<String> tables = jiraDB.getPixelConcepts();
			for (String tbl : tables) {
				if (TABLE.equals(tbl)) {
					return tbl;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to retrieve table name in getTableName():"+e.getMessage()));
		}
		return null;
	}

	/**
	 * Checks if a user with the given keyName exists in the JIRA_USER table.
	 * 
	 * @param keyName The user key to check.
	 * @return true if user exists, false otherwise.
	 */
	private static boolean userExists(String keyName) {
		String tableName = getTableName();
		if (tableName == null)
			return false;
		String checkQuery = "SELECT 1 FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = ?";
		try (Connection conn = jiraDB.makeConnection(); PreparedStatement pstmt = conn.prepareStatement(checkQuery)) {
			pstmt.setString(1, keyName);
			try (ResultSet rs = pstmt.executeQuery()) {
				return rs.next();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("Unable to verify user presence:"+e.getMessage()));
		}
	}

	/**
	 * Retrieves a specific field value for a user from the JIRA_USER table.
	 * 
	 * @param keyName The user key.
	 * @param field   The field/column name.
	 * @return The field value as String, or null if not found.
	 */
	private static String getFieldFromDB(String keyName, String field) {
		String tableName = getTableName();
		if (tableName == null)
			return null;
		String query = "SELECT " + field + " FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = ?";
		try (Connection conn = jiraDB.makeConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, keyName);
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					return rs.getString(field);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Failed to retrieve field '" + field + "' for user '" + keyName + "' in getFieldFromDB()."));
		}
		return null;
	}

	/**
	 * Lists all JIRA issues for the project associated with the given user key.
	 * 
	 * @param keyName The user key (JIRA_UNIQUE_ID).
	 * @return NounMetadata containing a list of JiraTicketDetails.
	 */
	public static NounMetadata listIssue(String keyName) {
		try {
			if (!userExists(keyName)) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(keyName + " is not present in DB"));
			}
			String apiKey = getFieldFromDB(keyName, "API_KEY");
			String username = getFieldFromDB(keyName, "USER_ID");
			String urlBase = getFieldFromDB(keyName, "URL");
			String projectName = getFieldFromDB(keyName, "PROJECT");

			if (apiKey == null || username == null || urlBase == null || projectName == null) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User credentials, URL, or project missing in DB"));
			}

			String normalizedUsername = username.trim();
			String normalizedApiKey = apiKey.trim();
			if (!urlBase.matches("^https?://.*")) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Invalid JIRA URL format in DB"));
			}
			if (!projectName.matches("^[A-Za-z0-9_-]+$")) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Invalid project name format in DB"));
			}

			String auth = normalizedUsername + ":" + normalizedApiKey;
			String encodeToString = Base64.getEncoder()
					.encodeToString(auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			Map<String, String> map = new HashMap<>();
			map.put("Authorization", "Basic " + encodeToString);

			String url = urlBase + JIRA_LIST_ISSUE_URL + projectName;
			String response = HttpHelperUtility.getRequest(url, map, null, null, null);

			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode root = objectMapper.readTree(response);
			JsonNode issues = root.path("issues");

			List<JiraTicketDetails> jiraIssueDetails = new ArrayList<>();
			for (JsonNode issue : issues) {
				String id = issue.path("id").asText();
				String link = issue.path("self").asText();
				String summary = issue.path("fields").path("summary").asText();
				jiraIssueDetails.add(new JiraTicketDetails(id, link, summary));
			}

			return new NounMetadata(jiraIssueDetails, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in listIssue for user '{}': {}", keyName, e.getMessage(), e);
			throw new SemossPixelException(NounMetadata
					.getErrorNounMessage("Failed to list issues for project associated with '" + keyName + "'."));
		}
	}

	/**
	 * Creates a new JIRA issue for the project associated with the given user key.
	 * 
	 * @param summary     The issue summary/title.
	 * @param description The issue description.
	 * @param istype      The issue type (e.g., "Task", "Bug").
	 * @param keyName     The user key (JIRA_UNIQUE_ID).
	 * @return NounMetadata containing JiraTicketDetails for the created issue.
	 */
	public static NounMetadata createIssue(String summary, String description, String istype, String keyName) {
		try {
			if (!userExists(keyName)) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User id " + keyName + " is not present in DB"));
			}
			String apiKey = getFieldFromDB(keyName, "API_KEY");
			String username = getFieldFromDB(keyName, "USER_ID");
			String urlBase = getFieldFromDB(keyName, "URL");
			String projectName = getFieldFromDB(keyName, "PROJECT");

			if (apiKey == null || username == null || urlBase == null || projectName == null) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User credentials, URL, or project missing in DB"));
			}

			String normalizedUsername = username.trim();
			String normalizedApiKey = apiKey.trim();
			if (!urlBase.matches("^https?://.*")) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Invalid JIRA URL format in DB"));
			}
			if (!projectName.matches("^[A-Za-z0-9_-]+$")) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Invalid project name format in DB"));
			}

			String auth = normalizedUsername + ":" + normalizedApiKey;
			String encodeToString = Base64.getEncoder()
					.encodeToString(auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			Map<String, String> map = new HashMap<>();
			map.put("Authorization", "Basic " + encodeToString);

			String url = urlBase + JIRA_CREATE_DELETE_ISSUE_URL;

			Project project = new Project();
			project.setKey(projectName);
			IssueType issuetype = new IssueType();
			issuetype.setName(istype);
			Fields fields = new Fields();
			fields.setProject(project);
			fields.setSummary(summary);
			fields.setDescription(description);
			fields.setIssuetype(issuetype);

			JiraRequestBodyModel jiraRequestBodyModel = new JiraRequestBodyModel();
			jiraRequestBodyModel.setFields(fields);

			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			String body = gson.toJson(jiraRequestBodyModel);

			String nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, map, body,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, Map.class);
			Object id = responseMap.get("id");
			Object link = responseMap.get("self");

			JiraTicketDetails jiraDetail = new JiraTicketDetails(id != null ? id.toString() : null,
					link != null ? link.toString() : null, null);

			return new NounMetadata(jiraDetail, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in createIssue for user '{}': {}", keyName, e.getMessage(), e);
			throw new SemossPixelException(NounMetadata
					.getErrorNounMessage("Failed to create issue for project associated with '" + keyName + "'."));
		}
	}

	/**
	 * Deletes a JIRA issue by its ID, only if it belongs to the user's project.
	 * 
	 * @param jiraId  The JIRA issue ID.
	 * @param keyName The user key (JIRA_UNIQUE_ID).
	 * @return NounMetadata with {"success": true} if deleted.
	 */
	public static NounMetadata deleteIssue(String jiraId, String keyName) {
		try {
			ObjectMapper objectMapper = new ObjectMapper();

			if (!userExists(keyName)) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User id " + keyName + " is not present in DB"));
			}
			String apiKey = getFieldFromDB(keyName, "API_KEY");
			String username = getFieldFromDB(keyName, "USER_ID");
			String urlBase = getFieldFromDB(keyName, "URL");
			String project = getFieldFromDB(keyName, "PROJECT");

			if (apiKey == null || username == null || urlBase == null || project == null) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User credentials, URL, or project missing in DB"));
			}

			String normalizedUsername = username.trim();
			String normalizedApiKey = apiKey.trim();
			if (!urlBase.matches("^https?://.*")) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Invalid JIRA URL format in DB"));
			}
			if (!project.matches("^[A-Za-z0-9_-]+$")) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Invalid project name format in DB"));
			}

			String auth = normalizedUsername + ":" + normalizedApiKey;
			String encodeToString = Base64.getEncoder()
					.encodeToString(auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			Map<String, String> map = new HashMap<>();
			map.put("Authorization", "Basic " + encodeToString);

			String issueUrl = urlBase + JIRA_CREATE_DELETE_ISSUE_URL + "/" + jiraId;
			String issueResponse = HttpHelperUtility.getRequest(issueUrl, map, null, null, null);

			JsonNode root = objectMapper.readTree(issueResponse);
			String projectKey = root.path("fields").path("project").path("key").asText();

			if (!project.equalsIgnoreCase(projectKey)) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("Issue " + jiraId + " does not belong to project " + project));
			}

			HttpHelperUtility.deleteRequestStringBody(issueUrl, map, null, null, null);

			Map<String, Object> response = new HashMap<>();
			response.put("success", true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in deleteIssue for user '{}', issue '{}': {}", keyName, jiraId, e.getMessage(), e);
			String project = null;
			try {
				project = getFieldFromDB(keyName, "PROJECT");
			} catch (Exception ex) {
			}
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Failed to delete issue for project '" + project + "' and user '" + keyName + "'."));
		}
	}

	/**
	 * Retrieves all JIRA project keys accessible to the user via JIRA REST API.
	 * 
	 * @param urlBase The JIRA instance base URL.
	 * @param userId  The JIRA username/email.
	 * @param apiKey  The JIRA API key.
	 * @return NounMetadata containing a list of project keys.
	 */
	public static NounMetadata getAllProjects(String urlBase, String userId, String apiKey) {
		try {
			if (userId == null || userId.isEmpty() || apiKey == null || apiKey.isEmpty() || urlBase == null
					|| urlBase.isEmpty()) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User ID, API key, or URL is missing or empty"));
			}
			String normalizedUserId = userId.trim();
			String normalizedApiKey = apiKey.trim();
			if (!urlBase.matches("^https?://.*")) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Invalid JIRA URL format"));
			}

			String auth = normalizedUserId + ":" + normalizedApiKey;
			String encodeToString = Base64.getEncoder()
					.encodeToString(auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));

			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Basic " + encodeToString);

			String url = urlBase + JIRA_GETALL_PROJECTS_URL;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JSONArray projResponse = new JSONArray(response);
			List<String> projList = new ArrayList<>();
			for (int i = 0; i < projResponse.length(); i++) {
				JSONObject project = projResponse.getJSONObject(i);
				projList.add(project.getString("key"));
			}
			return new NounMetadata(projList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in getAllProjects: {}", e.getMessage(), e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("Failed to retrieve all projects."));
		}
	}

	/**
	 * Returns a list of supported JIRA issue types.
	 * 
	 * @return NounMetadata containing a list of issue type names.
	 */
	public static NounMetadata issueType() {
		ArrayList<String> jiraIssues = new ArrayList<>();
		jiraIssues.add("Epic");
		jiraIssues.add("Story");
		jiraIssues.add("Task");
		jiraIssues.add("Bug");
		jiraIssues.add("Subtask");
		return new NounMetadata(jiraIssues, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	/**
	 * Deletes a user record from the JIRA_USER table.
	 * 
	 * @param keyName The user key (JIRA_UNIQUE_ID).
	 * @return NounMetadata with {"success": true} if deleted.
	 */
	public static NounMetadata deleteRecordForUser(String keyName) {
		try {
			String tableName = getTableName();
			if (tableName == null) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Table not found in database."));
			}
			if (!userExists(keyName)) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User id " + keyName + " is not present in DB"));
			}
			String deleteQuery = "DELETE FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = ?";
			try (Connection conn = jiraDB.makeConnection();
					PreparedStatement pstmt = conn.prepareStatement(deleteQuery)) {
				pstmt.setString(1, keyName);
				pstmt.executeUpdate();
			}
			Map<String, Object> response = new HashMap<>();
			response.put("success", true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in deleteRecordForUser for user '{}': {}", keyName, e.getMessage(), e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to delete record for user '" + keyName + "'."));
		}
	}
}
