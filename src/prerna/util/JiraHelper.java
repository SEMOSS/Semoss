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

	public static void setJiraDB(IRDBMSEngine db) {
		jiraDB = db;
	}

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
			throw new SemossPixelException(NounMetadata
					.getErrorNounMessage("Failed to retrieve table name in getTableName(): " + e.getMessage()));
		}
		return null;
	}

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
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to verify user presence. " + e.getMessage()));
		}
	}

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
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("Failed to retrieve field '" + field
					+ "' for user '" + keyName + "' in getFieldFromDB():" + e.getMessage()));
		}
		return null;
	}

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

			String auth = username + ":" + apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
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
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to list issues for project associated with '" + keyName
							+ "' in listIssue():" + e.getMessage()));
		}
	}

	public static NounMetadata createIssue(String summary, String description, String istype, String keyName) {
		try {
			if (!userExists(keyName)) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User id " + keyName + " is not present in DB"));
			}

			String apiKey = getFieldFromDB(keyName, "API_KEY");
			String username = getFieldFromDB(keyName, "USER_ID");
			String urlBase = getFieldFromDB(keyName, "URL");
			String projectName = getFieldFromDB(keyName, "PROJECT"); // Fetch project from DB

			if (apiKey == null || username == null || urlBase == null || projectName == null) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User credentials, URL, or project missing in DB"));
			}

			String auth = username + ":" + apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
			Map<String, String> map = new HashMap<>();
			map.put("Authorization", "Basic " + encodeToString);

			String url = urlBase + JIRA_CREATE_DELETE_ISSUE_URL;

			Project project = new Project();
			project.setKey(projectName); // Use project fetched from DB
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

			// Create JiraIssueMetadata object
			JiraTicketDetails jiraDetail = new JiraTicketDetails(id != null ? id.toString() : null,
					link != null ? link.toString() : null, null);

			// Return as part of NounMetadata
			return new NounMetadata(jiraDetail, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to create issue for project associated with'" + keyName
							+ "' in createIssue():" + e.getMessage()));
		}
	}

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
			String project = getFieldFromDB(keyName, "PROJECT"); // Fetch project from DB

			if (apiKey == null || username == null || urlBase == null || project == null) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User credentials, URL, or project missing in DB"));
			}

			String auth = username + ":" + apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
			Map<String, String> map = new HashMap<>();
			map.put("Authorization", "Basic " + encodeToString);

			// Step 1: Get issue details to verify project
			String issueUrl = urlBase + JIRA_CREATE_DELETE_ISSUE_URL + "/" + jiraId;
			String issueResponse = HttpHelperUtility.getRequest(issueUrl, map, null, null, null);

			// Parse the response to check project key
			JsonNode root = objectMapper.readTree(issueResponse);
			String projectKey = root.path("fields").path("project").path("key").asText();

			if (!project.equalsIgnoreCase(projectKey)) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("Issue " + jiraId + " does not belong to project " + project));
			}

			// Step 2: Delete the issue
			HttpHelperUtility.deleteRequestStringBody(issueUrl, map, null, null, null);

			Map<String, Object> response = new HashMap<>();
			response.put("success", true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			// Fetch project for error message if possible
			String project = null;
			try {
				project = getFieldFromDB(keyName, "PROJECT");
			} catch (Exception ex) {
			}
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to delete issue '" + jiraId + "' for project '" + project
							+ "' and user '" + keyName + "' in deleteIssue(): " + e.getMessage()));
		}
	}

	public static NounMetadata truncateData(String keyName) {
		try {
			String tableName = getTableName();
			if (tableName == null) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage("Table not found in database."));
			}

			if (!userExists(keyName)) {
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(
						"User id " + keyName + " is not present in DB. Truncate operation aborted."));
			}

			String truncateQuery = "DELETE FROM " + tableName;
			try (Connection conn = jiraDB.makeConnection();
					PreparedStatement pstmt = conn.prepareStatement(truncateQuery)) {
				pstmt.executeUpdate();
			}

			Map<String, Object> response = new HashMap<>();
			response.put("success", true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Failed to truncate table for user '" + keyName + "' in truncateData(): " + e.getMessage()));
		}
	}

	public static NounMetadata getAllProjects(String urlBase, String userId, String apiKey) {

		try {
			// Validate input parameters
			if (userId == null || userId.isEmpty() || apiKey == null || apiKey.isEmpty() || urlBase == null
					|| urlBase.isEmpty()) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("User ID, API key, or URL is missing or empty"));
			}

			// Generate Basic Auth token using userId and apiKey
			String auth = userId + ":" + apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());

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
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata
					.getErrorNounMessage("Failed to retrieve all projects in getAllProjects(): " + e.getMessage()));
		}
	}

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
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Failed to delete record for user '" + keyName + "' in deleteRecordForUser(): " + e.getMessage()));
		}
	}

	public static NounMetadata issueType() {
		ArrayList<String> jiraIssues = new ArrayList<>();
		jiraIssues.add("Epic");
		jiraIssues.add("Story");
		jiraIssues.add("Task");
		jiraIssues.add("Bug");
		jiraIssues.add("Subtask");
		return new NounMetadata(jiraIssues, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static List<String> getAllUserIds() {
		List<String> userIds = new ArrayList<>();
		try {
			String tableName = getTableName();
			if (tableName == null)
				return userIds;
			String query = "SELECT " + JIRA_UNIQUE_ID + " FROM " + tableName;
			try (Connection conn = jiraDB.makeConnection();
					PreparedStatement pstmt = conn.prepareStatement(query);
					ResultSet rs = pstmt.executeQuery()) {
				while (rs.next()) {
					userIds.add(rs.getString(JIRA_UNIQUE_ID));
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata
					.getErrorNounMessage("Failed to retrieve user IDs in getAllUserIds():" + e.getMessage()));
		}
		return userIds;
	}
}
