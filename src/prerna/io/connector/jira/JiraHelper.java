package prerna.io.connector.jira;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IRDBMSEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class JiraHelper {

	private static final Logger classLogger = LogManager.getLogger(JiraHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).setPrettyPrinting().create();

	// Jira API URLs
	private static final String JIRA_LIST_ISSUE_URL = "/rest/api/3/search/jql";
	private static final String JIRA_CREATE_DELETE_ISSUE_URL = "/rest/api/2/issue";
	private static final String JIRA_GETALL_PROJECTS_URL = "/rest/api/2/project";

	// Database fields and identifiers
	private static final String TABLE = "JIRA_USER";
	private static final String JIRA_UNIQUE_ID = "KEY_NAME";
	private static final String API_KEY = "API_KEY";
	private static final String USER_ID = "USER_ID";
	private static final String URL = "URL";
	private static final String PROJECT = "PROJECT";

	// Jira response/request keys
	private static final String ID = "id";
	private static final String SELF = "self";
	private static final String SUMMARY = "summary";
	private static final String KEY = "key";
	private static final String SUCCESS = "success";

	// Authorization headers
	private static final String AUTHORIZATION = "Authorization";
	private static final String BASIC_PREFIX = "Basic ";

	public static IRDBMSEngine jiraDB;

	private JiraHelper() {
	}

	static {
		try {
			jiraDB = (IRDBMSEngine) Utility.getDatabase(Constants.SECURITY_DB);
		} catch (Exception e) {
			classLogger.error("Failed to initialize jiraDB", e);
			throw new SemossPixelException("Failed to initialize database connection for JiraHelper. Error message: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param db 
	 */
	public static void setJiraDB(IRDBMSEngine db) {
		jiraDB = db;
	}

	/**
	 * 
	 * @return 
	 */
	public static String getTableName() {
		try {
			List<String> tables = jiraDB.getPixelConcepts();
			for (String tbl : tables) {
				if (TABLE.equals(tbl)) {
					return tbl;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Failed to retrieve table name in getTableName(). Error message: " + e.getMessage());
		}
		return null;
	}

	/**
	 * 
	 * @param keyName 
	 * @return 
	 */
	private static boolean userExists(String keyName) {
		final String USER_EXISTS_QUERY_PREFIX = "SELECT 1 FROM ";
		final String USER_EXISTS_QUERY_SUFFIX = " WHERE " + JIRA_UNIQUE_ID + " = ?";

		String tableName = getTableName();
		if (tableName == null) {
			return false;
		}
		String checkQuery = USER_EXISTS_QUERY_PREFIX + tableName + USER_EXISTS_QUERY_SUFFIX;
		try (Connection conn = jiraDB.getConnection(); PreparedStatement pstmt = conn.prepareStatement(checkQuery)) {
			pstmt.setString(1, keyName);
			try (ResultSet rs = pstmt.executeQuery()) {
				return rs.next();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Unable to verify user presence. Error message: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param keyName 
	 * @param field   
	 * @return 
	 */
	private static String getFieldFromDB(String keyName, String field) {
		final String GET_FIELD_QUERY_PREFIX = "SELECT ";
		final String GET_FIELD_QUERY_MIDDLE = " FROM ";
		final String GET_FIELD_QUERY_SUFFIX = " WHERE " + JIRA_UNIQUE_ID + " = ?";

		String tableName = getTableName();
		if (tableName == null) {
			return null;
		}
		String query = GET_FIELD_QUERY_PREFIX + field + GET_FIELD_QUERY_MIDDLE + tableName + GET_FIELD_QUERY_SUFFIX;
		try (Connection conn = jiraDB.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, keyName);
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					return rs.getString(field);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Failed to retrieve field '" + field + "' for user '" + keyName
					+ "' in getFieldFromDB(). Error message: " + e.getMessage());
		}
		return null;
	}

	/**
	 * 
	 * @param keyName 
	 * @return 
	 */
	public static NounMetadata listIssue(String keyName) {
		final String ACCEPT = "Accept";
		final String APPLICATION_JSON = "application/json";
		final String JQL = "jql";
		final String FIELDS = "fields";
		final String ISSUES = "issues";
		final String PROJECT_QUERY_PREFIX = "project=";

		try {
			if (!userExists(keyName)) {
				throw new SemossPixelException("User '" + keyName + "' is not present in DB");
			}
			String apiKey = getFieldFromDB(keyName, API_KEY);
			String username = getFieldFromDB(keyName, USER_ID);
			String urlBase = getFieldFromDB(keyName, URL);
			String projectName = getFieldFromDB(keyName, PROJECT);

			if (apiKey == null || username == null || urlBase == null || projectName == null) {
				throw new SemossPixelException("User credentials, URL, or project missing in DB");
			}

			String normalizedUsername = username.trim();
			String normalizedApiKey = apiKey.trim();
			if (!urlBase.matches("^https?://.*")) {
				throw new SemossPixelException("Invalid JIRA URL format in DB");
			}
			if (!projectName.matches("^[A-Za-z0-9_-]+$")) {
				throw new SemossPixelException("Invalid project name format in DB");
			}

			String auth = normalizedUsername + ":" + normalizedApiKey;
			String encodeToString = Base64.getEncoder()
					.encodeToString(auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			Map<String, String> header = new HashMap<>();
			header.put(AUTHORIZATION, BASIC_PREFIX + encodeToString);
			header.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
			header.put(ACCEPT, APPLICATION_JSON);
			
			Map<String, Object> map = new HashMap<>();
			map.put(JQL, PROJECT_QUERY_PREFIX + projectName);
			List<String> fields = Arrays.asList(ID, SUMMARY, SELF);
			map.put(FIELDS, fields);
			String body = GSON.toJson(map);

			String requestUrl = urlBase + JIRA_LIST_ISSUE_URL;
			String response = HttpHelperUtility.postRequestStringBody(requestUrl, header, body, null, null, null, null);

			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode root = objectMapper.readTree(response);
			JsonNode issues = root.path(ISSUES);

			List<JiraTicketDetails> jiraIssueDetails = new ArrayList<>();
			for (JsonNode issue : issues) {
				String id = issue.path(ID).asText();
				String link = issue.path(SELF).asText();
				String summary = issue.path(FIELDS).path(SUMMARY).asText();
				jiraIssueDetails.add(new JiraTicketDetails(id, link, summary));
			}

			return new NounMetadata(jiraIssueDetails, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in listIssue for user '{}': {}", keyName, e.getMessage(), e);
			throw new SemossPixelException("Failed to list issues for project associated with '" + keyName + "'. Error message: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param summary    
	 * @param description 
	 * @param istype      
	 * @param keyName     
	 * @return 
	 */
	public static NounMetadata createIssue(String summary, String description, String istype, String keyName) {
		try {
			if (!userExists(keyName)) {
				throw new SemossPixelException("User id " + keyName + " is not present in DB");
			}
			String apiKey = getFieldFromDB(keyName, API_KEY);
			String username = getFieldFromDB(keyName, USER_ID);
			String urlBase = getFieldFromDB(keyName, URL);
			String projectName = getFieldFromDB(keyName, PROJECT);

			if (apiKey == null || username == null || urlBase == null || projectName == null) {
				throw new SemossPixelException("User credentials, URL, or project missing in DB");
			}

			String normalizedUsername = username.trim();
			String normalizedApiKey = apiKey.trim();
			if (!urlBase.matches("^https?://.*")) {
				throw new SemossPixelException("Invalid JIRA URL format in DB");
			}
			if (!projectName.matches("^[A-Za-z0-9_-]+$")) {
				throw new SemossPixelException("Invalid project name format in DB");
			}

			String auth = normalizedUsername + ":" + normalizedApiKey;
			String encodeToString = Base64.getEncoder()
					.encodeToString(auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			Map<String, String> map = new HashMap<>();
			map.put(AUTHORIZATION, BASIC_PREFIX + encodeToString);

			String requestUrl = urlBase + JIRA_CREATE_DELETE_ISSUE_URL;

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

			String body = GSON.toJson(jiraRequestBodyModel);

			String nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(requestUrl, map, body,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(nearestNeigborResponse, Map.class);
			Object id = responseMap.get(ID);
			Object link = responseMap.get(SELF);

			JiraTicketDetails jiraDetail = new JiraTicketDetails(id != null ? id.toString() : null,
					link != null ? link.toString() : null, null);

			return new NounMetadata(jiraDetail, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in createIssue for user '{}': {}", keyName, e.getMessage(), e);
			throw new SemossPixelException("Failed to create issue for project associated with '" + keyName + "'. Error message: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param jiraId  
	 * @param keyName 
	 * @return 
	 */
	public static NounMetadata deleteIssue(String jiraId, String keyName) {
		final String FIELDS = "fields";
		final String JSON_PROJECT = "project";

		try {
			ObjectMapper objectMapper = new ObjectMapper();

			if (!userExists(keyName)) {
				throw new SemossPixelException("User id " + keyName + " is not present in DB");
			}
			String apiKey = getFieldFromDB(keyName, API_KEY);
			String username = getFieldFromDB(keyName, USER_ID);
			String urlBase = getFieldFromDB(keyName, URL);
			String project = getFieldFromDB(keyName, PROJECT);

			if (apiKey == null || username == null || urlBase == null || project == null) {
				throw new SemossPixelException("User credentials, URL, or project missing in DB");
			}

			String normalizedUsername = username.trim();
			String normalizedApiKey = apiKey.trim();
			if (!urlBase.matches("^https?://.*")) {
				throw new SemossPixelException("Invalid JIRA URL format in DB");
			}
			if (!project.matches("^[A-Za-z0-9_-]+$")) {
				throw new SemossPixelException("Invalid project name format in DB");
			}

			String auth = normalizedUsername + ":" + normalizedApiKey;
			String encodeToString = Base64.getEncoder()
					.encodeToString(auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			Map<String, String> map = new HashMap<>();
			map.put(AUTHORIZATION, BASIC_PREFIX + encodeToString);

			String issueUrl = urlBase + JIRA_CREATE_DELETE_ISSUE_URL + "/" + jiraId;
			String issueResponse = HttpHelperUtility.getRequest(issueUrl, map, null, null, null);

			JsonNode root = objectMapper.readTree(issueResponse);
			String projectKey = root.path(FIELDS).path(JSON_PROJECT).path(KEY).asText();

			if (!project.equalsIgnoreCase(projectKey)) {
				throw new SemossPixelException("Issue " + jiraId + " does not belong to project " + project);
			}

			HttpHelperUtility.deleteRequestStringBody(issueUrl, map, null, null, null);

			Map<String, Object> response = new HashMap<>();
			response.put(SUCCESS, true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in deleteIssue for user '{}', issue '{}': {}", keyName, jiraId, e.getMessage(), e);
			String project = null;
			try {
				project = getFieldFromDB(keyName, PROJECT);
			} catch (Exception ex) {
				classLogger.error("Error retrieving project for user '{}': {}", keyName, ex.getMessage(), ex);
			}
			throw new SemossPixelException("Failed to delete issue for project '" + project + "' and user '" + keyName + "'. Error message: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param urlBase
	 * @param userId 
	 * @param apiKey  
	 * @return 
	 */
	public static NounMetadata getAllProjects(String urlBase, String userId, String apiKey) {
		try {
			if (userId == null || userId.isEmpty() || apiKey == null || apiKey.isEmpty() || urlBase == null
					|| urlBase.isEmpty()) {
				throw new SemossPixelException("User ID, API key, or URL is missing or empty");
			}
			String normalizedUserId = userId.trim();
			String normalizedApiKey = apiKey.trim();
			if (!urlBase.matches("^https?://.*")) {
				throw new SemossPixelException("Invalid JIRA URL format");
			}

			String auth = normalizedUserId + ":" + normalizedApiKey;
			String encodeToString = Base64.getEncoder()
					.encodeToString(auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));

			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BASIC_PREFIX + encodeToString);

			String requestUrl = urlBase + JIRA_GETALL_PROJECTS_URL;
			String response = HttpHelperUtility.getRequest(requestUrl, headers, null, null, null);

			JSONArray projResponse = new JSONArray(response);
			List<String> projList = new ArrayList<>();
			for (int i = 0; i < projResponse.length(); i++) {
				JSONObject project = projResponse.getJSONObject(i);
				projList.add(project.getString(KEY));
			}
			return new NounMetadata(projList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in getAllProjects: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve all projects. Error message: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @return 
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
	 * 
	 * @param keyName 
	 * @return
	 */
	public static NounMetadata deleteRecordForUser(String keyName) {
		try {
			String tableName = getTableName();
			if (tableName == null) {
				throw new SemossPixelException("Table not found in database.");
			}
			if (!userExists(keyName)) {
				throw new SemossPixelException("User id " + keyName + " is not present in DB");
			}
			String deleteQuery = "DELETE FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = ?";
			try (Connection conn = jiraDB.getConnection();
					PreparedStatement pstmt = conn.prepareStatement(deleteQuery)) {
				pstmt.setString(1, keyName);
				pstmt.executeUpdate();
			}
			Map<String, Object> response = new HashMap<>();
			response.put(SUCCESS, true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Error in deleteRecordForUser for user '{}': {}", keyName, e.getMessage(), e);
			throw new SemossPixelException("Failed to delete record for user '" + keyName + "'. Error message: " + e.getMessage());
		}
	}
}
