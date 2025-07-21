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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
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
        }
        return null;
    }

    private static boolean userExists(String keyName) {
        String tableName = getTableName();
        if (tableName == null) return false;
        String checkQuery = "SELECT 1 FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = ?";
        try (Connection conn = jiraDB.makeConnection();
             PreparedStatement pstmt = conn.prepareStatement(checkQuery)) {
            pstmt.setString(1, keyName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        }
        return false;
    }

    private static String getFieldFromDB(String keyName, String field) {
        String tableName = getTableName();
        if (tableName == null) return null;
        String query = "SELECT " + field + " FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = ?";
        try (Connection conn = jiraDB.makeConnection();
             PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, keyName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(field);
                }
            }
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        }
        return null;
    }

    public static NounMetadata listIssue(String projectName, String keyName) {
        if (projectName == null || projectName.isEmpty()) {
            return new NounMetadata("Project Name for listing issues is missing or null",
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        }
        try {
            if (!userExists(keyName)) {
                return new NounMetadata(keyName + " is not present in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String apiKey = getFieldFromDB(keyName, "API_KEY");
            String username = getFieldFromDB(keyName, "USER_ID");
            String urlBase = getFieldFromDB(keyName, "URL");
            if (apiKey == null || username == null || urlBase == null) {
                return new NounMetadata("User credentials or URL missing in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
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
            List<String> jiraIssueDetails = new ArrayList<>();
            for (JsonNode issue : issues) {
                jiraIssueDetails.add(issue.path("id").asText());
            }
            return new NounMetadata(jiraIssueDetails, PixelDataType.CUSTOM_DATA_STRUCTURE,
                    PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata("Error in listing all issues: " + e.getMessage(),
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        }
    }

    public static NounMetadata createIssue(String summary, String description, String istype, String projectName, String keyName) {
        if (summary == null || summary.isEmpty() ||
            description == null || description.isEmpty() ||
            istype == null || istype.isEmpty() ||
            projectName == null || projectName.isEmpty()) {
            return new NounMetadata("One or more of the information required for creating issue is missing or null",
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        }
        try {
            if (!userExists(keyName)) {
                return new NounMetadata("User id " + keyName + " is not present in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String apiKey = getFieldFromDB(keyName, "API_KEY");
            String username = getFieldFromDB(keyName, "USER_ID");
            String urlBase = getFieldFromDB(keyName, "URL");
            if (apiKey == null || username == null || urlBase == null) {
                return new NounMetadata("User credentials or URL missing in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String auth = username + ":" + apiKey;
            String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
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
            Object jiraId = responseMap.get("id");
            Object link = responseMap.get("self");

            return new NounMetadata("Jira id: " + jiraId + ", Jira link: " + link,
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata("Error in creating new issue: " + e.getMessage(),
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        }
    }

    public static NounMetadata deleteIssue(String jiraId, String keyName, String project) {
        try {
            if (!userExists(keyName)) {
                return new NounMetadata("User id " + keyName + " is not present in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String apiKey = getFieldFromDB(keyName, "API_KEY");
            String username = getFieldFromDB(keyName, "USER_ID");
            String urlBase = getFieldFromDB(keyName, "URL");
            if (apiKey == null || username == null || urlBase == null) {
                return new NounMetadata("User credentials or URL missing in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String auth = username + ":" + apiKey;
            String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
            Map<String, String> map = new HashMap<>();
            map.put("Authorization", "Basic " + encodeToString);

            // Step 1: Get issue details to verify project
            String issueUrl = urlBase + JIRA_CREATE_DELETE_ISSUE_URL + "/" + jiraId;
            String issueResponse = HttpHelperUtility.getRequest(issueUrl, map, null, null, null);

            // Parse the response to check project key
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(issueResponse);
            String projectKey = root.path("fields").path("project").path("key").asText();

            if (!project.equalsIgnoreCase(projectKey)) {
                return new NounMetadata("Issue " + jiraId + " does not belong to project " + project,
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }

            // Step 2: Delete the issue
            HttpHelperUtility.deleteRequestStringBody(issueUrl, map, null, null, null);
            return new NounMetadata("Jira id " + jiraId + " successfully deleted from project " + project,
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata("Error in deleting issue: " + e.getMessage(),
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        }
    }

    public static NounMetadata truncateData(String keyName) {
        try {
            String tableName = getTableName();
            if (tableName == null) {
                return new NounMetadata("Table not found in database.",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            if (!userExists(keyName)) {
                return new NounMetadata("User id " + keyName + " is not present in DB. Truncate operation aborted.",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String truncateQuery = "DELETE FROM " + tableName;
            try (Connection conn = jiraDB.makeConnection();
                 PreparedStatement pstmt = conn.prepareStatement(truncateQuery)) {
                pstmt.executeUpdate();
            }
            String msg = "Table '" + tableName + "' truncated successfully by user with user id: " + keyName;
            return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata("Data not truncated. Error: " + e.getMessage(),
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        }
    }

    public static NounMetadata getAllProjects(String keyName) {
        try {
            if (!userExists(keyName)) {
                return new NounMetadata("User id " + keyName + " is not present in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String apiKey = getFieldFromDB(keyName, "API_KEY");
            String username = getFieldFromDB(keyName, "USER_ID");
            String urlBase = getFieldFromDB(keyName, "URL");
            if (apiKey == null || username == null || urlBase == null) {
                return new NounMetadata("User credentials or URL missing in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String auth = username + ":" + apiKey;
            String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
            Map<String, String> map = new HashMap<>();
            map.put("Authorization", "Basic " + encodeToString);
            String url = urlBase + JIRA_GETALL_PROJECTS_URL;
            String response = HttpHelperUtility.getRequest(url, map, null, null, null);
            JSONArray projResponse = new JSONArray(response);
            List<String> projList = new ArrayList<>();
            for (int i = 0; i < projResponse.length(); i++) {
                JSONObject project = projResponse.getJSONObject(i);
                projList.add(project.getString("key"));
            }
            return new NounMetadata(projList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata("Error in getting project list: " + e.getMessage(),
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        }
    }

    public static NounMetadata deleteRecordForUser(String keyName) {
        try {
            String tableName = getTableName();
            if (tableName == null) {
                return new NounMetadata("Table not found in database.",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            if (!userExists(keyName)) {
                return new NounMetadata("User id " + keyName + " is not present in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String deleteQuery = "DELETE FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = ?";
            try (Connection conn = jiraDB.makeConnection();
                 PreparedStatement pstmt = conn.prepareStatement(deleteQuery)) {
                pstmt.setString(1, keyName);
                pstmt.executeUpdate();
            }
            String msg = "Record deleted successfully for user id " + keyName;
            return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            return new NounMetadata("Data not deleted. Error: " + e.getMessage(),
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
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
            if (tableName == null) return userIds;
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
        }
        return userIds;
    }
}
