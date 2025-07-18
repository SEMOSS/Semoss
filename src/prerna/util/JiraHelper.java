package prerna.util;

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

import prerna.engine.api.IDatabaseEngine;
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

    private static String getTableName(IDatabaseEngine database) {
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

    private static boolean userExists(IDatabaseEngine database, String keyName) {
        String tableName = getTableName(database);
        if (tableName == null) return false;
        String checkQuery = "SELECT 1 FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = '" + keyName + "'";
        try {
            HashMap<String, String> checkResult = (HashMap<String, String>) database.execQuery(checkQuery);
            Object rsObj = checkResult.get("RESULTSET_OBJECT");
            if (rsObj instanceof ResultSet) {
                ResultSet rs = (ResultSet) rsObj;
                boolean exists = rs.next();
                rs.close();
                return exists;
            }
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        }
        return false;
    }

    // Get a field value for a user from DB
    private static String getFieldFromDB(String keyName, String field) {
        try {
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            String tableName = getTableName(database);
            if (tableName == null) return null;
            String query = "SELECT " + field + " FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + "='" + keyName + "'";
            HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
            Object rsObj = hashmap.get("RESULTSET_OBJECT");
            if (rsObj instanceof ResultSet) {
                ResultSet rs = (ResultSet) rsObj;
                if (rs.next()) {
                    String value = rs.getString(field);
                    rs.close();
                    return value;
                }
                rs.close();
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
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            if (!userExists(database, keyName)) {
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
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            if (!userExists(database, keyName)) {
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
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            if (!userExists(database, keyName)) {
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
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            String tableName = getTableName(database);
            if (tableName == null) {
                return new NounMetadata("Table not found in database.",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            if (!userExists(database, keyName)) {
                return new NounMetadata("User id " + keyName + " is not present in DB. Truncate operation aborted.",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String truncateQuery = "DELETE FROM " + tableName;
            database.removeData(truncateQuery);
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
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            if (!userExists(database, keyName)) {
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
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            String tableName = getTableName(database);
            if (tableName == null) {
                return new NounMetadata("Table not found in database.",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            if (!userExists(database, keyName)) {
                return new NounMetadata("User id " + keyName + " is not present in DB",
                        PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String deleteQuery = "DELETE FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + " = '" + keyName + "'";
            database.removeData(deleteQuery);
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

    // Utility: Get all user IDs from DB
    private static List<String> getAllUserIds() {
        List<String> userIds = new ArrayList<>();
        try {
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            String tableName = getTableName(database);
            if (tableName == null) return userIds;
            String query = "SELECT " + JIRA_UNIQUE_ID + " FROM " + tableName;
            HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
            Object rsObj = hashmap.get("RESULTSET_OBJECT");
            if (rsObj instanceof ResultSet) {
                ResultSet rs = (ResultSet) rsObj;
                while (rs.next()) {
                    userIds.add(rs.getString(JIRA_UNIQUE_ID));
                }
                rs.close();
            }
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        }
        return userIds;
    }
}
