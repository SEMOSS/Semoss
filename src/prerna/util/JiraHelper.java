package prerna.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.model.Fields;
import prerna.engine.impl.model.IssueType;
import prerna.engine.impl.model.JiraRequestBodyModel;
import prerna.engine.impl.model.Project;
import prerna.io.connector.jira.reactor.JiraReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;

public class JiraHelper {

    public static final String JIRA_LIST_ISSUE_URL = "/rest/api/2/search?jql=project=";
    public static final String JIRA_DATABASE = "bcdb0a92-2a3b-4c73-bb79-5f5116bd6832";
    public static final String JIRA_UNIQUE_ID = "JIRAPROFILE_UNIQUE_ROW_ID";
    public static final String JIRA_CREATE_DELETE_ISSUE_URL = "/rest/api/2/issue";
    public static final String JIRA_GETALL_PROJECTS_URL = "/rest/api/2/project";

    private static final Logger classLogger = LogManager.getLogger(JiraHelper.class);

    public static NounMetadata listIssue(String projectName, String userId)
            throws JsonMappingException, JsonProcessingException {
        String msg = null;
        try {
            String apiKey = getDBDetails(userId);
            String username = getUserName(userId);
            String auth = username + ":" + apiKey;
            String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
            List<String> jiraIssueDetails = new ArrayList<String>();
            Map<String, String> map = new HashMap<String, String>();
            map.put("Authorization", "Basic " + encodeToString);
            String URL = getURLFromDB(userId);
            String url = URL + JIRA_LIST_ISSUE_URL + projectName;
            if (projectName == null || projectName.isEmpty()) {
                String error = "Project Name for listing issues is missing or null";
                return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            List<String> checkUserId = checkUserId(userId);
            if (!checkUserId.contains(userId)) {
                msg = userId + " is not present in DB";
                return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String response = HttpHelperUtility.getRequest(url, map, null, null, null);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(response);
            JsonNode issues = root.path("issues");
            for (JsonNode issue : issues) {
                String name = issue.path("id").asText();
                jiraIssueDetails.add(name);
            }
            return new NounMetadata(jiraIssueDetails, PixelDataType.CUSTOM_DATA_STRUCTURE,
                    PixelOperationType.OPERATION);

        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            msg = e.getMessage();
            return new NounMetadata("Error in listing all issues: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
                    PixelOperationType.OPERATION);
        }
    }

    private static String getURLFromDB(String userId) {
        String error = null;
        try {
            String URL = null;
            String tableName = null;
            IDatabaseEngine database = Utility.getDatabase(JIRA_DATABASE);
            List<String> tables = database.getPixelConcepts();
            for (String element : tables) {
                tableName = element;
            }
            String getURLQuery = "select URL from " + tableName + " WHERE " + JIRA_UNIQUE_ID + "='" + userId + "'";
            HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(getURLQuery);
            Object string = hashmap.get("RESULTSET_OBJECT");
            if (string instanceof ResultSet) {
                ResultSet rs = (ResultSet) string;
                while (rs.next()) {
                    URL = rs.getString("URL");
                }
            }
            return URL;
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        }
        return error;
    }

    private static String getUserName(String userId) {
        String error = null;
        try {
            String userName = null;
            String tableName = null;
            IDatabaseEngine database = Utility.getDatabase(JIRA_DATABASE);
            List<String> tables = database.getPixelConcepts();
            for (String element : tables) {
                tableName = element;
            }
            String selectQuery = "SELECT USER_ID FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + "='" + userId + "'";
            HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(selectQuery);
            Object string = hashmap.get("RESULTSET_OBJECT");
            if (string instanceof ResultSet) {
                ResultSet rs = (ResultSet) string;
                while (rs.next()) {
                    userName = rs.getString("USER_ID");
                }
            }
            return userName;
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        }
        return error;
    }

    private static String getDBDetails(String userId) throws Exception, SQLException {
        String tableName = null;
        String apiKey = null;
        IDatabaseEngine database = Utility.getDatabase(JIRA_DATABASE);
        List<String> tables = database.getPixelConcepts();
        for (String element : tables) {
            tableName = element;
        }
        String insertQuery = "SELECT API_KEY FROM " + tableName + " WHERE " + JIRA_UNIQUE_ID + "='" + userId + "'";
        HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(insertQuery);
        Object string = hashmap.get("RESULTSET_OBJECT");
        if (string instanceof ResultSet) {
            ResultSet rs = (ResultSet) string;
            while (rs.next()) {
                apiKey = rs.getString("API_KEY");
            }
        }
        return apiKey;
    }

    public static NounMetadata createIssue(String summary, String description, String istype, String projectName,
            String userId) {
        String msg = null;
        try {
            Map<String, Object> responseMap = null;
            String apiKey = getDBDetails(userId);
            String username = getUserName(userId);
            String auth = username + ":" + apiKey;
            String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
            Map<String, String> map = new HashMap<String, String>();
            map.put("Authorization", "Basic " + encodeToString);
            if ((summary == null && summary.isEmpty()) && (description == null && description.isEmpty())
                    && (istype == null && istype.isEmpty()) && (projectName == null && projectName.isEmpty())) {
                String error = "One or more of the information required for creating issue is missing or null";
                return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String nearestNeigborResponse = null;
            String URL = getURLFromDB(userId);
            String url = URL + JIRA_CREATE_DELETE_ISSUE_URL;
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
            List<String> checkUserId = checkUserId(userId);
            if (!checkUserId.contains(userId)) {
                msg = "User id " + userId + " is not present in DB";
                return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, map, body,
                    ContentType.APPLICATION_JSON, null, null, null);
            responseMap = gson.fromJson(nearestNeigborResponse, Map.class);
            Object jiraId = responseMap.get("id");
            Object link = responseMap.get("self");

            return new NounMetadata("Jira id: " + jiraId + "," + " Jira link: " + link,
                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            msg = e.getMessage();
            return new NounMetadata("Error in creating new issue: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
                    PixelOperationType.OPERATION);
        }
    }

    public static NounMetadata deleteIssue(String jiraId, String userId) {
        String msg = null;
        try {
            String apiKey = getDBDetails(userId);
            String username = getUserName(userId);
            String auth = username + ":" + apiKey;
            String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
            Map<String, String> map = new HashMap<String, String>();
            map.put("Authorization", "Basic " + encodeToString);
            if (jiraId == null && jiraId.isEmpty()) {
                String error = "Jira id can not be empty or null";
                return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String URL = getURLFromDB(userId);
            String url = URL + JIRA_CREATE_DELETE_ISSUE_URL + jiraId;
            List<String> checkUserId = checkUserId(userId);
            if (!checkUserId.contains(userId)) {
                msg = "User id " + userId + " is not present in DB";
                return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            HttpHelperUtility.deleteRequestStringBody(url, map, null, null, null);
            return new NounMetadata("Jira id " + jiraId + " succesfully deleted", PixelDataType.CUSTOM_DATA_STRUCTURE,
                    PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            msg = e.getMessage();
            return new NounMetadata("Error in deleting issue: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
                    PixelOperationType.OPERATION);
        }
    }

    public static NounMetadata truncateData(String userId) {
        Boolean truncateFlag = false;
        String msg = null;
        String error = null;
        try {
            String tableName = null;
            long Uid;
            IDatabaseEngine database = Utility.getDatabase(JIRA_DATABASE);
            List<String> tables = database.getPixelConcepts();
            for (String element : tables) {
                tableName = element;
            }
            List<String> checkUserId = checkUserId(userId);
            if (!checkUserId.contains(userId)) {
                msg = "User id " + userId + " is not present in DB";
                return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String truncateQuery = "Delete from JIRAPROFILE";
            database.removeData(truncateQuery);
            truncateFlag = true;
            Uid = Long.valueOf(userId);
            if (truncateFlag == true) {
                msg = "Table truncated succesfully by user with user id: " + Uid;
            }
            return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            error = e.getMessage();
        }
        return new NounMetadata("Data not truncated with error message: " + error, PixelDataType.CUSTOM_DATA_STRUCTURE,
                PixelOperationType.OPERATION);
    }

    private static List<String> checkUserId(String userId) {
        List<String> userIds = new ArrayList<String>();
        try {
            String tableName = null;
            String userID;
            IDatabaseEngine database = Utility.getDatabase(JIRA_DATABASE);
            List<String> tables = database.getPixelConcepts();
            for (String element : tables) {
                tableName = element;
            }
            String query = " SELECT " + JIRA_UNIQUE_ID + " from " + tableName;
            HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
            Object string = hashmap.get("RESULTSET_OBJECT");
            if (string instanceof ResultSet) {
                ResultSet rs = (ResultSet) string;
                while (rs.next()) {
                    userID = rs.getString(JIRA_UNIQUE_ID);
                    userIds.add(userID);
                }
            }
            return userIds;
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            return userIds;
        }
    }

    public static NounMetadata getAllProjects(String userId) {
        String msg = null;
        try {
            String apiKey = getDBDetails(userId);
            String username = getUserName(userId);
            String auth = username + ":" + apiKey;
            String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
            List<String> jiraIssueDetails = new ArrayList<String>();
            Map<String, String> map = new HashMap<String, String>();
            map.put("Authorization", "Basic " + encodeToString);
            String URL = getURLFromDB(userId);
            String url = URL + JIRA_GETALL_PROJECTS_URL;
            List<String> projList = new ArrayList<String>();
            List<String> checkUserId = checkUserId(userId);
            if (!checkUserId.contains(userId)) {
                msg = "User id " + userId + " is not present in DB";
                return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String response = HttpHelperUtility.getRequest(url, map, null, null, null);
            JSONArray projResponse = new JSONArray(response);
            for (int i = 0; i < projResponse.length(); i++) {
                JSONObject project = projResponse.getJSONObject(i);
                String key = project.getString("key");
                projList.add(key);
            }
            return new NounMetadata(projList, PixelDataType.CUSTOM_DATA_STRUCTURE,
                    PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            msg = e.getMessage();
            return new NounMetadata("Error in getting project list: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
                    PixelOperationType.OPERATION);
        }
    }

    public static NounMetadata deleteRecordForUser(String userId) {
        Boolean truncateFlag = false;
        String msg = null;
        String error = null;
        try {
            String tableName = null;
            long Uid;
            IDatabaseEngine database = Utility.getDatabase(JIRA_DATABASE);
            List<String> tables = database.getPixelConcepts();
            for (String element : tables) {
                tableName = element;
            }
            List<String> checkUserId = checkUserId(userId);
            if (!checkUserId.contains(userId)) {
                msg = "User id " + userId + " is not present in DB";
                return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            }
            String truncateQuery = "Delete from " + tableName + " WHERE " + JIRA_UNIQUE_ID + "='" + userId + "'";
            database.removeData(truncateQuery);
            truncateFlag = true;
            Uid = Long.valueOf(userId);
            if (truncateFlag == true) {
                msg = "Record deleted succesfully by user " + userId + " for user id " + userId;
            }
            return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            error = e.getMessage();
        }
        return new NounMetadata("Data not truncated with error message: " + error, PixelDataType.CUSTOM_DATA_STRUCTURE,
                PixelOperationType.OPERATION);
    }

    public static NounMetadata issueType(String userId) {
        ArrayList<String> jiraIssues = new ArrayList<>();
        jiraIssues.add("Epic");
        jiraIssues.add("Story");
        jiraIssues.add("Task");
        jiraIssues.add("Bug");
        jiraIssues.add("Subtask");
        return new NounMetadata(jiraIssues, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
    }
}
