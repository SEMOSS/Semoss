package prerna.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.entity.ContentType;

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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;

public class JiraHelper {

	public static NounMetadata listIssue(String projectName, String userName) throws JsonMappingException, JsonProcessingException {
		try {
			String apiKey = getDBDetails(userName);
			String auth=userName+":"+apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
			List<String> jiraIssueDetails = new ArrayList<String>();
			Map<String, String> map = new HashMap<String, String>();
			map.put("Authorization", "Basic "+encodeToString);
			String url = JiraConstant.JiraUrl+"/rest/api/2/search?jql=project=" + projectName;
			if (projectName == null || projectName.isEmpty()) {
				String error = "Project Name for listing issues is missing or null";
				return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
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
			String erroMessage = e.getMessage();
			return new NounMetadata("Error in listing all issues: " + erroMessage, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	private static String getDBDetails(String userName) throws Exception, SQLException {
		String tableName=null;
		String apiKey=null;
		IDatabaseEngine database = Utility.getDatabase("c44b138d-aa8e-42cc-a925-6c2ac855df64");
		List<String> pixelConcepts = database.getPixelConcepts();
		for (String element : pixelConcepts) {
			tableName = element;
		}
		String insertQuery = "SELECT Apikey FROM " + tableName + " WHERE UserId='" + userName + "'"+" ORDER BY DATE_CREATED DESC LIMIT 1";
		HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(insertQuery);
		Object string = hashmap.get("RESULTSET_OBJECT");
		if(string instanceof ResultSet) {
			ResultSet rs=(ResultSet)string;
			while(rs.next()) {
				apiKey = rs.getString("Apikey");
			}
		}
		return apiKey;
	}

	public static NounMetadata createIssue(String summary, String description, String istype, String projectName, String userName) {
		try {
			String apiKey = getDBDetails(userName);
			String auth=userName+":"+apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
			Map<String, String> map = new HashMap<String, String>();
			map.put("Authorization", "Basic "+encodeToString);
			if ((summary == null && summary.isEmpty()) && (description == null && description.isEmpty())
					&& (istype == null && istype.isEmpty()) && (projectName == null && projectName.isEmpty())) {
				String error = "One or more of the information required for creating issue is missing or null";
				return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String nearestNeigborResponse = null;
			String url = JiraConstant.JiraUrl+"/rest/api/2/issue";
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
			nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, map, body,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, Map.class);
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			String errorMessage = e.getMessage();
			return new NounMetadata("Error in creating new issue: " + errorMessage, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	public static NounMetadata deleteIssue(String jiraId, String userName) {
		try {
			String apiKey = getDBDetails(userName);
			String auth=userName+":"+apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
			Map<String, String> map = new HashMap<String, String>();
			map.put("Authorization", "Basic "+encodeToString);
			if (jiraId == null && jiraId.isEmpty()) {
				String error = "Jira id can not be empty or null";
				return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String url = JiraConstant.JiraUrl+"/rest/api/2/issue/" + jiraId;
			HttpHelperUtility.deleteRequestStringBody(url, map, null, null, null);
			return new NounMetadata(jiraId + " succesfully deleted", PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			String errorMessage = e.getMessage();
			return new NounMetadata("Error in deleting issue: " + errorMessage, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}

	}

}
