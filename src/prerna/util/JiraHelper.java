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

	public static NounMetadata listIssue(String projectName, String userId) throws JsonMappingException, JsonProcessingException {
		String msg=null;
		try {
			String apiKey = getDBDetails(userId);
			String username=getUserName(userId);
			String auth=username+":"+apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
			List<String> jiraIssueDetails = new ArrayList<String>();
			Map<String, String> map = new HashMap<String, String>();
			map.put("Authorization", "Basic "+encodeToString);
			String URL=getURLFromDB(userId);
			String url = URL+"/rest/api/2/search?jql=project=" + projectName;
			if (projectName == null || projectName.isEmpty()) {
				String error = "Project Name for listing issues is missing or null";
				return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			List<String> checkUserId = checkUserId(userId);
				if(!checkUserId.contains(userId)) {
					msg=userId+" is not present in DB";
					return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
							PixelOperationType.OPERATION);
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
			msg = e.getMessage();
			return new NounMetadata("Error in listing all issues: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	private static String getURLFromDB(String userId) {
		String error=null;
		try {
			String URL = null;
			String tableName = null;
			IDatabaseEngine database = Utility.getDatabase("3c6f0856-25f0-4bf2-83ae-c4b6253e8b01");
			List<String> pixelConcepts = database.getPixelConcepts();
			for (String element : pixelConcepts) {
				tableName = element;
			}
			String getURLQuery = "select URL from " + tableName + " WHERE JIRAPROFILE_UNIQUE_ROW_ID='" + userId + "'";
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
			error=e.getMessage();
		}
		return error;
	}

	private static String getUserName(String userId) {
		String error=null;
		try {
			String userName = null;
			String tableName = null;
			IDatabaseEngine database = Utility.getDatabase("3c6f0856-25f0-4bf2-83ae-c4b6253e8b01");
			List<String> pixelConcepts = database.getPixelConcepts();
			for (String element : pixelConcepts) {
				tableName = element;
			}
			String selectQuery = "SELECT USER_ID FROM " + tableName + " WHERE JIRAPROFILE_UNIQUE_ROW_ID='" + userId
					+ "'";
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
			error=e.getMessage();
			
		}
		return error;
	}

	private static String getDBDetails(String userId) throws Exception, SQLException {
		String tableName=null;
		String apiKey=null;
		IDatabaseEngine database = Utility.getDatabase("3c6f0856-25f0-4bf2-83ae-c4b6253e8b01");
		List<String> pixelConcepts = database.getPixelConcepts();
		for (String element : pixelConcepts) {
			tableName = element;
		}
		String insertQuery = "SELECT API_KEY FROM " + tableName + " WHERE JIRAPROFILE_UNIQUE_ROW_ID='" + userId + "'";
		HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(insertQuery);
		Object string = hashmap.get("RESULTSET_OBJECT");
		if(string instanceof ResultSet) {
			ResultSet rs=(ResultSet)string;
			while(rs.next()) {
				apiKey = rs.getString("API_KEY");
			}
		}
		return apiKey;
	}

	public static NounMetadata createIssue(String summary, String description, String istype, String projectName, String userId) {
		String msg=null;
		try {
			Map<String, Object> responseMap =null;
			String apiKey = getDBDetails(userId);
			String username=getUserName(userId);
			String auth=username+":"+apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
			Map<String, String> map = new HashMap<String, String>();
			map.put("Authorization", "Basic "+encodeToString);
			if ((summary == null && summary.isEmpty()) && (description == null && description.isEmpty())
					&& (istype == null && istype.isEmpty()) && (projectName == null && projectName.isEmpty())) {
				String error = "One or more of the information required for creating issue is missing or null";
				return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String nearestNeigborResponse = null;
			String URL=getURLFromDB(userId);
			String url = URL+"/rest/api/2/issue";
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
				if(!checkUserId.contains(userId)) {
					msg="User id "+userId+ " is not present in DB";
					return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
				}
					nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, map, body,
							ContentType.APPLICATION_JSON, null, null, null);
					responseMap = gson.fromJson(nearestNeigborResponse, Map.class);
					Object jiraId = responseMap.get("id");
					Object link = responseMap.get("self");
				
			
			return new NounMetadata("Jira id: "+jiraId+","+" Jira link: "+link, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			msg= e.getMessage();
			return new NounMetadata("Error in creating new issue: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	public static NounMetadata deleteIssue(String jiraId, String userId) {
		String msg=null;
		try {
			String apiKey = getDBDetails(userId);
			String username=getUserName(userId);
			String auth=username+":"+apiKey;
			String encodeToString = Base64.getEncoder().encodeToString(auth.getBytes());
			Map<String, String> map = new HashMap<String, String>();
			map.put("Authorization", "Basic "+encodeToString);
			if (jiraId == null && jiraId.isEmpty()) {
				String error = "Jira id can not be empty or null";
				return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String URL=getURLFromDB(userId);
			String url = URL+"/rest/api/2/issue/" + jiraId;
			List<String> checkUserId = checkUserId(userId);
				if(!checkUserId.contains(userId)) {
					msg="User id "+userId+ " is not present in DB";
					return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
							PixelOperationType.OPERATION);
				}
					HttpHelperUtility.deleteRequestStringBody(url, map, null, null, null);
			return new NounMetadata("Jira id "+jiraId + " succesfully deleted", PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			msg = e.getMessage();
			return new NounMetadata("Error in deleting issue: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}

	}

	public static NounMetadata truncateData(String userId) {
		Boolean truncateFlag=false;
		String msg=null;
		String error=null;
		try {
			String tableName = null;
			long Uid;
			IDatabaseEngine database = Utility.getDatabase("3c6f0856-25f0-4bf2-83ae-c4b6253e8b01");
			List<String> pixelConcepts = database.getPixelConcepts();
			for (String element : pixelConcepts) {
				tableName = element;
			}
			List<String> checkUserId = checkUserId(userId);
				if(!checkUserId.contains(userId)) {
					msg="User id "+userId+ " is not present in DB";
					return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
							PixelOperationType.OPERATION);
				}
					String truncateQuery = "Delete from JIRAPROFILE";
					database.removeData(truncateQuery);
					truncateFlag=true;
					Uid=Long.valueOf(userId);
					if(truncateFlag==true) {
						msg="Table truncated succesfully by user with user id: "+Uid;
						
					}
			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			error=e.getMessage();
		}
		return new NounMetadata("Data not truncated with error message: "+error, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}

	private static List<String> checkUserId(String userId) {
		List<String> userIds = new ArrayList<String>();
		try {
			String tableName = null;
			String userID;
			IDatabaseEngine database = Utility.getDatabase("3c6f0856-25f0-4bf2-83ae-c4b6253e8b01");
			List<String> pixelConcepts = database.getPixelConcepts();
			for (String element : pixelConcepts) {
				tableName = element;
			}
			String query = " SELECT JIRAPROFILE_UNIQUE_ROW_ID from " + tableName;
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query); 
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					userID = rs.getString("JIRAPROFILE_UNIQUE_ROW_ID");
					userIds.add(userID);
				}
			}
			return userIds;
		} catch (Exception e) {
			e.printStackTrace();
			return userIds;
		}
	}

	

}
