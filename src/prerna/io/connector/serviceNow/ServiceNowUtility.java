package prerna.io.connector.serviceNow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;

public class ServiceNowUtility {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	private static final String API_ENDPOINT_SUFFIX = "/api/now/table/";
	private static final String SLASH = "/";
	private static final String SYSPARM_LIMIT = "?sysparm_limit=";
	private static final String TABLE_LIST_ENDPOINT_SUFFIX = "/api/now/doc/table/schema";
	private static final String TABLE_METADATA_ENDPOINT_SUFFIX = "/api/now/ui/meta/";
	private static final String RECORD_URL_ENDPOINT = ".do?sys_id=";
	
	private static final Logger classLogger = LogManager.getLogger(ServiceNowUtility.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	public static Map<String, Object> createRecord(String instanceUrl, String accessToken, String tableName,
			Map<String, Object> fieldValues) throws Exception {
		
		String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName;
		classLogger.info("Creating records in table: {}", tableName);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json",
				"Content-Type", "application/json");

		boolean allSuccess = true;
		String sysId = null;

		try {
			    // converting the field values map to JSON for POST body
			    String jsonBody = gson.toJson(fieldValues);
				String response = HttpHelperUtility.postRequestStringBody(endpoint, headers, jsonBody,
						ContentType.APPLICATION_JSON, null, null, null);
				try {
					// fetch sys id from response
					sysId = JsonParser.parseString(response)
				            .getAsJsonObject()
				            .getAsJsonObject("result")
				            .get("sys_id")
				            .getAsString();
				} catch (Exception e) {
					throw new IllegalStateException("sys_id not found in ServiceNow create response", e);
				}
				
		} catch (Exception e) {
				classLogger.error("Exception in createRecord: ", e);
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		String record_url = instanceUrl + SLASH + tableName + RECORD_URL_ENDPOINT + sysId;
		Map<String, Object> result = new HashMap<>();
		result.put("success", allSuccess);
		result.put("record_url", record_url);
		return result;
	}
	
	public static List<Map<String, Object>> getAllRecords(String instanceUrl, String accessToken, String tableName,
			String limit) throws Exception {
		
		String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SYSPARM_LIMIT + limit;
		classLogger.info("Fetching all records from table: {} with limit: {}", tableName, limit);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json");
		try {
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> jsonMap = objectMapper.readValue(response, Map.class);
			return (List<Map<String, Object>>) jsonMap.get("result");
			
		} catch (Exception e) {
			classLogger.error("Exception in getAllRecords: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	public static Map<String, Object> getRecordBySysId(String instanceUrl, String accessToken, String tableName,
			String sysId) throws Exception {
		String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SLASH + sysId;
		classLogger.info("Fetching record from table: {} with sys_id: {}", tableName, sysId);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json");
		try {
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> jsonMap = objectMapper.readValue(response, Map.class);
			Object resultObj = jsonMap.get("result");
			
			if (resultObj instanceof Map) {
				return (Map<String, Object>) resultObj;
				
			} else if (resultObj instanceof List) {
				List<Map<String, Object>> resultList = (List<Map<String, Object>>) resultObj;
				
				if (!resultList.isEmpty()) {
					return resultList.get(0);
				}
			}
			throw new Exception("No record found in ServiceNow response.");
		} catch (Exception e) {
			classLogger.error("Exception in getRecordBySysId: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	public static Map<String, Object> updateRecord(String instanceUrl, String accessToken, String tableName,
			String sysId, Map<String, Object> fieldValues) throws Exception {
		
		String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SLASH + sysId;
		classLogger.info("Updating (PATCH) record in table: {} with sys_id: {}", tableName, sysId);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json",
				"Content-Type", "application/json");

		Map<String, Object> result = new HashMap<>();
		StringBuilder errorMessages = new StringBuilder();
		boolean success = false;

		try {
			// converting the field values map to JSON for POST body
		    String jsonBody = gson.toJson(fieldValues);
			HttpHelperUtility.patchRequestStringBody(endpoint, headers, jsonBody, ContentType.APPLICATION_JSON, null,
					null, null);
			success = true;
			
		} catch (Exception e) {
			classLogger.error("Exception in updateRecord: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		result.put("success", success);
		return result;
	}
	
	public static Map<String, Object> deleteRecord(String instanceUrl, String accessToken, String tableName,
			String sysId) throws Exception {
		
		String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SLASH + sysId;
		classLogger.info("Deleting record from table: {} with sys_id: {}", tableName, sysId);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json");

		Map<String, Object> result = new HashMap<>();
		boolean success = false;

		try {
			HttpHelperUtility.deleteRequestStringBody(endpoint, headers, null, null, null);
			success = true;
			
		} catch (Exception e) {
			classLogger.error("Exception in deleteRecord: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		result.put("success", success);
		return result;
	}
	
	public static Map<String, Object> getAllTables(String instanceUrl, String accessToken) throws Exception {
		
		String endpoint = instanceUrl + TABLE_LIST_ENDPOINT_SUFFIX;
		classLogger.info("Fetching all tables from ServiceNow instance: " + instanceUrl);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json");
		try {
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> jsonMap = objectMapper.readValue(response, Map.class);
			
			List<Map<String, Object>> allTables =(List<Map<String, Object>>) jsonMap.get("result");
			
			// FILTER ONLY USER-CREATED TABLES (u_)
			List<Map<String, Object>> userTables = allTables.stream()
	                .filter(t -> {
	                    Object value = t.get("value");
	                    return value instanceof String && ((String) value).startsWith("u_");
	                })
	                .toList();
			Map<String, Object> responseMap = new HashMap<>();
			responseMap.put("tables", userTables);
			return responseMap;
			
		} catch (Exception e) {
			classLogger.error("Exception in getAllTables: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	public static Map<String, Object> getTableMetadata(String instanceUrl, String accessToken, String tableName) throws Exception {
		
		String endpoint = instanceUrl + TABLE_METADATA_ENDPOINT_SUFFIX + tableName;
		classLogger.info("Fetching table metadata for {} table", tableName);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json");
		try {
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> jsonMap = objectMapper.readValue(response, Map.class);
			return (Map<String, Object>) jsonMap.get("result");
			
		} catch (Exception e) {
			classLogger.error("Exception in fetching table metadata: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	public static String getServiceNowAccessToken(User user) throws Exception {
		String accessToken = null;
		
		if (user == null) {
        	classLogger.error("User not found in session. Please login to ServiceNow.");
            throw new SemossPixelException("User not found in session. Please login to ServiceNow.");
        }

		AccessToken resourceToken = user.getResourceAccessToken(AuthProvider.SERVICENOW);
		if (resourceToken != null) {
			accessToken = resourceToken.getAccess_token();
		} else {
			AccessToken servicenowToken = user.getAccessToken(AuthProvider.SERVICENOW);
			if (servicenowToken == null) {
				classLogger.error("No ServiceNow Access Token fetched for user");
	            throw new SemossPixelException("No ServiceNow Access Token fetched.");
			} 
			accessToken = servicenowToken.getAccess_token();
		}
        return accessToken;
	}
}
