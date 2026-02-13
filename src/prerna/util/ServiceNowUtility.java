package prerna.util;

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

import prerna.engine.api.IRDBMSEngine;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;

/**
 * Utility class for interacting with ServiceNow REST APIs.
 */
public class ServiceNowUtility {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	private static final Logger logger = LogManager.getLogger(ServiceNowUtility.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	public static Map<String, Object> createRecord(String instanceUrl, String accessToken, String tableName,
			Map<String, Object> fieldValues) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/" + tableName;
		logger.info("Creating records in table: {}", tableName);

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
					sysId = JsonParser.parseString(response)
				            .getAsJsonObject()
				            .getAsJsonObject("result")
				            .get("sys_id")
				            .getAsString();
				} catch (Exception e) {
					throw new IllegalStateException("sys_id not found in ServiceNow create response", e);
				}
				
		} catch (Exception e) {
				logger.error("Exception in createRecord: ", e);
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		Map<String, Object> result = new HashMap<>();
		result.put("success", allSuccess);
		result.put("sys_id", sysId);
		return result;
	}
	
	public static List<Map<String, Object>> getAllRecords(String instanceUrl, String accessToken, String tableName,
			String limit) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/" + tableName + "?sysparm_limit=" + limit;
		logger.info("Fetching all records from table: {} with limit: {}", tableName, limit);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json");
		try {
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> jsonMap = objectMapper.readValue(response, Map.class);
			return (List<Map<String, Object>>) jsonMap.get("result");
		} catch (Exception e) {
			logger.error("Exception in getAllRecords: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	public static Map<String, Object> getRecordBySysId(String instanceUrl, String accessToken, String tableName,
			String sysId) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/" + tableName + "/" + sysId;
		logger.info("Fetching record from table: {} with sys_id: {}", tableName, sysId);

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
			logger.error("Exception in getRecordBySysId: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
	
	public static Map<String, Object> updateRecord(String instanceUrl, String accessToken, String tableName,
			String sysId, Map<String, Object> fieldValues) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/" + tableName + "/" + sysId;
		logger.info("Updating (PATCH) record in table: {} with sys_id: {}", tableName, sysId);

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
			logger.error("Exception in updateRecord: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		result.put("success", success);
		return result;
	}
	
	public static Map<String, Object> deleteRecord(String instanceUrl, String accessToken, String tableName,
			String sysId) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/" + tableName + "/" + sysId;
		logger.info("Deleting record from table: {} with sys_id: {}", tableName, sysId);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json");

		Map<String, Object> result = new HashMap<>();
		boolean success = false;

		try {
			HttpHelperUtility.deleteRequestStringBody(endpoint, headers, null, null, null);
			success = true;
		} catch (Exception e) {
			logger.error("Exception in deleteRecord: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		result.put("success", success);
		return result;
	}
}
