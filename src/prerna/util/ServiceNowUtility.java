package prerna.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.api.IRDBMSEngine;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;

/**
 * Utility class for interacting with ServiceNow REST APIs.
 */
public class ServiceNowUtility {

	private static final Logger logger = LogManager.getLogger(ServiceNowUtility.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Fetches all ServiceNow tables with their names and labels.
	 *
	 * @param instanceUrl ServiceNow instance URL.
	 * @param accessToken OAuth2 access token.
	 * @return JSON response as String.
	 * @throws Exception if the request fails.
	 */
	public static String fetchTables(String instanceUrl, String accessToken) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/sys_db_object?sysparm_fields=name,label&sysparm_limit=10000";
		logger.info("Fetching ServiceNow tables from endpoint: {}", endpoint);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json");
		try {
			return HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
		} catch (Exception e) {
			logger.error("Exception in fetchTables: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Creates records in a ServiceNow table.
	 *
	 * @param instanceUrl ServiceNow instance URL.
	 * @param accessToken OAuth2 access token.
	 * @param tableName   Table name.
	 * @param fieldValues List of field maps for each record.
	 * @return Map with {"success": true/false, "responses": [...], "error": "..."}.
	 * @throws Exception if any request fails.
	 */
	public static Map<String, Object> createRecord(String instanceUrl, String accessToken, String tableName,
			List<Map<String, String>> fieldValues) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/" + tableName;
		logger.info("Creating records in table: {}", tableName);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json",
				"Content-Type", "application/json");

		boolean allSuccess = true;
		StringBuilder errorMessages = new StringBuilder();

		for (Map<String, String> fields : fieldValues) {
			try {
				String jsonInputString = objectMapper.writeValueAsString(fields);
				HttpHelperUtility.postRequestStringBody(endpoint, headers, jsonInputString,
						ContentType.APPLICATION_JSON, null, null, null);
			} catch (Exception e) {
				logger.error("Exception in createRecord: ", e);
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
			}
		}

		Map<String, Object> result = new HashMap<>();
		result.put("success", allSuccess);
		return result;
	}

	/**
	 * Retrieves all records from a ServiceNow table.
	 *
	 * @param instanceUrl ServiceNow instance URL.
	 * @param accessToken OAuth2 access token.
	 * @param tableName   Table name.
	 * @param limit       Max number of records.
	 * @return List of records as maps.
	 * @throws Exception if the request fails.
	 */
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

	/**
	 * Retrieves a record by sys_id from a ServiceNow table.
	 *
	 * @param instanceUrl ServiceNow instance URL.
	 * @param accessToken OAuth2 access token.
	 * @param tableName   Table name.
	 * @param sysId       Record sys_id.
	 * @return Map of the record's columns and values, or throws Exception on error.
	 * @throws Exception if the request fails.
	 */
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

	// Code Generated by Sidekick is for learning and experimentation purposes only.
	/**
	 * Updates a record in a ServiceNow table by sys_id (PUT).
	 *
	 * @param instanceUrl ServiceNow instance URL.
	 * @param accessToken OAuth2 access token.
	 * @param tableName   Table name.
	 * @param sysId       Record sys_id.
	 * @param data        Map of fields for update.
	 * @return Map with {"success": true/false, "response": "...", "error": "..."}.
	 * @throws Exception if any request fails.
	 */
	public static Map<String, Object> updateRecord(String instanceUrl, String accessToken, String tableName,
			String sysId, Map<String, String> data) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/" + tableName + "/" + sysId;
		logger.info("Updating record in table: {} with sys_id: {}", tableName, sysId);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json",
				"Content-Type", "application/json");

		boolean success = false;
		String error = "";

		try {
			String jsonString = objectMapper.writeValueAsString(data);
			HttpHelperUtility.putRequestStringBody(endpoint, headers, jsonString, ContentType.APPLICATION_JSON, null,
					null, null);
			success = true;
		} catch (Exception e) {
			logger.error("Exception in updateRecord: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		Map<String, Object> result = new HashMap<>();
		result.put("success", success);
		return result;
	}

	/**
	 * Modifies (PATCH) a record in a ServiceNow table by sys_id.
	 *
	 * @param instanceUrl ServiceNow instance URL.
	 * @param accessToken OAuth2 access token.
	 * @param tableName   Table name.
	 * @param sysId       Record sys_id.
	 * @param data        Map of fields to update.
	 * @return Map with {"success": true/false, "response": "...", "error": "..."}.
	 * @throws Exception if any request fails.
	 */
	public static Map<String, Object> modifyRecord(String instanceUrl, String accessToken, String tableName,
			String sysId, Map<String, String> data) throws Exception {
		String endpoint = instanceUrl + "/api/now/table/" + tableName + "/" + sysId;
		logger.info("Modifying (PATCH) record in table: {} with sys_id: {}", tableName, sysId);

		Map<String, String> headers = Map.of("Authorization", "Bearer " + accessToken, "Accept", "application/json",
				"Content-Type", "application/json");

		Map<String, Object> result = new HashMap<>();
		StringBuilder errorMessages = new StringBuilder();
		boolean success = false;

		try {
			String jsonString = objectMapper.writeValueAsString(data);
			HttpHelperUtility.patchRequestStringBody(endpoint, headers, jsonString, ContentType.APPLICATION_JSON, null,
					null, null);
			success = true;
		} catch (Exception e) {
			logger.error("Exception in modifyRecord: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		result.put("success", success);
		return result;
	}

	/**
	 * Deletes a record by sys_id from a ServiceNow table.
	 *
	 * @param instanceUrl ServiceNow instance URL.
	 * @param accessToken OAuth2 access token.
	 * @param tableName   Table name.
	 * @param sysId       Record sys_id.
	 * @return Map with {"success": true/false, "message": "...", "error": "..."}.
	 * @throws Exception if the request fails.
	 */
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

	public static void insertAPIPermissionData(IRDBMSEngine serviceNowDB, String tableName, String userId, String uuid, String keyName, String type) {
		// TODO Auto-generated method stub
		
	}
}
