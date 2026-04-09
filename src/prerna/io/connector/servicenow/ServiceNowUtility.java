package prerna.io.connector.servicenow;

import java.util.ArrayList;
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

/**
 * Utility methods for ServiceNow REST operations used by ServiceNow reactors.
 * All methods return plain result objects (maps) and leave reactor-layer
 * wrapping/typing responsibilities to callers.
 */
public class ServiceNowUtility {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	private static final String ACCEPT = "Accept";
	private static final String BEARER = "Bearer ";
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String HEADER_AUTHORIZATION = "Authorization";
	private static final String HEADER_CONTENT_TYPE = "Content-Type";
	private static final String RECORD_URL_KEY = "recordUrl";
	private static final String SUCCESS_KEY = "success";
	private static final String TABLES_KEY = "tables";

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

		Map<String, String> headers = getBearerHeader(accessToken);
		headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);

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

		String recordUrl = instanceUrl + SLASH + tableName + RECORD_URL_ENDPOINT + sysId;
		Map<String, Object> result = new HashMap<>();
		result.put(SUCCESS_KEY, allSuccess);
		result.put(RECORD_URL_KEY, recordUrl);
		return result;
	}

	public static List<Map<String, Object>> getAllRecords(String instanceUrl, String accessToken, String tableName,
			String limit) throws Exception {

		String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SYSPARM_LIMIT + limit;
		classLogger.info("Fetching all records from table: {} with limit: {}", tableName, limit);

		Map<String, String> headers = getBearerHeader(accessToken);
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

		Map<String, String> headers = getBearerHeader(accessToken);
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

		Map<String, String> headers = getBearerHeader(accessToken);
		headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);

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

		result.put(SUCCESS_KEY, success);
		return result;
	}

	public static Map<String, Object> deleteRecord(String instanceUrl, String accessToken, String tableName,
			String sysId) throws Exception {

		String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SLASH + sysId;
		classLogger.info("Deleting record from table: {} with sys_id: {}", tableName, sysId);

		Map<String, String> headers = getBearerHeader(accessToken);

		Map<String, Object> result = new HashMap<>();
		boolean success = false;

		try {
			HttpHelperUtility.deleteRequestStringBody(endpoint, headers, null, null, null);
			success = true;

		} catch (Exception e) {
			classLogger.error("Exception in deleteRecord: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		result.put(SUCCESS_KEY, success);
		return result;
	}

	public static Map<String, Object> getAllTables(String instanceUrl, String accessToken) throws Exception {

		String endpoint = instanceUrl + TABLE_LIST_ENDPOINT_SUFFIX;
		classLogger.info("Fetching all tables from ServiceNow instance: " + instanceUrl);

		Map<String, String> headers = getBearerHeader(accessToken);
		try {
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> jsonMap = objectMapper.readValue(response, Map.class);

			List<Map<String, Object>> allTables = (List<Map<String, Object>>) jsonMap.get("result");

			// extract only table names
			List<String> tableNames = new ArrayList<>();
			
			for(Map<String, Object> table : allTables) {
				Object value = table.get("value");
				if (value instanceof String) {
					tableNames.add((String)value);
				}
			}
			Map<String, Object> responseMap = new HashMap<>();
			responseMap.put(TABLES_KEY, tableNames);
			return responseMap;

		} catch (Exception e) {
			classLogger.error("Exception in getAllTables: ", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	public static Map<String, Object> getTableMetadata(String instanceUrl, String accessToken, String tableName)
			throws Exception {

		String endpoint = instanceUrl + TABLE_METADATA_ENDPOINT_SUFFIX + tableName;
		classLogger.info("Fetching table metadata for {} table", tableName);

		Map<String, String> headers = getBearerHeader(accessToken);
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

        AccessToken servicenowToken = user.getAccessToken(AuthProvider.SERVICENOW);

        if (servicenowToken == null) {
        	classLogger.error("No ServiceNow Access Token fetched for user");
            throw new SemossPixelException("No ServiceNow Access Token fetched.");
        }

        accessToken = servicenowToken.getAccess_token();
        return accessToken;
	}

	public static Map<String, String> getBearerHeader(String accessToken) {
		Map<String, String> headers = new HashMap<>();
		headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
		headers.put(ACCEPT, CONTENT_TYPE_JSON);
		return headers;
	}
}
