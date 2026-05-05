/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.io.connector.servicenow;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;

/**
 * Utility methods for ServiceNow REST operations used by ServiceNow reactors.
 * All methods return plain result objects (maps/lists) and leave reactor-layer
 * wrapping and typing responsibilities to callers.
 */
public final class ServiceNowHelper {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private static final String ACCEPT = "Accept";
	private static final String BEARER = "Bearer ";
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String HEADER_AUTHORIZATION = "Authorization";
	private static final String HEADER_CONTENT_TYPE = "Content-Type";
	private static final String RECORD_URL_KEY = "recordUrl";
	private static final String RESULT_KEY = "result";
	private static final String SUCCESS_KEY = "success";
	private static final String TABLES_KEY = "tables";

	private static final String API_ENDPOINT_SUFFIX = "/api/now/table/";
	private static final String RECORD_URL_ENDPOINT = ".do?sys_id=";
	private static final String SLASH = "/";
	private static final String SYSPARM_LIMIT = "?sysparm_limit=";
	private static final String TABLE_LIST_ENDPOINT_SUFFIX = "/api/now/doc/table/schema";
	private static final String TABLE_METADATA_ENDPOINT_SUFFIX = "/api/now/ui/meta/";

	private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
	}.getType();

	/**
	 * Creates a record in a ServiceNow table.
	 *
	 * @param instanceUrl ServiceNow instance URL
	 * @param accessToken ServiceNow OAuth access token
	 * @param tableName   target ServiceNow table
	 * @param fieldValues field/value map for the new record
	 * @return map containing {@code success} and {@code recordUrl}
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> createRecord(String instanceUrl, String accessToken, String tableName,
			Map<String, Object> fieldValues) {
		try {
			validateServiceNowContext(accessToken, instanceUrl);
			validateRequiredString(tableName, "ServiceNow table name");
			if (fieldValues == null || fieldValues.isEmpty()) {
				throw new IllegalArgumentException("ServiceNow field values must not be empty.");
			}

			String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName;
			classLogger.info("Creating record in ServiceNow table '{}' via URL: {}", tableName, endpoint);

			Map<String, String> headers = getBearerHeader(accessToken);
			headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);

			String jsonBody = GSON.toJson(fieldValues);
			String response = HttpHelperUtility.postRequestStringBody(endpoint, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, MAP_TYPE);

			Object resultObj = responseMap.get(RESULT_KEY);
			if (!(resultObj instanceof Map<?, ?>)) {
				throw new IllegalStateException("Invalid ServiceNow create response: missing result object.");
			}

			Map<String, Object> createdRecord = (Map<String, Object>) resultObj;
			Object sysIdObj = createdRecord.get("sys_id");
			if (!(sysIdObj instanceof String) || ((String) sysIdObj).trim().isEmpty()) {
				throw new IllegalStateException("sys_id not found in ServiceNow create response.");
			}

			String recordUrl = instanceUrl + SLASH + tableName + RECORD_URL_ENDPOINT + sysIdObj;
			Map<String, Object> result = new HashMap<>();
			result.put(SUCCESS_KEY, true);
			result.put(RECORD_URL_KEY, recordUrl);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to create ServiceNow record.", e);
			throw new SemossPixelException("Failed to create ServiceNow record: " + e.getMessage(), e);
		}
	}

	/**
	 * Retrieves records from a ServiceNow table.
	 *
	 * @param instanceUrl ServiceNow instance URL
	 * @param accessToken ServiceNow OAuth access token
	 * @param tableName   target ServiceNow table
	 * @param limit       maximum number of records to return
	 * @return list of records from ServiceNow response {@code result}
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getAllRecords(String instanceUrl, String accessToken, String tableName,
			String limit) {
		try {
			validateServiceNowContext(accessToken, instanceUrl);
			validateRequiredString(tableName, "ServiceNow table name");
			validateRequiredString(limit, "ServiceNow record limit");

			String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SYSPARM_LIMIT + limit;
			classLogger.info("Fetching ServiceNow records from table '{}' with limit {} via URL: {}", tableName, limit,
					endpoint);

			Map<String, String> headers = getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, MAP_TYPE);

			Object resultObj = responseMap.get(RESULT_KEY);
			if (resultObj instanceof List<?>) {
				return (List<Map<String, Object>>) resultObj;
			}

			return new ArrayList<>();
		} catch (Exception e) {
			classLogger.error("Failed to retrieve ServiceNow records.", e);
			throw new SemossPixelException("Failed to retrieve ServiceNow records: " + e.getMessage(), e);
		}
	}

	/**
	 * Retrieves a single record by sys_id from a ServiceNow table.
	 *
	 * @param instanceUrl ServiceNow instance URL
	 * @param accessToken ServiceNow OAuth access token
	 * @param tableName   target ServiceNow table
	 * @param sysId       sys_id of the record
	 * @return record map from ServiceNow response {@code result}
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> getRecordBySysId(String instanceUrl, String accessToken, String tableName,
			String sysId) {
		try {
			validateServiceNowContext(accessToken, instanceUrl);
			validateRequiredString(tableName, "ServiceNow table name");
			validateRequiredString(sysId, "ServiceNow sys_id");

			String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SLASH + sysId;
			classLogger.info("Fetching ServiceNow record '{}' from table '{}' via URL: {}", sysId, tableName, endpoint);

			Map<String, String> headers = getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, MAP_TYPE);

			Object resultObj = responseMap.get(RESULT_KEY);
			if (resultObj instanceof Map<?, ?>) {
				return (Map<String, Object>) resultObj;
			}
			if (resultObj instanceof List<?>) {
				List<Map<String, Object>> resultList = (List<Map<String, Object>>) resultObj;
				if (!resultList.isEmpty()) {
					return resultList.get(0);
				}
			}

			throw new IllegalStateException("No ServiceNow record found for sys_id: " + sysId);
		} catch (Exception e) {
			classLogger.error("Failed to retrieve ServiceNow record by sys_id.", e);
			throw new SemossPixelException("Failed to retrieve ServiceNow record: " + e.getMessage(), e);
		}
	}

	/**
	 * Updates an existing ServiceNow record.
	 *
	 * @param instanceUrl ServiceNow instance URL
	 * @param accessToken ServiceNow OAuth access token
	 * @param tableName   target ServiceNow table
	 * @param sysId       sys_id of the record
	 * @param fieldValues field/value map of updates
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> updateRecord(String instanceUrl, String accessToken, String tableName,
			String sysId, Map<String, Object> fieldValues) {
		try {
			validateServiceNowContext(accessToken, instanceUrl);
			validateRequiredString(tableName, "ServiceNow table name");
			validateRequiredString(sysId, "ServiceNow sys_id");
			if (fieldValues == null || fieldValues.isEmpty()) {
				throw new IllegalArgumentException("ServiceNow field values must not be empty.");
			}

			String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SLASH + sysId;
			classLogger.info("Updating ServiceNow record '{}' in table '{}' via URL: {}", sysId, tableName, endpoint);

			Map<String, String> headers = getBearerHeader(accessToken);
			headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);

			String jsonBody = GSON.toJson(fieldValues);
			HttpHelperUtility.patchRequestStringBody(endpoint, headers, jsonBody, ContentType.APPLICATION_JSON, null,
					null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(SUCCESS_KEY, true);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to update ServiceNow record.", e);
			throw new SemossPixelException("Failed to update ServiceNow record: " + e.getMessage(), e);
		}
	}

	/**
	 * Deletes a ServiceNow record.
	 *
	 * @param instanceUrl ServiceNow instance URL
	 * @param accessToken ServiceNow OAuth access token
	 * @param tableName   target ServiceNow table
	 * @param sysId       sys_id of the record
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> deleteRecord(String instanceUrl, String accessToken, String tableName,
			String sysId) {
		try {
			validateServiceNowContext(accessToken, instanceUrl);
			validateRequiredString(tableName, "ServiceNow table name");
			validateRequiredString(sysId, "ServiceNow sys_id");

			String endpoint = instanceUrl + API_ENDPOINT_SUFFIX + tableName + SLASH + sysId;
			classLogger.info("Deleting ServiceNow record '{}' from table '{}' via URL: {}", sysId, tableName, endpoint);

			Map<String, String> headers = getBearerHeader(accessToken);
			HttpHelperUtility.deleteRequestStringBody(endpoint, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(SUCCESS_KEY, true);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to delete ServiceNow record.", e);
			throw new SemossPixelException("Failed to delete ServiceNow record: " + e.getMessage(), e);
		}
	}

	/**
	 * Retrieves all available table names from a ServiceNow instance.
	 *
	 * @param instanceUrl ServiceNow instance URL
	 * @param accessToken ServiceNow OAuth access token
	 * @return map containing table names under the {@code tables} key
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> getAllTables(String instanceUrl, String accessToken) {
		try {
			validateServiceNowContext(accessToken, instanceUrl);

			String endpoint = instanceUrl + TABLE_LIST_ENDPOINT_SUFFIX;
			classLogger.info("Fetching ServiceNow table list via URL: {}", endpoint);

			Map<String, String> headers = getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, MAP_TYPE);

			Object resultObj = responseMap.get(RESULT_KEY);
			List<String> tableNames = new ArrayList<>();
			if (resultObj instanceof List<?>) {
				List<Map<String, Object>> allTables = (List<Map<String, Object>>) resultObj;
				for (Map<String, Object> table : allTables) {
					Object value = table.get("value");
					if (value instanceof String) {
						tableNames.add((String) value);
					}
				}
			}

			Map<String, Object> result = new HashMap<>();
			result.put(TABLES_KEY, tableNames);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve ServiceNow table list.", e);
			throw new SemossPixelException("Failed to retrieve ServiceNow table list: " + e.getMessage(), e);
		}
	}

	/**
	 * Retrieves metadata for a specific ServiceNow table.
	 *
	 * @param instanceUrl ServiceNow instance URL
	 * @param accessToken ServiceNow OAuth access token
	 * @param tableName   target ServiceNow table
	 * @return table metadata map from ServiceNow response {@code result}
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> getTableMetadata(String instanceUrl, String accessToken, String tableName) {
		try {
			validateServiceNowContext(accessToken, instanceUrl);
			validateRequiredString(tableName, "ServiceNow table name");

			String endpoint = instanceUrl + TABLE_METADATA_ENDPOINT_SUFFIX + tableName;
			classLogger.info("Fetching ServiceNow metadata for table '{}' via URL: {}", tableName, endpoint);

			Map<String, String> headers = getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(endpoint, headers, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, MAP_TYPE);

			Object resultObj = responseMap.get(RESULT_KEY);
			if (resultObj instanceof Map<?, ?>) {
				return (Map<String, Object>) resultObj;
			}

			throw new IllegalStateException("Invalid ServiceNow metadata response for table: " + tableName);
		} catch (Exception e) {
			classLogger.error("Failed to retrieve ServiceNow table metadata.", e);
			throw new SemossPixelException("Failed to retrieve ServiceNow table metadata: " + e.getMessage(), e);
		}
	}

	/**
	 * Builds a standard JSON bearer-auth header map for ServiceNow REST calls.
	 *
	 * @param accessToken ServiceNow OAuth access token
	 * @return request headers containing {@code Authorization} and {@code Accept}
	 */
	public static Map<String, String> getBearerHeader(String accessToken) {
		validateRequiredString(accessToken, "ServiceNow access token");

		Map<String, String> headers = new HashMap<>();
		headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
		headers.put(ACCEPT, CONTENT_TYPE_JSON);
		return headers;
	}

	/**
	 * Validates required ServiceNow request context shared across helper methods.
	 *
	 * @param accessToken ServiceNow OAuth access token
	 * @param instanceUrl ServiceNow instance URL
	 */
	private static void validateServiceNowContext(String accessToken, String instanceUrl) {
		validateRequiredString(accessToken, "ServiceNow access token");
		validateRequiredString(instanceUrl, "ServiceNow instance URL");
	}

	/**
	 * Validates that an input string is not null or blank.
	 *
	 * @param value     string to validate
	 * @param fieldName user-facing field name for exception text
	 */
	private static void validateRequiredString(String value, String fieldName) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException(fieldName + " must not be empty.");
		}
	}

	private ServiceNowHelper() {
		// utility class
	}
}
