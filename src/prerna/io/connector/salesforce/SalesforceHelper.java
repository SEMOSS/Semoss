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
package prerna.io.connector.salesforce;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
 * Utility methods for Salesforce REST operations used by Salesforce reactors.
 * All methods return plain result objects (maps) and leave reactor-layer
 * wrapping/typing responsibilities to callers.
 */
public final class SalesforceHelper {

	private static final Logger classLogger = LogManager.getLogger(SalesforceHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private static final String BEARER = "Bearer ";
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String ERRORS_KEY = "errors";
	private static final String FIELDS_KEY = "fields";
	private static final String HEADER_AUTHORIZATION = "Authorization";
	private static final String HEADER_CONTENT_TYPE = "Content-Type";
	private static final String ID_KEY = "id";
	private static final String NAME_KEY = "name";
	private static final String OBJECTS_KEY = "objects";
	private static final String RECORDS_KEY = "records";
	private static final String SEARCH_RECORDS_KEY = "searchRecords";
	private static final String SOBJECTS_KEY = "sobjects";
	private static final String SUCCESS_KEY = "success";
	private static final String TOTAL_SIZE_KEY = "totalSize";

	private static final String FIELDS_DESCRIBE_SUFFIX = "/describe";
	private static final String SOBJECTS_ENDPOINT_SUFFIX = "/services/data/v64.0/sobjects/";
	private static final String QUERY_ENDPOINT_SUFFIX = "/services/data/v64.0/query/?q=";
	private static final String SEARCH_ENDPOINT_SUFFIX = "/services/data/v64.0/search/?q=";

	/**
	 * Fetches Salesforce metadata from the REST API.
	 * <p>
	 * If {@code sObjectName} is blank, this returns all available object API names
	 * under the {@code objects} key. If {@code sObjectName} is provided, this
	 * returns only the field API names for that object under the {@code fields}
	 * key.
	 * </p>
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @param instanceUrl Salesforce instance URL
	 * @param sObjectName optional object API name
	 * @return map containing object or field names
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> fetchObjectSchema(String accessToken, String instanceUrl, String sObjectName) {
		Map<String, Object> result = new HashMap<>();

		try {
			validateSalesforceContext(accessToken, instanceUrl);
			Map<String, String> headers = getBearerHeader(accessToken);

			String objectsUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX;
			classLogger.info("Fetching Salesforce object metadata from URL: {}", objectsUrl);

			String objectsResponse = HttpHelperUtility.getRequest(objectsUrl, headers, null, null, null);
			Map<String, Object> objectsList = GSON.fromJson(objectsResponse, new TypeToken<Map<String, Object>>() {
			}.getType());

			if (sObjectName == null || sObjectName.trim().isEmpty()) {
				List<String> objectNames = new ArrayList<>();
				Object sObjectsPayload = objectsList.get(SOBJECTS_KEY);
				if (sObjectsPayload instanceof List<?>) {
					List<Map<String, Object>> sObjects = (List<Map<String, Object>>) sObjectsPayload;
					for (Map<String, Object> object : sObjects) {
						Object objectName = object.get(NAME_KEY);
						if (objectName instanceof String) {
							objectNames.add((String) objectName);
						}
					}
				}

				result.put(OBJECTS_KEY, objectNames);
				return result;
			}

			String describeUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName + FIELDS_DESCRIBE_SUFFIX;
			classLogger.info("Fetching field metadata for Salesforce object '{}' from URL: {}", sObjectName,
					describeUrl);

			String fieldsResponse = HttpHelperUtility.getRequest(describeUrl, headers, null, null, null);
			Map<String, Object> fieldsMetadata = GSON.fromJson(fieldsResponse, new TypeToken<Map<String, Object>>() {
			}.getType());

			List<String> fieldNames = new ArrayList<>();
			Object fieldsPayload = fieldsMetadata.get(FIELDS_KEY);
			if (fieldsPayload instanceof List<?>) {
				List<Map<String, Object>> fields = (List<Map<String, Object>>) fieldsPayload;
				for (Map<String, Object> field : fields) {
					Object fieldName = field.get(NAME_KEY);
					if (fieldName instanceof String) {
						fieldNames.add((String) fieldName);
					}
				}
			}

			result.put(FIELDS_KEY, fieldNames);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to fetch Salesforce metadata.", e);
			throw new SemossPixelException("Failed to fetch Salesforce metadata: " + e.getMessage(), e);
		}
	}

	/**
	 * Executes a SOQL query against Salesforce.
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @param instanceUrl Salesforce instance URL
	 * @param soqlQuery   SOQL query text
	 * @return map containing {@code totalSize} and {@code records}
	 */
	public static Map<String, Object> runSoqlQuery(String accessToken, String instanceUrl, String soqlQuery) {
		Map<String, Object> result = new HashMap<>();

		try {
			validateSalesforceContext(accessToken, instanceUrl);
			validateRequiredString(soqlQuery, "SOQL query");
			Map<String, String> headers = getBearerHeader(accessToken);

			String fullUrl = instanceUrl + QUERY_ENDPOINT_SUFFIX + URLEncoder.encode(soqlQuery, StandardCharsets.UTF_8);
			classLogger.info("Running SOQL query via URL: {}", fullUrl);

			String response = HttpHelperUtility.getRequest(fullUrl, headers, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

			result.put(TOTAL_SIZE_KEY, responseMap.get(TOTAL_SIZE_KEY));
			result.put(RECORDS_KEY, responseMap.get(RECORDS_KEY));

			return result;
		} catch (Exception e) {
			classLogger.error("Failed to run SOQL query on Salesforce.", e);
			throw new SemossPixelException("Failed to run SOQL query: " + e.getMessage(), e);
		}
	}

	/**
	 * Fetches a Salesforce record by object API name and record ID.
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @param instanceUrl Salesforce instance URL
	 * @param sObjectName object API name (for example, {@code Account})
	 * @param recordId    Salesforce record ID
	 * @return map with the fetched record under {@code fields}
	 */
	public static Map<String, Object> fetchRecordById(String accessToken, String instanceUrl, String sObjectName,
			String recordId) {
		Map<String, Object> result = new HashMap<>();

		try {
			validateSalesforceContext(accessToken, instanceUrl);
			validateRequiredString(sObjectName, "Salesforce object name");
			validateRequiredString(recordId, "Salesforce record Id");
			Map<String, String> headers = getBearerHeader(accessToken);

			String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName + "/" + recordId;
			classLogger.info("Fetching Salesforce record '{}' from object '{}' via URL: {}", recordId, sObjectName,
					fullUrl);

			String response = HttpHelperUtility.getRequest(fullUrl, headers, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

			result.put(FIELDS_KEY, responseMap);
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to fetch Salesforce record by Id.", e);
			throw new SemossPixelException("Failed to fetch Salesforce record by Id: " + e.getMessage(), e);
		}
	}

	/**
	 * Executes a SOSL search query against Salesforce.
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @param instanceUrl Salesforce instance URL
	 * @param soslQuery   SOSL query text
	 * @return map containing {@code searchRecords}
	 */
	public static Map<String, Object> runSoslQuery(String accessToken, String instanceUrl, String soslQuery) {
		Map<String, Object> result = new HashMap<>();

		try {
			validateSalesforceContext(accessToken, instanceUrl);
			validateRequiredString(soslQuery, "SOSL query");
			Map<String, String> headers = getBearerHeader(accessToken);

			String fullUrl = instanceUrl + SEARCH_ENDPOINT_SUFFIX
					+ URLEncoder.encode(soslQuery, StandardCharsets.UTF_8);
			classLogger.info("Running SOSL query via URL: {}", fullUrl);

			String response = HttpHelperUtility.getRequest(fullUrl, headers, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

			result.put(SEARCH_RECORDS_KEY, responseMap.get(SEARCH_RECORDS_KEY));
			return result;
		} catch (Exception e) {
			classLogger.error("Failed to run SOSL query on Salesforce.", e);
			throw new SemossPixelException("Failed to run SOSL query: " + e.getMessage(), e);
		}
	}

	/**
	 * Creates a Salesforce record for a given object.
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @param instanceUrl Salesforce instance URL
	 * @param sObjectName object API name (for example, {@code Contact})
	 * @param fieldValues map of fields and values to create
	 * @return map containing {@code id}, {@code success}, and optional
	 *         {@code errors}
	 */
	public static Map<String, Object> createRecord(String accessToken, String instanceUrl, String sObjectName,
			Map<String, Object> fieldValues) {
		Map<String, Object> result = new HashMap<>();

		try {
			validateSalesforceContext(accessToken, instanceUrl);
			validateRequiredString(sObjectName, "Salesforce object name");
			if (fieldValues == null || fieldValues.isEmpty()) {
				throw new IllegalArgumentException("Field values map must not be empty.");
			}
			Map<String, String> headers = getBearerHeader(accessToken);

			String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName;
			String jsonBody = GSON.toJson(fieldValues);
			classLogger.info("Creating Salesforce record for object '{}' via URL: {}", sObjectName, fullUrl);

			String response = HttpHelperUtility.postRequestStringBody(fullUrl, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

			String recordId = (String) responseMap.get(ID_KEY);
			boolean success = Boolean.TRUE.equals(responseMap.get(SUCCESS_KEY));
			Object errors = responseMap.get(ERRORS_KEY);

			result.put(ID_KEY, recordId);
			result.put(SUCCESS_KEY, success);
			if (errors instanceof List<?> && !((List<?>) errors).isEmpty()) {
				result.put(ERRORS_KEY, errors);
			}

			return result;
		} catch (Exception e) {
			classLogger.error("Failed to create Salesforce record.", e);
			throw new SemossPixelException("Failed to create Salesforce record: " + e.getMessage(), e);
		}
	}

	/**
	 * Deletes a Salesforce record by object API name and record ID.
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @param instanceUrl Salesforce instance URL
	 * @param sObjectName object API name
	 * @param recordId    Salesforce record ID
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> deleteRecord(String accessToken, String instanceUrl, String sObjectName,
			String recordId) {
		Map<String, Object> result = new HashMap<>();

		try {
			validateSalesforceContext(accessToken, instanceUrl);
			validateRequiredString(sObjectName, "Salesforce object name");
			validateRequiredString(recordId, "Salesforce record Id");
			Map<String, String> headers = getBearerHeader(accessToken);

			String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName + "/" + recordId;
			classLogger.info("Deleting Salesforce record '{}' from object '{}' via URL: {}", recordId, sObjectName,
					fullUrl);

			String response = HttpHelperUtility.deleteRequestStringBody(fullUrl, headers, null, null, null);
			boolean success = response == null || response.trim().isEmpty();
			result.put(SUCCESS_KEY, success);

			return result;
		} catch (Exception e) {
			classLogger.error("Failed to delete Salesforce record.", e);
			throw new SemossPixelException("Failed to delete Salesforce record: " + e.getMessage(), e);
		}
	}

	/**
	 * Updates fields for an existing Salesforce record.
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @param instanceUrl Salesforce instance URL
	 * @param sObjectName object API name
	 * @param recordId    Salesforce record ID
	 * @param fieldValues map of fields and updated values
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> updateRecord(String accessToken, String instanceUrl, String sObjectName,
			String recordId, Map<String, Object> fieldValues) {
		Map<String, Object> result = new HashMap<>();

		try {
			validateSalesforceContext(accessToken, instanceUrl);
			validateRequiredString(sObjectName, "Salesforce object name");
			validateRequiredString(recordId, "Salesforce record Id");
			if (fieldValues == null || fieldValues.isEmpty()) {
				throw new IllegalArgumentException("Field values map must not be empty.");
			}
			Map<String, String> headers = getBearerHeader(accessToken);

			String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName + "/" + recordId;
			String jsonBody = GSON.toJson(fieldValues);
			classLogger.info("Updating Salesforce record '{}' for object '{}' via URL: {}", recordId, sObjectName,
					fullUrl);

			String response = HttpHelperUtility.patchRequestStringBody(fullUrl, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);
			boolean success = response == null || response.trim().isEmpty();
			result.put(SUCCESS_KEY, success);

			return result;
		} catch (Exception e) {
			classLogger.error("Failed to update Salesforce record.", e);
			throw new SemossPixelException("Failed to update Salesforce record: " + e.getMessage(), e);
		}
	}

	/**
	 * Builds a standard JSON bearer-auth header map for Salesforce REST calls.
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @return request headers containing {@code Authorization} and
	 *         {@code Content-Type}
	 */
	public static Map<String, String> getBearerHeader(String accessToken) {
		validateRequiredString(accessToken, "Salesforce access token");

		Map<String, String> headers = new HashMap<>();
		headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
		headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
		return headers;
	}

	/**
	 * Validates required Salesforce request context shared across helper methods.
	 *
	 * @param accessToken Salesforce OAuth access token
	 * @param instanceUrl Salesforce instance URL
	 */
	private static void validateSalesforceContext(String accessToken, String instanceUrl) {
		validateRequiredString(accessToken, "Salesforce access token");
		validateRequiredString(instanceUrl, "Salesforce instance URL");
	}

	/**
	 * Validates that an input string is not null/blank.
	 *
	 * @param value     string to validate
	 * @param fieldName user-facing name for exception text
	 */
	private static void validateRequiredString(String value, String fieldName) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException(fieldName + " must not be empty.");
		}
	}

	private SalesforceHelper() {
		// utility class
	}
}
