/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.auth.external;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;
import prerna.util.Constants;
import prerna.util.Utility;

public class ExternalDatabaseMetadataHelper {

	private static final Logger classLogger = LogManager.getLogger(ExternalDatabaseMetadataHelper.class);

	/**
	 * Parses the given JSON string and converts the data elements into OWL format.
	 *
	 * @param json
	 * @param database
	 * @throws Exception
	 */
	public static void parseJsonToOwl(AbstractDatabaseEngine database) throws Exception {
		// Make the api call to pull the data
		String apiResponse = getClientApiJsonResponse(database.getEngineId());

		// Extract table data from JSON structure
		Map<String, Map<String, String>> tableData = extractTableData(apiResponse);

		// Write extracted data to OWL format
		writeToOwl(tableData, database);

		// Add to local master
		Utility.synchronizeEngineMetadata(database.getEngineId());
	}

	/**
	 * Make the api call to pull the metadata
	 *
	 * @param databaseId
	 * @return
	 * @throws Exception
	 */
	private static String getClientApiJsonResponse(String databaseId) throws Exception {
		String url = Utility.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_URL);
		String requestKey = Utility.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_REQUEST_KEY);

		JSONObject requestBody = new JSONObject();
		requestBody.put(requestKey, databaseId);

		String username = Utility.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_AUTH_USERNAME);
		String password = Utility.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_AUTH_PASSWORD);
		String basicAuth = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put("Authorization", "Basic " + basicAuth);
		headersMap.put("Accept", "application/json");
		headersMap.put("Content-Type", "application/json");

		return HttpHelperUtility.postRequestStringBody(url, headersMap, requestBody.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
	}

	/**
	 * Extracts the table name, column name, and data type from the JSON structure.
	 *
	 * @param apiResponse
	 * @return
	 */
	private static Map<String, Map<String, String>> extractTableData(String apiResponse) {
		final String JMES_PATH_EXPRESSION = Utility
				.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_RESPONSE_JMES_PATH);
		if (JMES_PATH_EXPRESSION == null || JMES_PATH_EXPRESSION.isEmpty()) {
			throw new IllegalArgumentException(
					"Must have a valid value for " + Constants.EXTERNAL_DATABASE_MANAGEMENT_RESPONSE_JMES_PATH);
		}
		final String TABLE_KEY = Utility.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_TABLENAME);
		final String COLUMN_KEY = Utility.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_COLUMNNAME);
		final String DATATYPE_KEY = Utility.getDIHelperProperty(Constants.EXTERNAL_DATABASE_MANAGEMENT_DATATYPE);

		Map<String, Map<String, String>> allMetadata = new HashMap<>();
		try {
			// Parse and Manipulate JSON Response
			JsonNode parsedJsonNode = BeanFiller.getJmesResult(apiResponse, JMES_PATH_EXPRESSION);
			if (parsedJsonNode == null) {
				throw new IllegalArgumentException(
						"Unable to process api response = " + apiResponse + " to determine database metadata");
			}
			for (JsonNode detail : parsedJsonNode) {
				String table = detail.path(TABLE_KEY).asText();
				String column = detail.path(COLUMN_KEY).asText();
				String dataType = detail.path(DATATYPE_KEY).asText();

				Map<String, String> tableMap = null;
				if (allMetadata.containsKey(table)) {
					tableMap = allMetadata.get(table);
				} else {
					tableMap = new HashMap<>();
					allMetadata.put(table, tableMap);
				}

				tableMap.put(column, dataType);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return allMetadata;
	}

	/**
	 * Converts the collected table data into OWL format and writes it using the OWL
	 * engine.
	 *
	 * @param tableData
	 * @param database
	 * @throws Exception
	 */
	private static void writeToOwl(Map<String, Map<String, String>> tableData, AbstractDatabaseEngine database)
			throws Exception {
		try (WriteOWLEngine owlWriter = database.getOWLEngineFactory().getWriteOWL()) {
			owlWriter.createEmptyOWLFile();
			for (Map.Entry<String, Map<String, String>> tableEntry : tableData.entrySet()) {
				String tableName = tableEntry.getKey();
				owlWriter.addConcept(tableName);

				for (Map.Entry<String, String> columnEntry : tableEntry.getValue().entrySet()) {
					String columnName = columnEntry.getKey();
					String columnDataType = columnEntry.getValue();

					classLogger.info("Adding table: {} with column: {} (type: {})", tableName, columnName,
							columnDataType);
					owlWriter.addProp(tableName, columnName, columnDataType);
				}
			}

			// Export the OWL file
			owlWriter.export();
		}
	}
}
