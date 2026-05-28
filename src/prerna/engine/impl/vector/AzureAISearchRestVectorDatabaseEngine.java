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
package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;

import net.snowflake.client.jdbc.internal.apache.commons.io.FilenameUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class AzureAISearchRestVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureAISearchRestVectorDatabaseEngine.class);

	public static final String AZURE_AI_SEARCH_VECTOR_ID = "id";
	public static final String API_VERSION = "API_VERSION";
	public static final String AZURE_AI_API_KEY = "api-key";
	public static final String AZURE_API_VERSION = "api-version";

	public static final String INDEX_NAME = "INDEX_NAME";

	private static final String TEXT_DATATYPE = "Edm.String";
	private static final String INT_DATATYPE = "Edm.Int64";
	private static final String EMBEDDINGS_DATATYPE = "Collection(Edm.Single)";

	private static final String CREATE_INDEX = "indexes";
	private static final String SEARCH_ENDPOINT = "indexes/{{INDEX_NAME}}/docs/search";
	private static final String BULK_ENDPOINT = "indexes/{{INDEX_NAME}}/docs/index";
	private static final String DELETE_BY_QUERY_ENDPOINT = "indexes/{{INDEX_NAME}}/docs/index";
	private static final String DOES_INDEX_EXISTS = "indexes/{{INDEX_NAME}}";

	private static final String EMBEDDINGS_COLUMN = "EMBEDDINGS_COLUMN";
	private static final String DIMENSION_SIZE = "DIMENSION_SIZE";
	private static final String METHOD_NAME = "METHOD_NAME";
	private static final String DISTANCE_METHOD = "DISTANCE_METHOD";
	private static final String SPACE_TYPE = "SPACE_TYPE";
	private static final String INDEX_ENGINE = "INDEX_ENGINE";
	private static final String EF_CONSTRUCTION = "EF_CONSTRUCTION";
	private static final String M_VALUE = "M_VALUE";
	private static final String ADDITIONAL_MAPPINGS = "ADDITIONAL_MAPPINGS";
	private static final String INDEX_NAME_PATTERN = "^[a-z0-9](?:[a-z0-9-]{0,126}[a-z0-9])?$";

	private String clusterUrl = null;
	private String apiKey = null;
	private String apiVersion = null;

	private String indexName = null;

	private String embeddings = "embeddings";
	private int dimension = 1024;
	private String methodName = "hnsw";
	private String spaceType = "l2";
	private String indexEngine = "lucene";
	private String distanceMethod = "euclidean";
	private int efConstruction = 128;
	private int m = 24;

	private Map<String, String> otherPropsToType = new HashMap<>();

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.clusterUrl = this.smssProp.getProperty(Constants.HOSTNAME);
		if (this.clusterUrl == null || (this.clusterUrl = this.clusterUrl.trim()).isEmpty()) {
			classLogger.error("Hostname (Cluster URL) is required");
			throw new IllegalArgumentException("Hostname (Cluster URL) is required");
		}
		this.apiKey = this.smssProp.getProperty(Constants.API_KEY);
		this.apiVersion = this.smssProp.getProperty(API_VERSION);
		if (this.apiVersion == null || (this.apiVersion = this.apiVersion.trim()).isEmpty()) {
			this.apiVersion = "2024-07-01";
		}

		if (this.apiKey == null || (this.apiKey = this.apiKey.trim()).isEmpty()) {
			classLogger.error("API Key is required");
			throw new IllegalArgumentException("API Key is required");
		}

		this.indexName = this.smssProp.getProperty(INDEX_NAME);
		if (!this.indexName.matches(INDEX_NAME_PATTERN)) {
			throw new IllegalArgumentException(
					"Index name must only contain lowercase letters, digits or dashes, cannot start or end with dashes and is limited to 128 characters");
		}
		String customEmbeddingsName = this.smssProp.getProperty(EMBEDDINGS_COLUMN);
		if (customEmbeddingsName != null && !(customEmbeddingsName = customEmbeddingsName.trim()).isEmpty()) {
			this.embeddings = customEmbeddingsName;
		}
		String dimensionInput = this.smssProp.getProperty(DIMENSION_SIZE);
		if (dimensionInput != null && !(dimensionInput = dimensionInput.trim()).isEmpty()) {
			try {
				this.dimension = ((Number) Double.parseDouble(dimensionInput)).intValue();
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid string value for dimension '{}'. Must be an integer value", dimensionInput,
						e);
			}
		}
		String methodNameInput = this.smssProp.getProperty(METHOD_NAME);
		if (methodNameInput != null && !(methodNameInput = methodNameInput.trim()).isEmpty()) {
			this.methodName = methodNameInput;
		}

		String distanceMethodInput = this.smssProp.getProperty(DISTANCE_METHOD);
		if (distanceMethodInput != null && !(distanceMethodInput = distanceMethodInput.trim()).isEmpty()) {
			this.distanceMethod = distanceMethodInput;
		}
		String spaceTypeInput = this.smssProp.getProperty(SPACE_TYPE);
		if (spaceTypeInput != null && !(spaceTypeInput = spaceTypeInput.trim()).isEmpty()) {
			this.spaceType = spaceTypeInput;
		}
		String indexEngineInput = this.smssProp.getProperty(INDEX_ENGINE);
		if (indexEngineInput != null && !(indexEngineInput = indexEngineInput.trim()).isEmpty()) {
			this.indexEngine = indexEngineInput;
		}
		String efConstructionInput = this.smssProp.getProperty(EF_CONSTRUCTION);
		if (efConstructionInput != null && !(efConstructionInput = efConstructionInput.trim()).isEmpty()) {
			try {
				this.efConstruction = ((Number) Double.parseDouble(efConstructionInput)).intValue();
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid string value for ef construction '{}'. Must be an integer value",
						efConstructionInput, e);
			}
		}
		String mValueInput = this.smssProp.getProperty(M_VALUE);
		if (mValueInput != null && !(mValueInput = mValueInput.trim()).isEmpty()) {
			try {
				this.m = ((Number) Double.parseDouble(mValueInput)).intValue();
				if (!(this.m > 3 && this.m < 11)) {
					throw new IllegalArgumentException("M_VALUE should be between 4 and 10");
				}
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid string value for m value '{}'. Must be an integer value", mValueInput, e);
			}
		}

		String additionalMappingsStr = this.smssProp.getProperty(ADDITIONAL_MAPPINGS);
		if (additionalMappingsStr != null && !(additionalMappingsStr = additionalMappingsStr.trim()).isEmpty()) {
			this.otherPropsToType = GSON.fromJson(additionalMappingsStr, new TypeToken<Map<String, String>>() {
			}.getType());
		}

		// we need to store our stuff
		this.otherPropsToType.put(AZURE_AI_SEARCH_VECTOR_ID, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.SOURCE, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.MODALITY, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.DIVIDER, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.PART, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.TOKENS, INT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.CONTENT, TEXT_DATATYPE);
		this.otherPropsToType.put(this.embeddings, EMBEDDINGS_DATATYPE);

		getIndex(this.indexName, this.embeddings, this.dimension, this.methodName, this.spaceType, this.indexEngine,
				this.efConstruction, this.m);
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight,
			Map<String, Object> parameters) throws Exception {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}

		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		// if we were able to extract files, begin embeddings process
		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);

		// send all the strings to embed in one shot
		vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
		Map<String, Integer> fileRecordCountMap = new HashMap<>();
		JsonObject bulkInsert = new JsonObject();
		{
			JsonArray value = new JsonArray();

			Map<String, Integer> sourceId = new HashMap<>();
			for (VectorDatabaseCSVRow row : vectorCsvTable.getRows()) {
				String source = row.getSource();
				fileRecordCountMap.put(source, fileRecordCountMap.getOrDefault(source, 0) + 1);
				int index = 0;
				if (sourceId.containsKey(source)) {
					index = sourceId.get(source);
					sourceId.put(source, index + 1);
				} else {
					sourceId.put(source, Integer.valueOf(0));
				}

				// store the actual index details
				{
					JsonObject record = new JsonObject();
					record.addProperty(AZURE_AI_SEARCH_VECTOR_ID, this.sanitizeKey(source) + "_" + index);
					record.addProperty(VectorDatabaseCSVTable.SOURCE, source);
					record.addProperty(VectorDatabaseCSVTable.MODALITY, row.getModality());
					record.addProperty(VectorDatabaseCSVTable.DIVIDER, row.getDivider());
					record.addProperty(VectorDatabaseCSVTable.PART, row.getPart());
					record.addProperty(VectorDatabaseCSVTable.TOKENS, row.getTokens());
					record.addProperty(VectorDatabaseCSVTable.CONTENT, row.getContent());
					record.add(this.embeddings, convertListNumToJsonArray(row.getEmbeddings()));
					value.add(record);
				}
			}
			bulkInsert.add("value", value);
		}

		String url = this.clusterUrl + "/" + BULK_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName) + "?"
				+ this.getMustQueryParamString();
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, bulkInsert.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		if (response == null || (response = response.trim()).isEmpty()) {
			throw new IllegalArgumentException("Received no response from azure ai search endpoint");
		}

		Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
		List<Map<String, Object>> insertions = (List<Map<String, Object>>) responseMap.get("value");
		classLogger.info("Inserted {} bulk inserts (create index + record value) into azure ai search index '{}'",
				insertions.size(), this.indexName);
		Map<String, Long> successCountMap = new HashMap<>();
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (Map<String, Object> result : insertions) {
			String docId = (String) result.get("key");
			boolean succeeded = (boolean) result.getOrDefault("status", true); // true if no error

			String source = docId.substring(0, docId.lastIndexOf("_"));
			if (succeeded) {
				successCountMap.put(source, successCountMap.getOrDefault(source, 0L) + 1);
			}
		}

		// Prepare FileEmbeddingStatus Map
		for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
			String file = entry.getKey();
			int totalRecords = entry.getValue();
			long inserted = successCountMap.getOrDefault(file, 0L);
			long failed = totalRecords - inserted;

			String status;
			if (inserted == totalRecords) {
				status = "SUCCESS";
			} else if (inserted > 0 && inserted < totalRecords) {
				status = "PARTIAL";
			} else {
				status = "FAILED";
			}

			fileStatusList.add(new FileEmbeddingStatus(file, status, inserted, failed, totalRecords));
		}
		Boolean errors = (Boolean) responseMap.get("errors");
		if (errors != null && errors) {
			classLogger.warn("There were errors with some of the bulk insertions in the azure ai search index '{}'",
					this.indexName);
		} else {
			classLogger.info("All records inserted successfully into Azure AI Search index '{}'", this.indexName);
		}

		return fileStatusList;
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws IOException {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		List<String> sourceNames = new ArrayList<>();
		for (String document : fileNames) {
			String documentName = FilenameUtils.getName(document);
			File f = new File(document);
			if (f.exists() && f.getName().endsWith(".csv")) {
				sourceNames.addAll(VectorDatabaseCSVTable.pullSourceColumn(f));
			} else {
				sourceNames.add(documentName);
			}
		}

		// Logic to retrieve Id's against all file names
		JsonObject getIdRq = new JsonObject();
		String searchArr = new String();

		// Get the file name and join it with comma and add to string ex: file1.pdf ,
		// file2.pdf , file3.pdf
		searchArr = String.join(",", sourceNames);
		getIdRq.addProperty("select", AZURE_AI_SEARCH_VECTOR_ID + "," + VectorDatabaseCSVTable.SOURCE);
		getIdRq.addProperty("search", searchArr);
		getIdRq.addProperty("searchFields", VectorDatabaseCSVTable.SOURCE);
		getIdRq.addProperty("count", true);

		classLogger.info("Retriving ids against file name :: Request :: {}", getIdRq);

		String url = this.clusterUrl + "/" + SEARCH_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName) + "?"
				+ this.getMustQueryParamString();

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String responseSearchId = HttpHelperUtility.postRequestStringBody(url, headersMap, getIdRq.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJsonSearchId = JsonParser.parseString(responseSearchId).getAsJsonObject();
		JsonArray sourceArrId = responseJsonSearchId.getAsJsonObject().getAsJsonArray("value");
		classLogger.info("Response source ids :: {}", sourceArrId);
		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass
				+ FILE_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;
		// Delete Rq
		JsonArray valueArr = new JsonArray();

		// loop over this
		for (JsonElement el : sourceArrId) {
			String source = el.getAsJsonObject().get("Source").getAsString();
			classLogger.info("Response :: Source :: '{}' fileNames in Para {}", source, sourceNames);
			if (sourceNames.contains(source)) {
				String fId = el.getAsJsonObject().get("id").getAsString();
				JsonObject sourceRq = new JsonObject();
				sourceRq.addProperty("@search.action", "delete");
				sourceRq.addProperty("id", fId);
				valueArr.add(sourceRq);
			}
		}

		classLogger.info("Request Object for Deleting ::isEmpty Value {}", valueArr.isEmpty());

		if (!valueArr.isEmpty()) {
			JsonObject delRq = new JsonObject();
			delRq.add("value", valueArr);
			String urlDel = this.clusterUrl + "/" + DELETE_BY_QUERY_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName)
					+ "?" + this.getMustQueryParamString();
			headersMap.put(AZURE_AI_API_KEY, this.apiKey);
			headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

			String response = HttpHelperUtility.postRequestStringBody(urlDel, headersMap, delRq.toString(),
					ContentType.APPLICATION_JSON, null, null, null);
			JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
			classLogger.info("For '{}' removed docs for files = {}",
					SmssUtilities.getUniqueName(this.engineName, this.engineId), fileNames);
			JsonArray responseArr = responseJson.get("value").getAsJsonArray();
			JsonArray errors = new JsonArray();
			for (JsonElement el : responseArr) {
				JsonObject respObj = el.getAsJsonObject();

				if (!respObj.get("errorMessage").isJsonNull()) {
					errors.add(respObj);
				}
			}

			if (errors != null && !errors.isEmpty()) {
				classLogger.warn("For '{}' errors = '{}' when attempting to delete files = {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), errors, fileNames);
			}

			// using the search result for the source, we need to delete all the ids we
			// found
			List<String> filesToRemoveFromCloud = new ArrayList<String>();
			for (String document : sourceNames) {
				String documentName = Paths.get(document).getFileName().toString();
				// remove the physical documents
				File documentFile = new File(DOCUMENT_FOLDER, documentName);
				if (documentFile.exists()) {
					try {
						FileUtils.forceDelete(documentFile);
					} catch (IOException e) {
						classLogger.error("Failed to delete document file '{}'", documentFile.getAbsolutePath(), e);
					}
					filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
				}
			}

			if (ClusterUtil.IS_CLUSTER) {
				Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId,
						this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
				deleteFilesFromCloudThread.start();
			}
		}
	}

	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		if (!this.modelPropsLoaded) {
			verifyModelProps();
		}

		IModelEngine engine = Utility.getModel(this.embedderEngineId);
		EmbeddingsModelEngineResponse embeddingsResponse = engine
				.embeddings(Arrays.asList(new String[] { searchStatement }), insight, null);

		// construct search query
		JsonObject search = new JsonObject();
		search.addProperty("count", true);
		search.addProperty("select", "*");
		{
			JsonArray vectorQueries = new JsonArray();
			{
				JsonObject vectorQuery = new JsonObject();
				{
					vectorQuery.add("vector", convertListNumToJsonArray(embeddingsResponse.getResponse().get(0)));
					vectorQuery.addProperty("k", limit);
					vectorQuery.addProperty("fields", this.embeddings);
					vectorQuery.addProperty("kind", "vector");
				}
				vectorQueries.add(vectorQuery);
			}
			search.add("vectorQueries", vectorQueries);
		}
		if (parameters.containsKey("filters")) {
			List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove("filters");
			search.addProperty("vectorFilterMode", "preFilter");
			String filterString = "";
			for (IQueryFilter queryFilter : filters) {
				filterString = processFilter(queryFilter, filterString);
			}
			search.addProperty("filter", filterString);

		}

		String url = this.clusterUrl + "/" + SEARCH_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName) + "?"
				+ this.getMustQueryParamString();
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray hits = getHitsFromSearch(responseJson);

		List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
		for (JsonElement e : hits) {
			Map<String, Object> thisMatch = new HashMap<>();
			vectorSearchResults.add(thisMatch);

			JsonObject hitJson = e.getAsJsonObject();
			Double score = hitJson.get("@search.score").getAsDouble();
			thisMatch.put("Score", score);

			thisMatch.put(VectorDatabaseCSVTable.SOURCE, hitJson.get(VectorDatabaseCSVTable.SOURCE).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.MODALITY, hitJson.get(VectorDatabaseCSVTable.MODALITY).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.DIVIDER, hitJson.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.PART, hitJson.get(VectorDatabaseCSVTable.PART).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.TOKENS, hitJson.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
			thisMatch.put(VectorDatabaseCSVTable.CONTENT, hitJson.get(VectorDatabaseCSVTable.CONTENT).getAsString());
		}
		return vectorSearchResults;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		String url = this.clusterUrl + "/" + SEARCH_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName) + "?"
				+ this.getMustQueryParamString();

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		JsonObject search = new JsonObject();
		{
			JsonArray facets = new JsonArray();
			facets.add(VectorDatabaseCSVTable.SOURCE);
			search.addProperty("search", "*");
			search.addProperty("select", VectorDatabaseCSVTable.SOURCE);
			search.add("facets", facets);
		}
		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray sourceArr = responseJson.getAsJsonObject("@search.facets")
				.getAsJsonArray(VectorDatabaseCSVTable.SOURCE);
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ DOCUMENTS_FOLDER_NAME);
		List<Map<String, Object>> returnSources = new ArrayList<>();
		for (JsonElement bucket : sourceArr) {
			JsonObject bucketDetails = bucket.getAsJsonObject();
			Map<String, Object> fileInfo = new HashMap<>();
			String fileName = bucketDetails.get("value").getAsString();
			fileInfo.put("fileName", fileName);
			File thisF = new File(documentsDir, fileName);
			if (thisF.exists() && thisF.isFile()) {
				long fileSizeInBytes = thisF.length();
				double fileSizeInMB = (double) fileSizeInBytes / (1024);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String lastModified = dateFormat.format(new Date(thisF.lastModified()));

				// add file size and last modified into the map
				fileInfo.put("fileSize", fileSizeInMB);
				fileInfo.put("lastModified", lastModified);
			}

			returnSources.add(fileInfo);
		}
		return returnSources;
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		String url = this.clusterUrl + "/" + SEARCH_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName) + "?"
				+ this.getMustQueryParamString();

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		JsonObject search = new JsonObject();
		{
			search.addProperty("search", "*");
			search.addProperty("orderby", AZURE_AI_SEARCH_VECTOR_ID);
		}
		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray data = responseJson.getAsJsonArray("value");

		List<Map<String, Object>> documentsList = new ArrayList<>();
		for (JsonElement recordEle : data) {
			JsonObject record = recordEle.getAsJsonObject();
			Map<String, Object> document = new HashMap<>();
			document.put(VectorDatabaseCSVTable.SOURCE, getJsonValue(record.get(VectorDatabaseCSVTable.SOURCE)));
			document.put(VectorDatabaseCSVTable.MODALITY, getJsonValue(record.get(VectorDatabaseCSVTable.MODALITY)));
			document.put(VectorDatabaseCSVTable.DIVIDER, getJsonValue(record.get(VectorDatabaseCSVTable.DIVIDER)));
			document.put(VectorDatabaseCSVTable.PART, getJsonValue(record.get(VectorDatabaseCSVTable.PART)));
			document.put(VectorDatabaseCSVTable.TOKENS, getJsonValue(record.get(VectorDatabaseCSVTable.TOKENS)));
			document.put(VectorDatabaseCSVTable.CONTENT, getJsonValue(record.get(VectorDatabaseCSVTable.CONTENT)));
			document.put(AZURE_AI_SEARCH_VECTOR_ID, getJsonValue(record.get(AZURE_AI_SEARCH_VECTOR_ID)));
			documentsList.add(document);
		}
		return documentsList;
	}

	/**
	 * 
	 * @param responseObject
	 * @return
	 */
	private JsonArray getHitsFromSearch(JsonObject responseObject) {
		JsonArray hitsArray = responseObject.get("value").getAsJsonArray();
		return hitsArray;
	}

	/**
	 * 
	 * @param specificIndexName
	 * @param embeddings
	 * @param dimension
	 * @param methodName
	 * @param spaceType
	 * @param engine
	 * @param efConstruction
	 * @param m
	 */
	private void getIndex(String specificIndexName, String embeddings, int dimension, String methodName,
			String spaceType, String engine, int efConstruction, int m) {
		Boolean exisits = doesIndexExsist(specificIndexName);
		if (!exisits) {
			createIndex(specificIndexName, embeddings, dimension, methodName, spaceType, engine, efConstruction, m);
		}
	}

	/**
	 * 
	 * @param specificIndexName
	 * @return
	 */
	private Boolean doesIndexExsist(String specificIndexName) {
		String url = this.clusterUrl + "/" + DOES_INDEX_EXISTS.replace("{{INDEX_NAME}}", specificIndexName) + "?"
				+ this.getMustQueryParamString();
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		try {
			HttpHelperUtility.getRequest(url, headersMap, null, null, null);
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to check if index '{}' exists", specificIndexName, e);
		}
		return false;
	}

	/**
	 * @param specificIndexName
	 * @param dimension
	 * @param methodName
	 * @param spaceType
	 * @param engine
	 * @param efConstruction
	 * @param m
	 */
	private void createIndex(String specificIndexName, String embeddings, int dimension, String methodName,
			String spaceType, String engine, int efConstruction, int m) {
		JsonObject createIndexJson = new JsonObject();
		{
			createIndexJson.addProperty("name", specificIndexName);
			JsonArray fields = new JsonArray();
			{
				for (String propName : this.otherPropsToType.keySet()) {
					String propType = this.otherPropsToType.get(propName);

					JsonObject field = new JsonObject();
					field.addProperty("name", propName);
					field.addProperty("type", propType);
					if (propType.equals(TEXT_DATATYPE)) {
						field.addProperty("searchable", true);
					}
					if (propName.equals(AZURE_AI_SEARCH_VECTOR_ID)) {
						field.addProperty("key", true);
					}
					if (propName.equals(this.embeddings)) {
						field.addProperty("searchable", true);
						field.addProperty("dimensions", this.dimension);
						field.addProperty("vectorSearchProfile", "hnsw_vector_config_profile_" + this.distanceMethod);
						field.addProperty("facetable", false);
						field.addProperty("filterable", false);
						field.addProperty("sortable", false);
					} else {
						field.addProperty("filterable", true);
					}

					fields.add(field);
				}
			}
			// add to parent
			createIndexJson.add("fields", fields);

			JsonObject vectorSearch = new JsonObject();
			{
				JsonArray algorithms = new JsonArray();
				{
					JsonObject algorithm = new JsonObject();
					{
						algorithm.addProperty("name", "hnsw_vector_config_" + this.distanceMethod);
						algorithm.addProperty("kind", this.methodName);
						JsonObject hnswParameters = new JsonObject();
						{
							hnswParameters.addProperty("m", this.m);
							hnswParameters.addProperty("efConstruction", this.efConstruction);
							hnswParameters.addProperty("metric", this.distanceMethod);
						}
						algorithm.add("hnswParameters", hnswParameters);
					}
					algorithms.add(algorithm);
				}
				vectorSearch.add("algorithms", algorithms);

				JsonArray profiles = new JsonArray();
				{
					JsonObject profile = new JsonObject();
					{
						profile.addProperty("name", "hnsw_vector_config_profile_" + this.distanceMethod);
						profile.addProperty("algorithm", "hnsw_vector_config_" + this.distanceMethod);
					}
					profiles.add(profile);
				}
				vectorSearch.add("profiles", profiles);
			}
			// add to parent
			createIndexJson.add("vectorSearch", vectorSearch);
		}

		String url = this.clusterUrl + "/" + CREATE_INDEX + "?" + this.getMustQueryParamString();
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, createIndexJson.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		if (!parseResponseForAcknowledged(response, specificIndexName)) {
			throw new IllegalArgumentException(
					"Did not receive an acknowledgement from the server for creating the index with the embeddings column");
		}
	}

	/**
	 * 
	 * @param response
	 * @return
	 */
	private boolean parseResponseForAcknowledged(String response, String specificIndexName) {
		if (response == null || (response = response.trim()).isEmpty()) {
			return false;
		}

		Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
		String createdIndexName = (String) responseMap.get("name");
		if (createdIndexName.equals(specificIndexName)) {
			return true;
		}

		return false;
	}

	/**
	 * Helper method to extract the correct type from JsonElement.
	 * 
	 * @param element
	 * @return
	 */
	private Object getJsonValue(JsonElement element) {
		if (element == null || element.isJsonNull()) {
			return null;
		} else if (element.isJsonPrimitive()) {
			JsonPrimitive primitive = element.getAsJsonPrimitive();
			if (primitive.isNumber()) {
				return primitive.getAsInt();
			} else if (primitive.isBoolean()) {
				return primitive.getAsBoolean();
			} else {
				return primitive.getAsString();
			}
		} else {
			return element.toString();
		}
	}

	/**
	 * 
	 * @param row
	 * @return
	 */
	private JsonArray convertListNumToJsonArray(List<? extends Number> row) {
		JsonArray arr = new JsonArray();
		for (int i = 0; i < row.size(); i++) {
			arr.add(row.get(i));
		}
		return arr;
	}

	@Override
	protected String getDefaultDistanceMethod() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.AZURE_AI_SEARCH;
	}

	public String getMustQueryParamString() {
		return AZURE_API_VERSION + "=" + this.apiVersion;
	}

	public String sanitizeKey(String key) {
		// Regular expression to match allowed characters
		String regex = "[^a-zA-Z0-9_\\-=]";
		// Replace all characters not matching the regex with an underscore
		return key.replaceAll(regex, "_");
	}

//	FILTER HELPER FUNCTIONS

	public String processFilter(IQueryFilter queryFilter, String filterString) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = queryFilter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return addSimpleQueryFilter((SimpleQueryFilter) queryFilter, filterString);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return addAndFilter((AndQueryFilter) queryFilter, filterString);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return addOrFilter((OrQueryFilter) queryFilter, filterString);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.FUNCTION) {
			throw new IllegalArgumentException(
					"Filters with a Query Filter Type of Function are not supported for Elastic Search vector databases");
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			throw new IllegalArgumentException(
					"Filters with a Query Filter Type of Between are not supported for Elastic Search vector databases");
		}
		return filterString;
	}

	/**
	 * 
	 * @param filter
	 * @param targetArray
	 * @param must_not
	 */
	private String addSimpleQueryFilter(SimpleQueryFilter filter, String filterString) {
		return filterString += processSimpleQueryFilter(filter);
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	public String processSimpleQueryFilter(SimpleQueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();

		FILTER_TYPE fType = filter.getSimpleFilterType();

		if (fType == FILTER_TYPE.COL_TO_COL) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of COL_TO_COL are not supported for Elastic/Open Search vector databases");
		} else if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it
			// is numeric
			return addSelectorToValuesFilter(rightComp, leftComp,
					IQueryFilter.getReverseNumericalComparator(thisComparator));
		} else if (fType == FILTER_TYPE.COL_TO_QUERY) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of COL_TO_QUERY are not supported for Elastic/Open Search vector databases");
		} else if (fType == FILTER_TYPE.QUERY_TO_COL) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of QUERY_TO_COL are not supported for Elastic/Open Search vector databases");
		} else if (fType == FILTER_TYPE.COL_TO_LAMBDA) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of COL_TO_LAMBDA are not supported for Elastic/Open Search vector databases");
		} else if (fType == FILTER_TYPE.LAMBDA_TO_COL) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of LAMBDA_TO_COL are not supported for Elastic/Open Search vector databases");
		} else if (fType == FILTER_TYPE.VALUE_TO_VALUE) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of VALUE_TO_VALUE are not supported for Elastic/Open Search vector databases");
		}
		return null;
	}

	/**
	 * 
	 * @param queryFilter
	 * @param filter
	 * @param should
	 * @param must_not
	 */
	private String addAndFilter(AndQueryFilter queryFilter, String filterString) {
		List<IQueryFilter> filterList = queryFilter.getFilterList();
		int numAnds = filterList.size();
		for (int i = 0; i < numAnds; i++) {
			if (i > 0) {
				filterString += " and ";
			}
			filterString += " ( ";
			IQueryFilter filter2 = filterList.get(i);
			if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				filterString = addSimpleQueryFilter((SimpleQueryFilter) filterList.get(i), filterString);
			} else if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
				filterString = addAndFilter((AndQueryFilter) filterList.get(i), filterString);
			} else if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
				filterString = addOrFilter((OrQueryFilter) filterList.get(i), filterString);
			}
			filterString += " ) ";
		}
		return filterString;

	}

	/**
	 * 
	 * @param queryFilter
	 * @param filter
	 * @param should
	 * @param must_not
	 */
	private String addOrFilter(OrQueryFilter queryFilter, String filterString) {
		List<IQueryFilter> filterList = queryFilter.getFilterList();
		int numAnds = filterList.size();
		for (int i = 0; i < numAnds; i++) {
			if (i > 0) {
				filterString += " or ";
			}
			filterString += " ( ";
			IQueryFilter filter2 = filterList.get(i);
			if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				filterString = addSimpleQueryFilter((SimpleQueryFilter) filterList.get(i), filterString);
			} else if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
				filterString = addOrFilter((OrQueryFilter) filterList.get(i), filterString);
			} else if (filter2.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
				filterString = addAndFilter((AndQueryFilter) filterList.get(i), filterString);
			}
			filterString += " )";
		}
		return filterString;
	}

	/**
	 * 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 * @return
	 */
	private String addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		List<Object> normalizedValues = normalizeToList(rightComp.getValue());
		boolean isNumeric = this.otherPropsToType.get(leftComp.getValue().toString()).equals(INT_DATATYPE);
		if (thisComparator.equals("<") || thisComparator.equals(">") || thisComparator.equals("<=")
				|| thisComparator.equals(">=")) {
			if (!NumberUtils.isCreatable(rightComp.getValue().toString())) {
				throw new IllegalArgumentException("Right hand operand must be a number");
			}
		}
		if (thisComparator.equals("==")) {
			return normalizedValues.stream().map(value -> {
				String val = value.toString().replace("'", "''");
				return isNumeric ? String.format("%s eq %s", leftComp.getValue().toString(), val)
						: String.format("%s eq '%s'", leftComp.getValue().toString(), val);
			}).collect(Collectors.joining(" or "));
		} else if (thisComparator.equals("!=")) {
			return normalizedValues.stream().map(value -> {
				String val = value.toString().replace("'", "''");
				return isNumeric ? String.format("%s ne %s", leftComp.getValue().toString(), val)
						: String.format("%s ne '%s'", leftComp.getValue().toString(), val);
			}).collect(Collectors.joining(" or "));

		} else if (thisComparator.equals("<")) {
			String expression = "";
			expression += leftComp.getValue().toString() + " lt " + rightComp.getValue();
			return expression;
		} else if (thisComparator.equals(">")) {
			String expression = "";
			expression += leftComp.getValue().toString() + " gt " + rightComp.getValue();
			return expression;
		} else if (thisComparator.equals("<=")) {
			String expression = "";
			expression += leftComp.getValue().toString() + " le " + rightComp.getValue();
			return expression;
		} else if (thisComparator.equals(">=")) {
			String expression = "";
			expression += leftComp.getValue().toString() + " ge " + rightComp.getValue();
			return expression;
		} else if (thisComparator.equals("?like")) {
			String expression = "";
			expression += "search.ismatch(" + "'" + rightComp.getValue().toString() + "'," + " '"
					+ leftComp.getValue().toString() + "')";
			return expression;
		} else if (thisComparator.equals("?begins")) {
			throw new IllegalArgumentException(
					"Filters with a Query Filter Type of Begins with are not supported for Azure AI Search vector databases");
		} else if (thisComparator.equals("?ends")) {
			throw new IllegalArgumentException(
					"Filters with a Query Filter Type of Ends with are not supported for Azure AI Search vector databases");
		}

		return null;
	}

	private List<Object> normalizeToList(Object values) {
		if (values instanceof String || values instanceof Number) {
			return Collections.singletonList(values);
		} else if (values instanceof Collection<?>) {
			return ((Collection<?>) values).stream().filter(Objects::nonNull).collect(Collectors.toList());
		} else {
			throw new IllegalArgumentException("Unsupported input type");
		}
	}

}