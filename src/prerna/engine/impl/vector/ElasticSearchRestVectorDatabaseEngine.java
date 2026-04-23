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
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class ElasticSearchRestVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(ElasticSearchRestVectorDatabaseEngine.class);

	public static final String INDEX_NAME = "INDEX_NAME";

	private static final String TEXT_DATATYPE = "text";
	private static final String KEYWORD_DATATYPE = "keyword";
	private static final String INT_DATATYPE = "integer";

	private static final String SEARCH_ENDPOINT = "/_search";
	private static final String BULK_ENDPOINT = "/_bulk";
	private static final String UPDATE_MAPPINGS_ENDPOINT = "/_mapping";
	private static final String DELETE_BY_QUERY_ENDPOINT = "/_delete_by_query";
	private static final String DELETE_ENDPOINT_STRING = "/delete";

	private static final String EMBEDDINGS_COLUMN = "EMBEDDINGS_COLUMN";
	private static final String DIMENSION_SIZE = "DIMENSION_SIZE";
	private static final String METHOD_NAME = "METHOD_NAME";
	private static final String SPACE_TYPE = "SPACE_TYPE";
	private static final String INDEX_ENGINE = "INDEX_ENGINE";
	private static final String EF_CONSTRUCTION = "EF_CONSTRUCTION";
	private static final String M_VALUE = "M_VALUE";
	private static final String ADDITIONAL_MAPPINGS = "ADDITIONAL_MAPPINGS";

	private String clusterUrl = null;
	private String username = null;
	private String password = null;
	private String apiKey = null;
	private String apiKeyId = null;
	// TODO: move this into enum for apiKey/Creds
	private String authorizationMethod = null;

	private String indexName = null;

	private String embeddings = "embeddings";
	private int dimension = 1024;
	private String methodName = "hnsw";
	private String indexEngine = "lucene";
	private int efConstruction = 128;
	private int m = 24;

	private Map<String, String> otherPropsToType = new HashMap<>();

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.clusterUrl = this.smssProp.getProperty(Constants.HOSTNAME);
		this.username = this.smssProp.getProperty(Constants.USERNAME);
		this.password = this.smssProp.getProperty(Constants.PASSWORD);
		this.apiKey = this.smssProp.getProperty(Constants.API_KEY);
		this.apiKeyId = this.smssProp.getProperty(Constants.API_KEY_ID);

		if (this.apiKey != null && !this.apiKey.trim().isEmpty() && this.apiKeyId != null
				&& !this.apiKeyId.trim().isEmpty()) {
			this.authorizationMethod = "API_KEY";
		} else if (this.username != null && this.password != null && !this.username.trim().isEmpty()
				&& !this.password.trim().isEmpty()) {
			this.authorizationMethod = "BASIC_AUTH";
		} else {
			classLogger.error("Username/Password or ApiKey/Id required");
			throw new IllegalArgumentException("Username/Password or ApiKey/Id required");
		}

		this.indexName = this.smssProp.getProperty(INDEX_NAME);
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
		this.otherPropsToType.put(VectorDatabaseCSVTable.SOURCE, KEYWORD_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.MODALITY, KEYWORD_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.DIVIDER, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.PART, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.TOKENS, INT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.CONTENT, TEXT_DATATYPE);

		getIndex(this.indexName, this.embeddings, this.dimension, this.methodName, this.distanceMethod,
				this.indexEngine, this.efConstruction, this.m);
		updateIndexMapping(this.indexName, this.otherPropsToType);
	}

	@Override
	protected String getDefaultDistanceMethod() {
		return "cosine";
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

		List<JsonObject> bulkInsert = new ArrayList<>();
		Map<String, Integer> fileRecordCountMap = new HashMap<>();
		Map<String, Integer> sourceId = new HashMap<>();
		for (VectorDatabaseCSVRow row : vectorCsvTable.getRows()) {
			String source = row.getSource();
			fileRecordCountMap.put(source, fileRecordCountMap.getOrDefault(source, 0) + 1);
			int index = 0;
			if (sourceId.containsKey(source)) {
				index = sourceId.get(source);
				sourceId.put(source, index + 1);
			} else {
				sourceId.put(source, new Integer(0));
			}

			// store creation of the index
			{
				JsonObject createIndexJson = new JsonObject();
				JsonObject indexDetails = new JsonObject();
				indexDetails.addProperty("_index", this.indexName);
				indexDetails.addProperty("_id", source + "_" + index);
				createIndexJson.add("index", indexDetails);
				bulkInsert.add(createIndexJson);
			}
			// store the actual index details
			{
				JsonObject record = new JsonObject();
				record.addProperty(VectorDatabaseCSVTable.SOURCE, source);
				record.addProperty(VectorDatabaseCSVTable.MODALITY, row.getModality());
				record.addProperty(VectorDatabaseCSVTable.DIVIDER, row.getDivider());
				record.addProperty(VectorDatabaseCSVTable.PART, row.getPart());
				record.addProperty(VectorDatabaseCSVTable.TOKENS, row.getTokens());
				record.addProperty(VectorDatabaseCSVTable.CONTENT, row.getContent());
				record.add(this.embeddings, convertListNumToJsonArray(row.getEmbeddings()));
				bulkInsert.add(record);
			}
		}

		String bulkRequest = String.join("\n", bulkInsert.stream().map(x -> x.toString()).collect(Collectors.toList()))
				+ "\n";

		String url = this.clusterUrl + "/" + this.indexName + BULK_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, bulkRequest,
				ContentType.APPLICATION_JSON, null, null, null);
		if (response == null || (response = response.trim()).isEmpty()) {
			throw new IllegalArgumentException("Received no response from elastic search endpoint");
		}

		Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
		Number insertions = (Number) responseMap.get("took");
		classLogger.info("Inserted {} bulk inserts (create index + record value) into elastic search index '{}'",
				insertions.intValue(), this.indexName);
		Boolean errors = (Boolean) responseMap.get("errors");
		List<Map<String, Object>> items = (List<Map<String, Object>>) responseMap.get("items");
		Map<String, Long> successCountMap = new HashMap<>();
		List<FileEmbeddingStatus> fileStatusMap = new ArrayList<>();
		for (int i = 0; i < items.size(); i++) {
			Map<String, Object> item = items.get(i);
			Map<String, Object> indexResult = (Map<String, Object>) item.get("index");
			String docId = (String) indexResult.get("_id");

			String sourceName = docId.substring(0, docId.lastIndexOf("_"));
			boolean isOk = !indexResult.containsKey("error");

			if (isOk) {
				successCountMap.put(sourceName, successCountMap.getOrDefault(sourceName, 0L) + 1);
			}
		}

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
			fileStatusMap.add(new FileEmbeddingStatus(file, status, inserted, failed, totalRecords));
		}
		if (errors) {
			classLogger.warn(
					"There were errors with some of the bulk insertions in the elastic search index " + this.indexName);
		} else {
			classLogger.info("All records inserted successfully into Elasticsearch index '{}'", this.indexName);
		}

		return fileStatusMap;
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass
				+ FILE_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;

		// construct search query
		JsonObject search = new JsonObject();
		search.addProperty("_source", false);
		search.addProperty("size", 10_000);
		JsonArray fieldsArr = new JsonArray(1);
		fieldsArr.add("_id");
		search.add("fields", fieldsArr);
		{
			JsonObject query = new JsonObject();
			{
				JsonObject terms = new JsonObject();
				terms.add(VectorDatabaseCSVTable.SOURCE, convertListStrToJsonArray(fileNames));
				query.add("terms", terms);
			}
			// add to parent
			search.add("query", query);
		}

		String url = this.clusterUrl + "/" + this.indexName + DELETE_BY_QUERY_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		classLogger.info("For '{}' removed {} docs for files = {}",
				SmssUtilities.getUniqueName(this.engineName, this.engineId), responseJson.get("deleted"), fileNames);
		JsonArray errors = responseJson.get("failures").getAsJsonArray();
		if (errors != null && !errors.isEmpty()) {
			classLogger.warn("For '{}' errors = '{}' when attempting to delete files = {}",
					SmssUtilities.getUniqueName(this.engineName, this.engineId), errors, fileNames);
		}

		// using the search result for the source, we need to delete all the ids we
		// found
		List<String> filesToRemoveFromCloud = new ArrayList<String>();
		for (String document : fileNames) {
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
		search.addProperty("size", limit);
		{
			JsonObject query = new JsonObject();
			{
				if (!parameters.containsKey("filters")) {
					JsonObject knn = new JsonObject();
					{
						knn.add("query_vector", convertListNumToJsonArray(embeddingsResponse.getResponse().get(0)));
						knn.addProperty("k", limit);
						// store key using the field name for the vector in parent
						knn.addProperty("field", this.embeddings);
					}
					// add to parent
					query.add("knn", knn);
				} else if (parameters.containsKey("filters")) {
					JsonObject bool = new JsonObject();
					{
						JsonArray must = new JsonArray();
						{
							JsonObject knnParent = new JsonObject();
							{
								JsonObject knn = new JsonObject();
								{
									knn.add("query_vector",
											convertListNumToJsonArray(embeddingsResponse.getResponse().get(0)));
									knn.addProperty("k", limit);
									// store key using the field name for the vector in parent
									knn.addProperty("field", this.embeddings);
									JsonObject filterParent = new JsonObject();
									{
										JsonObject filterBool = new JsonObject();
										{
											// filteration logic starts here
											// filter contains simple or AND conditions
											JsonArray filter = new JsonArray();

											// should contains OR condition filters
											JsonArray should = new JsonArray();

											// must not contains not equals to filters
											JsonArray must_not = new JsonArray();

											List<IQueryFilter> filters = (List<IQueryFilter>) parameters
													.remove("filters");
											for (IQueryFilter queryFilter : filters) {
												RestVectorQueryFilterTranslationHelper.processFilter(queryFilter,
														filter, should, must_not);
											}

											filterBool.add("filter", filter);
											filterBool.add("should", should);
											filterBool.add("must_not", must_not);

											if (should.size() > 1) {
												filterBool.addProperty("minimum_should_match", 1);
											}
										}
										filterParent.add("bool", filterBool);
									}
									knn.add("filter", filterParent);
								}
								knnParent.add("knn", knn);
							}
							must.add(knnParent);
						}
						bool.add("must", must);
					}
					query.add("bool", bool);
				}
			}
			// add to parent
			search.add("query", query);
		}

		classLogger.debug("ELASTIC FINAL SEARCH QUERY : {}", search);

		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
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
			Double score = hitJson.get("_score").getAsDouble();
			thisMatch.put("Score", score);

			JsonObject sourceDetails = hitJson.get("_source").getAsJsonObject();
			thisMatch.put(VectorDatabaseCSVTable.SOURCE,
					sourceDetails.get(VectorDatabaseCSVTable.SOURCE).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.MODALITY,
					sourceDetails.get(VectorDatabaseCSVTable.MODALITY).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.DIVIDER,
					sourceDetails.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.PART, sourceDetails.get(VectorDatabaseCSVTable.PART).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.TOKENS, sourceDetails.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
			thisMatch.put(VectorDatabaseCSVTable.CONTENT,
					sourceDetails.get(VectorDatabaseCSVTable.CONTENT).getAsString());
		}
		return vectorSearchResults;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		final String UNIQUE_SOURCES = "unique_sources";
		// construct search query
		JsonObject search = new JsonObject();
		{
			JsonObject aggs = new JsonObject();
			{
				JsonObject uniqueScores = new JsonObject();
				{
					JsonObject terms = new JsonObject();
					terms.addProperty("field", VectorDatabaseCSVTable.SOURCE);
					terms.addProperty("min_doc_count", 1);
					// add to parent
					uniqueScores.add("terms", terms);
				}
				// add to parent
				aggs.add(UNIQUE_SOURCES, uniqueScores);
			}
			// add to parent
			search.add("aggs", aggs);
			search.addProperty("size", 0);
		}

		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray bucketsArr = responseJson.getAsJsonObject("aggregations").getAsJsonObject(UNIQUE_SOURCES)
				.getAsJsonArray("buckets");

		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ DOCUMENTS_FOLDER_NAME);

		List<Map<String, Object>> returnSources = new ArrayList<>();
		for (JsonElement bucket : bucketsArr) {
			JsonObject bucketDetails = bucket.getAsJsonObject();
			Map<String, Object> fileInfo = new HashMap<>();
			String fileName = bucketDetails.get("key").getAsString();
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
		// construct search query
		JsonObject search = new JsonObject();
		{
			JsonArray fields = new JsonArray();
			{
				fields.add(VectorDatabaseCSVTable.SOURCE);
				fields.add(VectorDatabaseCSVTable.MODALITY);
				fields.add(VectorDatabaseCSVTable.DIVIDER);
				fields.add(VectorDatabaseCSVTable.PART);
				fields.add(VectorDatabaseCSVTable.TOKENS);
				fields.add(VectorDatabaseCSVTable.CONTENT);
			}
			// add to parent
			search.add("fields", fields);
			search.addProperty("_source", false);
		}

		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT + "?size=10000";
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray hits = getHitsFromSearch(responseJson);

		List<Map<String, Object>> allDocuments = new ArrayList<>();
		for (JsonElement e : hits) {
			Map<String, Object> thisDocument = new HashMap<>();
			allDocuments.add(thisDocument);

			JsonObject fields = e.getAsJsonObject().get("fields").getAsJsonObject();
			thisDocument.put(VectorDatabaseCSVTable.SOURCE, fields.get(VectorDatabaseCSVTable.SOURCE).getAsString());
			thisDocument.put(VectorDatabaseCSVTable.MODALITY,
					fields.get(VectorDatabaseCSVTable.MODALITY).getAsString());
			thisDocument.put(VectorDatabaseCSVTable.DIVIDER, fields.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
			thisDocument.put(VectorDatabaseCSVTable.PART, fields.get(VectorDatabaseCSVTable.PART).getAsString());
			thisDocument.put(VectorDatabaseCSVTable.TOKENS, fields.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
			thisDocument.put(VectorDatabaseCSVTable.CONTENT, fields.get(VectorDatabaseCSVTable.CONTENT).getAsString());
		}

		return allDocuments;
	}

	/**
	 * 
	 * @param responseObject
	 * @return
	 */
	private JsonArray getHitsFromSearch(JsonObject responseObject) {
		JsonObject hitsObject = responseObject.get("hits").getAsJsonObject();
		JsonArray hitsArray = hitsObject.get("hits").getAsJsonArray();
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
		String url = this.clusterUrl + "/" + specificIndexName;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		try {
			HttpHelperUtility.headRequest(url, headersMap, null, null, null);
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
			JsonObject mappings = new JsonObject();
			{
				JsonObject properties = new JsonObject();
				{
					JsonObject thisIndex = new JsonObject();
					thisIndex.addProperty("type", "dense_vector");
					if (dimension > 0) {
						thisIndex.addProperty("dims", dimension);
					}
					thisIndex.addProperty("index", true);
					thisIndex.addProperty("similarity", spaceType);
					{
						JsonObject indexOptions = new JsonObject();
						indexOptions.addProperty("ef_construction", efConstruction);
						indexOptions.addProperty("m", m);
						indexOptions.addProperty("type", methodName);
						// add to parent
						thisIndex.add("index_options", indexOptions);
					}
					// add to parent - key is the embeddings column name
					properties.add(embeddings, thisIndex);
				}
				// add to parent
				mappings.add("properties", properties);
			}
			// add to parent
			createIndexJson.add("mappings", mappings);
		}

		String url = this.clusterUrl + "/" + specificIndexName;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		String response = HttpHelperUtility.putRequestStringBody(url, headersMap, createIndexJson.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		if (!parseResponseForAcknowledged(response)) {
			throw new IllegalArgumentException(
					"Did not receive an acknowledgement from the server for creating the index with the embeddings column");
		}
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
	private void updateIndexMapping(String specificIndexName, Map<String, String> propNameToType) {
		JsonObject updateProperties = new JsonObject();
		{
			JsonObject properties = new JsonObject();
			for (String propName : propNameToType.keySet()) {
				String propType = propNameToType.get(propName);

				JsonObject type = new JsonObject();
				type.addProperty("type", propType);
				properties.add(propName, type);
			}
			// add to parent
			updateProperties.add("properties", properties);
		}

		String url = this.clusterUrl + "/" + this.indexName + UPDATE_MAPPINGS_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		String response = HttpHelperUtility.putRequestStringBody(url, headersMap, updateProperties.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		if (!parseResponseForAcknowledged(response)) {
			throw new IllegalArgumentException(
					"Did not receive an acknowledgement from the server for updating the mappings");
		}
	}

	/**
	 * 
	 * @param response
	 * @return
	 */
	private boolean parseResponseForAcknowledged(String response) {
		if (response == null || (response = response.trim()).isEmpty()) {
			return false;
		}

		Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
		Boolean valid = (Boolean) responseMap.get("acknowledged");
		if (valid != null && valid) {
			return true;
		}

		return false;
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

	/**
	 * 
	 * @param row
	 * @return
	 */
	private JsonArray convertListStrToJsonArray(List<String> row) {
		JsonArray arr = new JsonArray();
		for (int i = 0; i < row.size(); i++) {
			arr.add(row.get(i));
		}
		return arr;
	}

	/**
	 * 
	 * @return
	 */
	private String getCredsBase64Encoded() {
		String encoding = null;
		if (this.authorizationMethod.equals("API_KEY")) {
			encoding = "ApiKey " + Base64.getEncoder().encodeToString((this.apiKeyId + ":" + this.apiKey).getBytes());

		} else if (this.authorizationMethod.equals("BASIC_AUTH")) {
			encoding = "Basic " + Base64.getEncoder().encodeToString((this.username + ":" + this.password).getBytes());
		}

		return encoding;
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.ELASTIC_SEARCH;
	}

}