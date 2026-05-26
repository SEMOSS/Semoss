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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class MilvusVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(MilvusVectorDatabaseEngine.class);

	public static final String DATABASE_NAME = "DATABASE_NAME";
	public static final String COLLECTION_NAME = "COLLECTION_NAME";

	public static final String DEFAULT_DATABASE = "default_database";

	private static final String V2_VECTOR_ENDPOINT = "/v2/vectordb";
	private static final String CREATE_INDEX_ENDPOINT = "/indexes/create";
	private static final String DATABASE_LIST_ENDPOINT = "/databases/list";
	private static final String DATABASE_CREATE_ENDPOINT = "/databases/create";
	private static final String DATABASE_DISCRIBE_ENDPOINT = "/databases/describe";
	private static final String DATABASE_DROP_ENDPOINT = "/databases/drop";

	private static final String COLLECTION_LIST_ENDPOINT = "/collections/list";
	private static final String COLLECTION_CREATE_ENDPOINT = "/collections/create";
	private static final String COLLECTION_DESCRIBE_ENDPOINT = "/collections/describe";
	private static final String COLLECTION_GET_STATS_ENDPOINT = "/collections/get_stats";
	private static final String COLLECTION_GET_LOAD_STATE_ENDPOINT = "/collections/get_load_state";
	private static final String COLLECTION_LOAD_ENDPOINT = "/collections/load";
	private static final String COLLECTION_RELEASE_ENDPOINT = "/collections/release";
	private static final String COLLECTION_RENAME_ENDPOINT = "/collections/rename";
	private static final String COLLECTION_DROP_ENDPOINT = "/collections/drop";

	private static final String ENTITIES_ENDPOINT = "/v2/vectordb/entities";
	private static final String QUERY_ENDPOINT = "/query";
	private static final String INSERT_ENDPOINT = "/insert";
	private static final String DELETE_ENDPOINT = "/delete";
	private static final String SEARCH_ENDPOINT = "/search";

	private static final String INDEX_TYPE = "INDEX_TYPE";
	private static final String EF_CONSTRUCTION = "EF_CONSTRUCTION";
	private static final String M_VALUE = "M_VALUE";

	private final String ID = "id";
	private final String EMBEDDINGS = "vector";

	private String apiKey = null;
	private String milvusUrl = null;
	private String databaseName = null;
	private String collectionName = null;

	private String indexType = "HNSW";
	private int dimension = 1024;
	private int efConstruction = 128;
	private int m = 24;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.apiKey = smssProp.getProperty(Constants.API_KEY);
		if (this.apiKey == null || (this.apiKey = this.apiKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the api key");
		}
		this.milvusUrl = this.smssProp.getProperty(Constants.HOSTNAME);
		this.collectionName = this.smssProp.getProperty(COLLECTION_NAME);
		this.databaseName = smssProp.getProperty(DATABASE_NAME);
		if (this.databaseName != null && ((this.databaseName = this.databaseName.trim()).isEmpty()
				|| DEFAULT_DATABASE.equalsIgnoreCase(this.databaseName))) {
			// keep database as null for all of these
			this.databaseName = null;
		}

		if (this.collectionName == null || (this.collectionName = this.collectionName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Collection name must be provided");
		}

		String indexTypeInput = this.smssProp.getProperty(INDEX_TYPE);
		if (indexTypeInput != null && !(indexTypeInput = indexTypeInput.trim()).isEmpty()) {
			this.indexType = indexTypeInput;
		}
		String efConstructionInput = this.smssProp.getProperty(EF_CONSTRUCTION);
		if (efConstructionInput != null && !(efConstructionInput = efConstructionInput.trim()).isEmpty()) {
			try {
				this.efConstruction = ((Number) Double.parseDouble(efConstructionInput)).intValue();
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid string value for ef construction '" + efConstructionInput
						+ "'. Must be an integer value");
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		String mValueInput = this.smssProp.getProperty(M_VALUE);
		if (mValueInput != null && !(mValueInput = mValueInput.trim()).isEmpty()) {
			try {
				this.m = ((Number) Double.parseDouble(mValueInput)).intValue();
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid string value for m value '" + mValueInput + "'. Must be an integer value");
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		if (this.databaseName != null) {
			if (!doesDatabaseExist()) {
				createDatabase();
			}
		}

		if (!doesCollectionExist()) {
			createCollection();
			createEmbeddingsIndex();
		}
	}

	@Override
	protected String getDefaultDistanceMethod() {
		return "COSINE";
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

		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);

		vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
		JsonArray entities = new JsonArray();
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

			JsonObject record = new JsonObject();
			// Generate a unique primary key for Milvus Vector Database
			record.addProperty(this.ID, source + "_" + index);
			record.addProperty(VectorDatabaseCSVTable.SOURCE, row.getSource());
			record.addProperty(VectorDatabaseCSVTable.MODALITY, row.getModality());
			record.addProperty(VectorDatabaseCSVTable.DIVIDER, row.getDivider());
			record.addProperty(VectorDatabaseCSVTable.PART, row.getPart());
			record.addProperty(VectorDatabaseCSVTable.TOKENS, row.getTokens());
			record.addProperty(VectorDatabaseCSVTable.CONTENT, row.getContent());
			record.add(this.EMBEDDINGS, convertListNumToJsonArray(row.getEmbeddings()));
			entities.add(record);
		}
		JsonObject requestBody = new JsonObject();
		requestBody.addProperty("collectionName", this.collectionName);
		requestBody.add("data", entities);
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		String url = this.milvusUrl + ENTITIES_ENDPOINT + INSERT_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), requestBody.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to add embeddings into collection '{}' within database '{}'", this.collectionName,
					this.databaseName);
			for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
				fileStatusList
						.add(new FileEmbeddingStatus(entry.getKey(), "FAILED", 0, entry.getValue(), entry.getValue()));
			}
			return fileStatusList;
		} else {
			// {"code":0,"cost":284,"data":{"insertCount":228,"insertIds":["documentId1",
			// ...
			JsonObject dataJson = json.get("data").getAsJsonObject();
			long insertedCount = dataJson.get("insertCount").getAsLong();
			classLogger.info("Inserted {} records into Milvus Vector collection: {}", insertedCount,
					this.collectionName);
			for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
				String file = entry.getKey();
				int totalRecords = entry.getValue();

				long inserted = 0;
				long failed = 0;
				String status;

				if (insertedCount == totalRecords) {
					inserted = totalRecords;
					failed = 0;
					status = "SUCCESS";
					insertedCount -= totalRecords;
				} else if (insertedCount > 0 && insertedCount < totalRecords) {
					inserted = insertedCount;
					failed = totalRecords - insertedCount;
					status = "PARTIAL";
					insertedCount = 0;
				} else {
					inserted = 0;
					failed = totalRecords;
					status = "FAILED";
				}

				fileStatusList.add(new FileEmbeddingStatus(file, status, inserted, failed, totalRecords));
			}

			return fileStatusList;
		}
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws Exception {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass
				+ FILE_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;

		JsonObject deleteRequest = new JsonObject();
		deleteRequest.addProperty("collectionName", this.collectionName);

		// Delete by file name filter
		String filter = fileNames.stream()
				.map(fileName -> "Source like '" + fileName.replace("'", "''").replace("\\", "\\\\") + "'")
				.collect(Collectors.joining(" OR "));
		deleteRequest.addProperty("filter", filter);

		String url = this.milvusUrl + ENTITIES_ENDPOINT + DELETE_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), deleteRequest.toString(),
				ContentType.APPLICATION_JSON, null, null, null);

		Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
		Map<String, Object> data = (Map<String, Object>) responseMap.get("data");
		if (data != null) {
			int deleteCount = ((Double) data.get("deleteCount")).intValue();

			if (deleteCount > 0) {
				classLogger.info("Deleted " + deleteCount + " records from Milvus vector database.");
			} else {
				classLogger.warn("No documents found to delete from Milvus vector database.");
			}
		} else {
			classLogger.warn("Failed to delete documents from Milvus vector database: " + response);
		}
		// Remove physical files
		List<String> filesToRemoveFromCloud = new ArrayList<>();
		for (String name : fileNames) {
			String documentName = Paths.get(name).getFileName().toString();
			File documentFile = new File(DOCUMENT_FOLDER, documentName);

			if (documentFile.exists()) {
				try {
					FileUtils.forceDelete(documentFile);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
				filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
			}
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId,
					this.getCatalogType(), filesToRemoveFromCloud.toArray(new String[0])));
			deleteFilesFromCloudThread.start();
		}
	}

	@Override
	protected List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
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
		JsonObject search = new JsonObject();
		search.addProperty("collectionName", this.collectionName);
		search.addProperty("limit", limit);

		JsonArray dataArray = new JsonArray();
		dataArray.add(convertListNumToJsonArray(embeddingsResponse.getResponse().get(0)));
		search.add("data", dataArray);

		JsonArray outputFields = new JsonArray();
		outputFields.add("*");
		search.add("outputFields", outputFields);

		List<IQueryFilter> filters = null;
		List<IQueryFilter> metaFilters = null;
		StringBuilder filterBuilder = new StringBuilder();
		MilvusVectorQueryFitlerTranslationHelper dbfilter = new MilvusVectorQueryFitlerTranslationHelper();

		if (parameters.containsKey(AbstractVectorDatabaseEngine.FILTERS_KEY)) {
			filters = (List<IQueryFilter>) parameters.get(AbstractVectorDatabaseEngine.FILTERS_KEY);
			for (IQueryFilter filter : filters) {
				filterBuilder.append(dbfilter.processMilvusFilter(filter));
			}
		}

		if (parameters.containsKey(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY)) {
			StringBuilder metaFilterBuilder = new StringBuilder();
			metaFilters = (List<IQueryFilter>) parameters.get(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY);
			for (IQueryFilter metaFilter : metaFilters) {
				metaFilterBuilder.append(dbfilter.processMilvusFilter(metaFilter));
			}

			if (filterBuilder.length() > 0 && metaFilterBuilder.length() > 0) {
				filterBuilder.append(" AND ").append(metaFilterBuilder);
			} else {
				filterBuilder.append(metaFilterBuilder);
			}
		}

		if (filterBuilder.length() > 0) {
			search.addProperty("filter", filterBuilder.toString());
		}

		String url = this.milvusUrl + ENTITIES_ENDPOINT + SEARCH_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);

		JsonObject jsonObject = JsonParser.parseString(response).getAsJsonObject();
		JsonArray data = jsonObject.getAsJsonArray("data");
		List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
		for (JsonElement matchEle : data) {
			JsonObject matchObj = matchEle.getAsJsonObject();
			Map<String, Object> retMap = new HashMap<>();
			retMap.put(VectorDatabaseCSVTable.SOURCE, getJsonValue(matchObj.get(VectorDatabaseCSVTable.SOURCE)));
			retMap.put(VectorDatabaseCSVTable.MODALITY, getJsonValue(matchObj.get(VectorDatabaseCSVTable.MODALITY)));
			retMap.put(VectorDatabaseCSVTable.DIVIDER, getJsonValue(matchObj.get(VectorDatabaseCSVTable.DIVIDER)));
			retMap.put(VectorDatabaseCSVTable.PART, getJsonValue(matchObj.get(VectorDatabaseCSVTable.PART)));
			retMap.put(VectorDatabaseCSVTable.TOKENS, getJsonValue(matchObj.get(VectorDatabaseCSVTable.TOKENS)));
			retMap.put(VectorDatabaseCSVTable.CONTENT, getJsonValue(matchObj.get(VectorDatabaseCSVTable.CONTENT)));
			retMap.put("Score", matchObj.get("distance"));
			vectorSearchResults.add(retMap);
		}
		return vectorSearchResults;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ DOCUMENTS_FOLDER_NAME);

		List<Map<String, Object>> filesInMilvus = new ArrayList<>();

		JsonObject queryRequest = new JsonObject();
		queryRequest.addProperty("collectionName", this.collectionName);
		queryRequest.addProperty("limit", 50);
		queryRequest.addProperty("filter", "");

		JsonArray outputFields = new JsonArray();
		outputFields.add(VectorDatabaseCSVTable.SOURCE);
		queryRequest.add("outputFields", outputFields);

		/*
		 * As of 2025-04, there is no way to get distinct. Seems to be in roadmap for
		 * 2025 example response: {"code":0,"cost":6,"data":[ {"Source":"document.pdf",
		 * "id":"document.pdf_0"}, {"Source":"document.pdf", "id":"document.pdf_1"}, ...
		 * ]}
		 */
		String url = this.milvusUrl + ENTITIES_ENDPOINT + QUERY_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), queryRequest.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject jsonObject = JsonParser.parseString(response).getAsJsonObject();
		JsonArray data = jsonObject.getAsJsonArray("data");
		if (data != null && !data.isEmpty()) {
			Set<String> uniqueFileNames = new HashSet<>();

			for (JsonElement recordEle : data) {
				JsonObject record = recordEle.getAsJsonObject();
				String source = record.get(VectorDatabaseCSVTable.SOURCE).getAsString();

				if (!uniqueFileNames.contains(source)) {
					uniqueFileNames.add(source);

					Map<String, Object> fileInfo = new HashMap<>();
					fileInfo.put("fileName", source);
					File thisF = new File(documentsDir, source);
					if (thisF.exists() && thisF.isFile()) {
						long fileSizeInBytes = thisF.length();
						double fileSizeInMB = (double) fileSizeInBytes / (1024);
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						String lastModified = dateFormat.format(new Date(thisF.lastModified()));

						// add file size and last modified into the map
						fileInfo.put("fileSize", fileSizeInMB);
						fileInfo.put("lastModified", lastModified);
					}
					filesInMilvus.add(fileInfo);
				}
			}
		}
		return filesInMilvus;
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		List<Map<String, Object>> documentsList = new ArrayList<>();
		int offset = 0;
		int limit = 100; // Fetch 100 records per request
		boolean hasMoreRecords = true;

		while (hasMoreRecords) {
			JsonObject queryRequest = new JsonObject();
			queryRequest.addProperty("dbName", this.databaseName);
			queryRequest.addProperty("collectionName", this.collectionName);
			queryRequest.addProperty("offset", offset);
			queryRequest.addProperty("limit", limit);

			JsonArray outputFields = new JsonArray();
			outputFields.add("*");
			queryRequest.add("outputFields", outputFields);

			String url = this.milvusUrl + ENTITIES_ENDPOINT + QUERY_ENDPOINT;
			String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), queryRequest.toString(),
					ContentType.APPLICATION_JSON, null, null, null);
			JsonObject jsonObject = JsonParser.parseString(response).getAsJsonObject();
			JsonArray data = jsonObject.getAsJsonArray("data");
			if (data != null && !data.isEmpty()) {
				for (JsonElement recordEle : data) {
					JsonObject record = recordEle.getAsJsonObject();
					Map<String, Object> document = new HashMap<>();
					document.put(VectorDatabaseCSVTable.SOURCE,
							getJsonValue(record.get(VectorDatabaseCSVTable.SOURCE)));
					document.put(VectorDatabaseCSVTable.MODALITY,
							getJsonValue(record.get(VectorDatabaseCSVTable.MODALITY)));
					document.put(VectorDatabaseCSVTable.DIVIDER,
							getJsonValue(record.get(VectorDatabaseCSVTable.DIVIDER)));
					document.put(VectorDatabaseCSVTable.PART, getJsonValue(record.get(VectorDatabaseCSVTable.PART)));
					document.put(VectorDatabaseCSVTable.TOKENS,
							getJsonValue(record.get(VectorDatabaseCSVTable.TOKENS)));
					document.put(VectorDatabaseCSVTable.CONTENT,
							getJsonValue(record.get(VectorDatabaseCSVTable.CONTENT)));
					document.put(this.ID, getJsonValue(record.get(this.ID)));
					documentsList.add(document);
				}
				// if we didn't pull the limit
				// we are done
				if (data.size() != limit) {
					hasMoreRecords = false;
				}
				// Move to the next batch
				offset += limit;
			} else {
				hasMoreRecords = false; // No more data to fetch
			}
		}
		return documentsList;
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

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.MILVUS;
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, String> getHeaders() {
		Map<String, String> headers = new HashMap<>();
		headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
		headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + this.apiKey);
		return headers;
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
	 * Indexing has not been implemented based on the intended use. Will implement
	 * it accordingly
	 */
	private void createEmbeddingsIndex() {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		request.addProperty("collectionName", this.collectionName);

		JsonObject indexParamObject = new JsonObject();
		indexParamObject.addProperty("index_type", this.indexType);
		indexParamObject.addProperty("metricType", this.distanceMethod);
		indexParamObject.addProperty("fieldName", this.EMBEDDINGS);
		indexParamObject.addProperty("indexName", this.EMBEDDINGS + "_index");

		JsonObject params = new JsonObject();
		params.addProperty("M", this.m);
		params.addProperty("efConstruction", this.efConstruction);
		indexParamObject.add("params", params);

		JsonArray indexParamsList = new JsonArray();
		indexParamsList.add(indexParamObject);
		request.add("indexParams", indexParamsList);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + CREATE_INDEX_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") && json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to create index on field '{}' in collection '{}'", "", this.collectionName);
			throw new RuntimeException("Failed to create index in Milvus collection: " + this.collectionName
					+ ". Detailed error = " + json);
		}

		classLogger.info("Index '{}' created successfully for collection '{}'", "", this.collectionName);
	}

	/**
	 * Check if the database exists in Milvus.
	 */
	private boolean doesDatabaseExist() {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + DATABASE_LIST_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to execute database list endpoint");
			throw new RuntimeException("Failed to pull database list endpoint. Detailed error = " + json);
		}

		// example payload = {"code":0,"data":["databaseName1"]}
		JsonArray databases = json.getAsJsonArray("data");
		for (int i = 0; i < databases.size(); i++) {
			if (this.databaseName.equalsIgnoreCase(databases.get(i).getAsString())) {
				return true;
			}
		}

		classLogger.warn("Database '{}' does not exist.", this.databaseName);
		return false;
	}

	/**
	 * Create the database if it does not exist.
	 */
	private void createDatabase() {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + DATABASE_CREATE_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to create database '{}'", this.databaseName);
			throw new RuntimeException("Failed to create database " + this.databaseName + ". Detailed error = " + json);
		}

		classLogger.info("Milvus database '{}' created successfully", this.databaseName);
	}

	/**
	 * 
	 * @return
	 */
	private boolean doesCollectionExist() {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_LIST_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to execute collection list endpoint");
			throw new RuntimeException("Failed to execute collection list endpoint. Detailed error = " + json);
		}

		// example payload = {"code":0,"data":["collectionName1"]}
		JsonArray collections = json.getAsJsonArray("data");
		for (int i = 0; i < collections.size(); i++) {
			if (this.collectionName.equalsIgnoreCase(collections.get(i).getAsString())) {
				return true;
			}
		}

		classLogger.warn("Collection '{}' does not exist in database '{}'.", this.collectionName, this.databaseName);
		return false;
	}

	/**
	 * 
	 */
	private void createCollection() {
		JsonObject request = new JsonObject();
		if (this.databaseName != null) {
			request.addProperty("dbName", this.databaseName);
		}
		request.addProperty("collectionName", this.collectionName);
		request.addProperty("primaryFieldName", this.ID);
		request.addProperty("idType", "VarChar");
		JsonObject params = new JsonObject();
		params.addProperty("max_length", 512);
		request.add("params", params);

		request.addProperty("vectorField", this.EMBEDDINGS);
		request.addProperty("dimension", this.dimension);
		request.addProperty("metricType", this.distanceMethod);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_CREATE_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to create collection '{}' in database '{}'", this.collectionName,
					this.databaseName);
			throw new RuntimeException("Failed to create collection " + this.collectionName + " in database "
					+ this.databaseName + ". Detailed error = " + json);
		}

		classLogger.info("Milvus collection '{}' created successfully", this.collectionName);
	}

	/**
	 * 
	 * @param databaseName
	 * @return
	 */
	private JsonObject describeDatabase(String databaseName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + DATABASE_DISCRIBE_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to describe database '{}'", this.databaseName);
			throw new RuntimeException(
					"Failed to describe database " + this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

	/**
	 * 
	 * @param databaseName
	 * @return
	 */
	private JsonObject dropDatabase(String databaseName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + DATABASE_DROP_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to drop database '{}'", this.databaseName);
			throw new RuntimeException("Failed to drop database " + this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	private JsonObject describeCollection(String collectionName) {
		JsonObject request = new JsonObject();
		if (this.databaseName != null) {
			request.addProperty("dbName", this.databaseName);
		}
		request.addProperty("collectionName", collectionName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_DESCRIBE_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to describe collection '{}' in database '{}'", this.collectionName,
					this.databaseName);
			throw new RuntimeException("Failed to drop collection " + this.collectionName + " in database "
					+ this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	private JsonObject getCollectionStats(String collectionName) {
		JsonObject request = new JsonObject();
		if (this.databaseName != null) {
			request.addProperty("dbName", this.databaseName);
		}
		request.addProperty("collectionName", collectionName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_GET_STATS_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to describe stats for collection '{}' in database '{}'", this.collectionName,
					this.databaseName);
			throw new RuntimeException("Failed to describe stats for collection " + this.collectionName
					+ " in database " + this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

	/**
	 * 
	 * @param collectionName
	 * @param partitionNames
	 * @return
	 */
	private JsonObject getCollectionLoadState(String collectionName, List<String> partitionNames) {
		JsonObject request = new JsonObject();
		if (this.databaseName != null) {
			request.addProperty("dbName", this.databaseName);
		}
		request.addProperty("collectionName", collectionName);
		JsonArray partitions = new JsonArray();
		partitionNames.forEach(partitions::add);
		request.add("partitionNames", partitions);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_GET_LOAD_STATE_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to get collection load state for collection '{}' in database '{}'",
					this.collectionName, this.databaseName);
			throw new RuntimeException("Failed to get collection load state for collection " + this.collectionName
					+ " in database " + this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	private JsonObject loadCollection(String collectionName) {
		JsonObject request = new JsonObject();
		if (this.databaseName != null) {
			request.addProperty("dbName", this.databaseName);
		}
		request.addProperty("collectionName", collectionName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_LOAD_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to load collection '{}' in database '{}'", this.collectionName,
					this.databaseName);
			throw new RuntimeException("Failed to load collection " + this.collectionName + " in database "
					+ this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	private JsonObject releaseCollection(String collectionName) {
		JsonObject request = new JsonObject();
		if (this.databaseName != null) {
			request.addProperty("dbName", this.databaseName);
		}
		request.addProperty("collectionName", collectionName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_RELEASE_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to release collection '{}' in database '{}'", this.collectionName,
					this.databaseName);
			throw new RuntimeException("Failed to release collection " + this.collectionName + " in database "
					+ this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

	/**
	 * 
	 * @param collectionName
	 * @param newCollectionName
	 * @return
	 */
	private JsonObject renameCollection(String collectionName, String newCollectionName) {
		JsonObject request = new JsonObject();
		if (this.databaseName != null) {
			request.addProperty("dbName", this.databaseName);
		}
		request.addProperty("collectionName", collectionName);
		request.addProperty("newCollectionName", newCollectionName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_RENAME_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to rename collection '{}' in database '{}'", this.collectionName,
					this.databaseName);
			throw new RuntimeException("Failed to rename collection " + this.collectionName + " in database "
					+ this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	private JsonObject dropCollection(String collectionName) {
		JsonObject request = new JsonObject();
		if (this.databaseName != null) {
			request.addProperty("dbName", this.databaseName);
		}
		request.addProperty("collectionName", collectionName);

		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + COLLECTION_DROP_ENDPOINT;
		String response = HttpHelperUtility.postRequestStringBody(url, getHeaders(), request.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject json = JsonParser.parseString(response).getAsJsonObject();
		if (!json.has("code") || json.get("code").getAsInt() != 0) {
			classLogger.error("Failed to drop collection '{}' in database '{}'", this.collectionName,
					this.databaseName);
			throw new RuntimeException("Failed to drop collection " + this.collectionName + " in database "
					+ this.databaseName + ". Detailed error = " + json);
		}
		return json;
	}

}
