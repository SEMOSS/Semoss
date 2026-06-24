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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class OpenSearchRestVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(OpenSearchRestVectorDatabaseEngine.class);

	public static final String INDEX_NAME = "INDEX_NAME";

	private static final String TEXT_DATATYPE = "text";
	private static final String KEYWORD_DATATYPE = "keyword";
	private static final String INT_DATATYPE = "integer";

	private static final String SEARCH_ENDPOINT = "/_search";
	private static final String BULK_ENDPOINT = "/_bulk";
	private static final String UPDATE_MAPPINGS_ENDPOINT = "/_mapping";
	private static final String DELETE_BY_QUERY_ENDPOINT = "/_delete_by_query";

	private static final String EMBEDDINGS_COLUMN = "EMBEDDINGS_COLUMN";
	private static final String DIMENSION_SIZE = "DIMENSION_SIZE";
	private static final String METHOD_NAME = "METHOD_NAME";
	private static final String INDEX_ENGINE = "INDEX_ENGINE";
	private static final String EF_CONSTRUCTION = "EF_CONSTRUCTION";
	private static final String M_VALUE = "M_VALUE";
	private static final String ADDITIONAL_MAPPINGS = "ADDITIONAL_MAPPINGS";

	private static final String EXTERNALLY_MANAGED_INDEX = "EXTERNALLY_MANAGED_INDEX";
	private static final String QUERY_LIMIT = "QUERY_LIMIT";
	private static final String BATCH_LIMIT = "BATCH_LIMIT";

	private static final String LIST_DOCUMENTS_QUERY = "LIST_DOCUMENTS_QUERY";
	private static final String LIST_DOCUMENTS_RESULTS_PATH = "LIST_DOCUMENTS_RESULTS_PATH";
	private static final String LIST_ALL_RECORDS_QUERY = "LIST_ALL_RECORDS_QUERY";
	private static final String LIST_ALL_RECORDS_RESULTS_PATH = "LIST_ALL_RECORDS_RESULTS_PATH";
	private static final String NEAREST_NEIGHBOR_QUERY = "NEAREST_NEIGHBOR_QUERY";
	private static final String NEAREST_NEIGHBOR_RESULTS_PATH = "NEAREST_NEIGHBOR_RESULTS_PATH";

	public static final String USE_HYBRID_SEARCH = "USE_HYBRID_SEARCH";
	private static final String HYBRID_SEARCH_PIPELINE_NAME = "semoss-hybrid-pipeline";
	private static final String PIPELINES_ENDPOINT = "/_search/pipeline";

	private static final String DEFAULT_LIST_DOCUMENTS_QUERY = """
				{
				  "size": 0,
				  "aggs": {
				    "unique_sources": {
				      "filter": ${FILTER},
				      "aggs": {
				        "filtered_sources": {
				          "terms": {
				            "field": "%s",
				            "min_doc_count": 1,
				            "size": ${SIZE},
				            "order": {
				              "_key": "asc"
				            }
				          }
				        }
				      }
				    }
				  }
				}
			""".formatted(VectorDatabaseCSVTable.SOURCE);
	private static final String DEFAULT_LIST_DOCUMENTS_RESULTS_PATH = "$..buckets[*].key";
	private static final String DEFAULT_LIST_ALL_RECORDS_QUERY = """
				{
				  "fields": ["%s","%s","%s","%s","%s","%s"],
				  "_source": false,
				  "size": ${SIZE},
				  "query": ${FILTER},
				  "sort": [
				    {"%s": "asc"},
				    {
				      "_script": {
				        "type": "number",
				        "script": {
				          "source": "def idValue = doc['_id'].getValue(); return Integer.parseInt( idValue.substring( idValue.lastIndexOf('_')+1 ) );"
				        },
				        "order": "asc"
				      }
				    }
				  ]
				  ${SEARCH_AFTER}
				}
			"""
			.formatted(VectorDatabaseCSVTable.SOURCE, VectorDatabaseCSVTable.MODALITY, VectorDatabaseCSVTable.DIVIDER,
					VectorDatabaseCSVTable.PART, VectorDatabaseCSVTable.TOKENS, VectorDatabaseCSVTable.CONTENT,
					VectorDatabaseCSVTable.SOURCE);
	private static final String DEFAULT_LIST_ALL_RECORDS_RESULTS_PATH = "$.hits.hits[*].fields";
	private static final String DEFAULT_NEAREST_NEIGHBOR_QUERY = """
				{
				  "from": ${FROM},
				  "size": ${SIZE},
				  "query": {
				    "knn": {
				      "${EMBEDDINGS}": {
				        "vector": ${VECTOR},
				        "k": ${K},
				        "filter": ${FILTER}
				      }
				    }
				  }
				}
			""";
	private static final String HYBRID_PIPELINE_BODY = """
				{
				  "phase_results_processors": [
				    {
				      "normalization-processor": {
				        "normalization": {
				          "technique": "min_max"
				        },
				        "combination": {
				          "technique": "arithmetic_mean"
				        }
				      }
				    }
				  ]
				}
				""";
	private static final String DEFAULT_HYBRID_NEAREST_NEIGHBOR_QUERY = """
				{
				  "from": ${FROM},
				  "size": ${SIZE},
				  "query": {
				    "hybrid": {
				      "queries": [
				        {
				          "match": {
				            "%s": {
				              "query": ${QUERY}
				            }
				          }
				        },
				        {
				          "knn": {
				            "${EMBEDDINGS}": {
				              "vector": ${VECTOR},
				              "k": ${K},
				              "filter": ${FILTER}
				            }
				          }
				        }
				      ]
				    }
				  }
				}
			""".formatted(VectorDatabaseCSVTable.CONTENT);
	private static final String DEFAULT_NEAREST_NEIGHBOR_RESULTS_PATH = "$.hits.hits[*]";

	private String clusterUrl = null;
	private String username = null;
	private String password = null;

	private String indexName = null;

	private String embeddings = "embeddings";
	private int dimension = 1024;
	private String methodName = "hnsw";
	private String indexEngine = "lucene";
	private int efConstruction = 128;
	private int m = 24;

	private boolean externallyManagedIndex = false;
	private int queryLimit = 9999;
	private int batchLimit = 9999;
	private boolean useHybridSearch = false;

	private String listDocumentsQuery = null;
	private String listDocumentsResultsPath = null;
	private String listAllRecordsQuery = null;
	private String listAllRecordsResultsPath = null;
	private String nearestNeighborQuery = null;
	private String nearestNeighborResultsPath = null;

	private Map<String, String> otherPropsToType = new HashMap<>();

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.clusterUrl = this.smssProp.getProperty(Constants.HOSTNAME);
		this.username = this.smssProp.getProperty(Constants.USERNAME);
		this.password = this.smssProp.getProperty(Constants.PASSWORD);

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

		externallyManagedIndex = Boolean
				.parseBoolean(StringUtils.trimToNull(this.smssProp.getProperty(EXTERNALLY_MANAGED_INDEX)));

		String queryLimitInput = this.smssProp.getProperty(QUERY_LIMIT);
		if (queryLimitInput != null && !(queryLimitInput = queryLimitInput.trim()).isEmpty()) {
			try {
				this.queryLimit = ((Number) Double.parseDouble(queryLimitInput)).intValue();
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid string value for query_limit value '{}'. Must be an integer value",
						queryLimitInput, e);
			}
		}
		String batchLimitInput = this.smssProp.getProperty(BATCH_LIMIT);
		if (batchLimitInput != null && !(batchLimitInput = batchLimitInput.trim()).isEmpty()) {
			try {
				this.batchLimit = ((Number) Double.parseDouble(batchLimitInput)).intValue();
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid string value for batch_limit value '{}'. Must be an integer value",
						batchLimitInput, e);
			}
		}

		this.useHybridSearch = Boolean.parseBoolean(this.smssProp.getProperty(USE_HYBRID_SEARCH, "false"));
		if (this.useHybridSearch) {
			ensureHybridPipeline();
		}

		if (!externallyManagedIndex) {
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
		} else if (!doesIndexExist(this.indexName)) {
			throw new IllegalArgumentException("Externally managed index does not exist");
		}

		listDocumentsQuery = (String) ObjectUtils.firstNonNull(
				StringUtils.trimToNull(this.smssProp.getProperty(LIST_DOCUMENTS_QUERY)), DEFAULT_LIST_DOCUMENTS_QUERY);
		listDocumentsResultsPath = (String) ObjectUtils.firstNonNull(
				StringUtils.trimToNull(this.smssProp.getProperty(LIST_DOCUMENTS_RESULTS_PATH)),
				DEFAULT_LIST_DOCUMENTS_RESULTS_PATH);
		listAllRecordsQuery = (String) ObjectUtils.firstNonNull(
				StringUtils.trimToNull(this.smssProp.getProperty(LIST_ALL_RECORDS_QUERY)),
				DEFAULT_LIST_ALL_RECORDS_QUERY);
		listAllRecordsResultsPath = (String) ObjectUtils.firstNonNull(
				StringUtils.trimToNull(this.smssProp.getProperty(LIST_ALL_RECORDS_RESULTS_PATH)),
				DEFAULT_LIST_ALL_RECORDS_RESULTS_PATH);
		nearestNeighborQuery = (String) ObjectUtils.firstNonNull(
				StringUtils.trimToNull(this.smssProp.getProperty(NEAREST_NEIGHBOR_QUERY)),
				DEFAULT_NEAREST_NEIGHBOR_QUERY);
		nearestNeighborResultsPath = (String) ObjectUtils.firstNonNull(
				StringUtils.trimToNull(this.smssProp.getProperty(NEAREST_NEIGHBOR_RESULTS_PATH)),
				DEFAULT_NEAREST_NEIGHBOR_RESULTS_PATH);
	}

	@Override
	protected String getDefaultDistanceMethod() {
		return "cosinesimil";
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight,
			Map<String, Object> parameters) throws Exception {
		if (externallyManagedIndex) {
			throw new NotImplementedException("Embeddings are disabled for this instance");
		}

		if (!modelPropsLoaded) {
			verifyModelProps();
		}

		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		// if we were able to extract files, begin embeddings process
		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		// send all the strings to embed in one shot
		try {
			vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Error occurred creating the embeddings for the generated chunks. Detailed error message = "
							+ e.getMessage(),
					e);
		}

		List<JsonObject> bulkInsert = new ArrayList<>();
		Map<String, Integer> fileRecordCountMap = new HashMap<>();
		Set<String> fileNamesSet = new HashSet<>();
		Map<String, Integer> sourceId = new HashMap<>();
		for (VectorDatabaseCSVRow row : vectorCsvTable.getRows()) {
			String source = row.getSource();
			fileRecordCountMap.put(source, fileRecordCountMap.getOrDefault(source, 0) + 1);
			int index = 0;
			if (sourceId.containsKey(source)) {
				index = sourceId.get(source);
				sourceId.put(source, ++index);
			} else {
				sourceId.put(source, 0);
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
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, bulkRequest,
				ContentType.APPLICATION_JSON, null, null, null);
		if (response == null || (response = response.trim()).isEmpty()) {
			throw new IllegalArgumentException("Received no response from open search endpoint");
		}

		Map<String, Object> responseMap = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
		Number insertions = (Number) responseMap.get("took");
		classLogger.info("Inserted {} bulk inserts (create index + record value) into open search index '{}'",
				insertions.intValue(), this.indexName);
		List<Map<String, Object>> items = (List<Map<String, Object>>) responseMap.get("items");
		Map<String, Integer> failedCountPerFile = new HashMap<>();
		for (Map<String, Object> item : items) {
			Map<String, Object> index = (Map<String, Object>) item.get("index");
			if (index.containsKey("error")) {
				String id = (String) index.get("_id"); // format: fileName_index
				String[] parts = id.split("_");
				String fileName = parts[0];
				failedCountPerFile.put(fileName, failedCountPerFile.getOrDefault(fileName, 0) + 1);
			}
		}
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (String fileName : fileNamesSet) {
			int total = fileRecordCountMap.getOrDefault(fileName, 0);
			int failed = failedCountPerFile.getOrDefault(fileName, 0);
			int inserted = total - failed;

			String status;
			if (failed == 0) {
				status = "SUCCESS";
			} else if (inserted == 0) {
				status = "FAILED";
			} else {
				status = "PARTIAL";
			}

			fileStatusList.add(new FileEmbeddingStatus(fileName, status, inserted, failed, total));
		}

		Boolean errors = (Boolean) responseMap.get("errors");
		if (errors) {
			classLogger.warn("There were errors with some of the bulk insertions in the open search index '{}'",
					this.indexName);
		} else {
			classLogger.info("All records inserted successfully into OpenSearchRest index '{}'", this.indexName);
		}

		return fileStatusList;
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws IOException {
		if (externallyManagedIndex) {
			throw new NotImplementedException("Document management features are disabled for this instance");
		}

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
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
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

	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		String activeQuery = this.useHybridSearch ? DEFAULT_HYBRID_NEAREST_NEIGHBOR_QUERY : nearestNeighborQuery;
		String activeSearchEndpoint = this.useHybridSearch
				? SEARCH_ENDPOINT + "?search_pipeline=" + HYBRID_SEARCH_PIPELINE_NAME
				: SEARCH_ENDPOINT;

		String vectorString = "";
		if (activeQuery.contains("${VECTOR}")) {
			if (!this.modelPropsLoaded) {
				verifyModelProps();
			}
			if (insight == null) {
				throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
			}
			IModelEngine engine = Utility.getModel(this.embedderEngineId);
			EmbeddingsModelEngineResponse embeddingsResponse = engine
					.embeddings(Arrays.asList(new String[] { searchStatement }), insight, null);
			vectorString = GSON.toJson(convertListNumToJsonArray(embeddingsResponse.getResponse().get(0)));
		}

		int size;
		int limitParam = (limit == null) ? queryLimit : limit.intValue();
		int sizeParam = queryLimit;
		if (parameters.containsKey("size")) {
			try {
				sizeParam = Integer.parseInt(parameters.get("size") + "");
			} catch (NumberFormatException e) {
				classLogger.warn("Given illegal size parameter: ", e.getMessage());
			}
		}
		size = Math.min(limitParam, sizeParam);

		int k;
		int kParam = queryLimit;
		if (parameters.containsKey("k")) {
			try {
				kParam = Integer.parseInt(parameters.get("k") + "");
			} catch (NumberFormatException e) {
				classLogger.warn("Given illegal size parameter: ", e.getMessage());
			}
		}
		k = Math.min(kParam, 9999);

		int baseFrom;
		int fromParam = 0;
		if (parameters.containsKey("from")) {
			try {
				fromParam = Integer.parseInt(parameters.get("from") + "");
			} catch (NumberFormatException e) {
				classLogger.warn("Given illegal from parameter: ", e.getMessage());
			}
		}
		baseFrom = Math.max(fromParam, 0);

		Object metaParameter = parameters.get("meta");
		boolean meta = metaParameter != null && Boolean.parseBoolean(metaParameter.toString());

		List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove("filters");
		if (filters == null) {
			filters = new ArrayList<>();
		}
		List<IQueryFilter> pageFilters = new ArrayList<>(filters);
		JsonObject filterObject = getFilterObject(pageFilters);
		String filter = GSON.toJson(filterObject);

		Configuration configuration = Configuration.builder().jsonProvider(new GsonJsonProvider()).build();

		List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
		Map<String, Object> metaData = new HashMap<>();

		int remainingToFetch = size;
		Map<String, String> replacements = new HashMap<>();
		do {
			int batchSize;
			if (remainingToFetch > batchLimit) {
				batchSize = batchLimit;
			} else {
				batchSize = remainingToFetch;
			}

			replacements.clear();
			replacements.put("FROM", Integer.toString(size - remainingToFetch + baseFrom));
			replacements.put("SIZE", Integer.toString(batchSize));
			replacements.put("FILTER", filter);
			replacements.put("QUERY", GSON.toJson(searchStatement));
			replacements.put("EMBEDDINGS", this.embeddings);
			replacements.put("VECTOR", vectorString);
			replacements.put("K", Integer.toString(k));

			StringSubstitutor substitutor = new StringSubstitutor(replacements);
			substitutor.setEnableSubstitutionInVariables(true);
			String searchString = substitutor.replace(activeQuery);

			String searchResponse = getSearchResponse(searchString, activeSearchEndpoint);
			DocumentContext jsonContext = JsonPath.using(configuration).parse(searchResponse);

			Set<String> hitKeys = new HashSet<>();

			JsonArray hits = jsonContext.read(nearestNeighborResultsPath);
			for (JsonElement e : hits) {
				JsonObject hitJson = e.getAsJsonObject();
				Map<String, Object> thisMatch = new HashMap<>();
				vectorSearchResults.add(thisMatch);
				remainingToFetch--;

				Double score = hitJson.get("_score").getAsDouble();

				// Add base information
				thisMatch.put("Score", score);

				JsonObject sourceDetails = hitJson.get("_source").getAsJsonObject();
				String source = sourceDetails.get(VectorDatabaseCSVTable.SOURCE).getAsString();
				hitKeys.add(source);

				thisMatch.put(VectorDatabaseCSVTable.CONTENT,
						sourceDetails.get(VectorDatabaseCSVTable.CONTENT).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.SOURCE, source);
				thisMatch.put(VectorDatabaseCSVTable.MODALITY,
						sourceDetails.get(VectorDatabaseCSVTable.MODALITY).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.DIVIDER,
						sourceDetails.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.PART,
						sourceDetails.get(VectorDatabaseCSVTable.PART).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.TOKENS,
						sourceDetails.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
			}

			if (meta) {
				JsonArray metaHits = jsonContext.read("$.hits.hits[*]");
				for (JsonElement e : metaHits) {
					JsonObject hitJson = e.getAsJsonObject();
					Map<String, Object> thisMatch = new HashMap<>();

					Double score = hitJson.get("_score").getAsDouble();
					thisMatch.put("Score", score);

					JsonObject sourceDetails = hitJson.get("_source").getAsJsonObject();
					if (!sourceDetails.has(VectorDatabaseCSVTable.SOURCE)) {
						continue;
					}
					String source = sourceDetails.get(VectorDatabaseCSVTable.SOURCE).getAsString();
					thisMatch.putAll(sourceDetails.asMap());

					if (!metaData.containsKey(source)) {
						hitKeys.add(source);
						metaData.put(source, thisMatch);
						remainingToFetch--;
					}
				}
			}

			if (hitKeys.isEmpty()) {
				break;
			}
		} while (remainingToFetch > 0);

		if (meta) {
			vectorSearchResults.add(metaData);
		}

		return vectorSearchResults;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		int size;
		Integer sizeParam = null;
		if (parameters.containsKey("size")) {
			try {
				sizeParam = Integer.valueOf(parameters.get("size") + "");
			} catch (NumberFormatException e) {
				classLogger.warn("Given illegal size parameter: ", e.getMessage());
			}
		}
		if (sizeParam != null && sizeParam > 0) {
			size = sizeParam;
		} else {
			size = queryLimit;
		}

		String searchAfter;
		Object searchAfterObj = parameters.get("search_after");
		if (searchAfterObj != null) {
			searchAfter = StringUtils.trimToNull(searchAfterObj.toString());
		} else {
			searchAfter = null;
		}

		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove("filters");
		if (filters == null) {
			filters = new ArrayList<>();
		}

		LinkedHashSet<String> sources = new LinkedHashSet<>();
		List<Map<String, Object>> metaData = new LinkedList<>();
		int remainingToFetch = size;
		Map<String, String> replacements = new HashMap<>();
		do {
			int batchSize;
			if (remainingToFetch > queryLimit) {
				batchSize = queryLimit;
			} else {
				batchSize = remainingToFetch;
			}

			List<IQueryFilter> pageFilters = new ArrayList<>(filters);
			if (searchAfter != null) {
				IQueryFilter sourceFilter = SimpleQueryFilter.makeColToValFilter(VectorDatabaseCSVTable.SOURCE, ">",
						"${SOURCE_OFFSET}");
				pageFilters.add(sourceFilter);
				if (pageFilters.size() > 1) {
					pageFilters = Lists.newArrayList(new AndQueryFilter(pageFilters));
				}
			}
			JsonObject filterObject = getFilterObject(pageFilters);
			String filter = GSON.toJson(filterObject);

			replacements.clear();
			replacements.put("SOURCE_OFFSET", StringEscapeUtils.escapeJava(searchAfter));
			replacements.put("SIZE", Integer.toString(batchSize));
			replacements.put("FILTER", filter);

			StringSubstitutor substitutor = new StringSubstitutor(replacements);
			substitutor.setEnableSubstitutionInVariables(true);
			String searchString = substitutor.replace(listDocumentsQuery);

			String searchResponse = getSearchResponse(searchString);
			List<Object> pageResult = JsonPath.read(searchResponse, listDocumentsResultsPath);

			for (Object o : pageResult) {
				Map<String, Object> resMap;
				if (o instanceof Map) {
					resMap = (Map<String, Object>) o;
				} else {
					resMap = new HashMap<>();
					resMap.put(VectorDatabaseCSVTable.SOURCE, o);
				}

				Map<String, Object> data = new HashMap<>();
				for (String key : resMap.keySet()) {
					Object valObj = resMap.get(key);
					List valList;
					if (valObj == null || !(valObj instanceof List)) {
						valList = Lists.newArrayList(valObj);
					} else {
						valList = (List) valObj;
					}

					if (!valList.isEmpty()) {
						data.put(key, valList.get(0));
						if (VectorDatabaseCSVTable.SOURCE.equalsIgnoreCase(key)) {
							sources.add(valList.get(0) + "");
						}
					}
				}
				metaData.add(data);
			}

			if (sources.isEmpty() || pageResult.isEmpty()) {
				break;
			} else {
				searchAfter = sources.getLast();
			}

			remainingToFetch -= batchSize;
		} while (remainingToFetch > 0);

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ DOCUMENTS_FOLDER_NAME);
		List<Map<String, Object>> returnSources = new ArrayList<>();
		for (String source : sources) {
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
			returnSources.add(fileInfo);
		}

		Object metaParameter = parameters.get("meta");
		if (metaParameter != null && Boolean.parseBoolean(metaParameter.toString())) {
			Map<String, Object> metaMatch = new HashMap<>();
			for (Map<String, Object> m : metaData) {
				String source = m.get(VectorDatabaseCSVTable.SOURCE) + "";
				metaMatch.put(source, m);
			}
			returnSources.add(metaMatch);
		}
		return returnSources;
	}

	public List<Map<String, Object>> listAllRecords() {
		return listAllRecords(new HashMap<>());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		int size;
		Integer sizeParam = null;
		if (parameters.containsKey("size")) {
			try {
				sizeParam = Integer.valueOf(parameters.get("size") + "");
			} catch (NumberFormatException e) {
				classLogger.warn("Given illegal size parameter: ", e.getMessage());
			}
		}
		if (sizeParam != null && sizeParam > 0) {
			size = sizeParam;
		} else {
			size = queryLimit;
		}

		String searchAfter;
		Object searchAfterObj = parameters.get("search_after");
		if (searchAfterObj != null) {
			searchAfter = StringUtils.trimToNull(searchAfterObj.toString());
		} else {
			searchAfter = null;
		}

		boolean canSearchAfter = listAllRecordsQuery.contains("${SEARCH_AFTER}");

		String sourceAfter = searchAfter;
		int indexAfter = -1;
		if (searchAfter != null) {
			int underscoreLocation = searchAfter.lastIndexOf('_');
			if (underscoreLocation > 0) {
				sourceAfter = searchAfter.substring(0, underscoreLocation);
				try {
					indexAfter = Integer.parseInt(searchAfter.substring(underscoreLocation + 1));
				} catch (NumberFormatException e) {
					// ignore
				}
			}
		}

		List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove("filters");
		if (filters == null) {
			filters = new ArrayList<>();
		}

		LinkedList<Map<String, Object>> rows = new LinkedList<>();

		int remainingToFetch = size;
		Map<String, String> replacements = new HashMap<>();

		pageProcessing: {
			do {
				int batchSize;
				if (remainingToFetch > queryLimit) {
					batchSize = queryLimit;
				} else {
					batchSize = remainingToFetch;
				}

				List<IQueryFilter> pageFilters = new ArrayList<>(filters);
				String searchAfterClause = "";
				if (sourceAfter != null) {
					IQueryFilter sourceFilter = SimpleQueryFilter.makeColToValFilter(VectorDatabaseCSVTable.SOURCE,
							">=", "${SOURCE_OFFSET}");
					pageFilters.add(sourceFilter);
					if (pageFilters.size() > 1) {
						pageFilters = Lists.newArrayList(new AndQueryFilter(pageFilters));
					}
					searchAfterClause = ", \"search_after\": [\"${SOURCE_OFFSET}\", ${CHUNK_OFFSET}]";
				}
				JsonObject filterObject = getFilterObject(pageFilters);
				String filter = GSON.toJson(filterObject);

				replacements.clear();
				replacements.put("SOURCE_OFFSET", StringEscapeUtils.escapeJava(sourceAfter));
				replacements.put("CHUNK_OFFSET", Integer.toString(indexAfter));
				replacements.put("SIZE", Integer.toString(batchSize));
				replacements.put("FILTER", filter);
				replacements.put("SEARCH_AFTER", searchAfterClause);

				StringSubstitutor substitutor = new StringSubstitutor(replacements);
				substitutor.setEnableSubstitutionInVariables(true);
				String searchString = substitutor.replace(listAllRecordsQuery);

				String searchResponse = getSearchResponse(searchString);

				Integer totalHits = JsonPath.read(searchResponse, "$.hits.total.value");
				if (totalHits == 0) {
					break;
				}

				List<Object> pageResult = JsonPath.read(searchResponse, listAllRecordsResultsPath);
				if (pageResult == null || pageResult.isEmpty()) {
					break;
				}
				for (Object o : pageResult) {
					Map<String, Object> resMap = (Map<String, Object>) o;
					List<Map<String, Object>> chunksForEntry = new ArrayList<>();
					for (String key : resMap.keySet()) {
						Object valObj = resMap.get(key);

						List valList;
						if (valObj == null || !(valObj instanceof List)) {
							valList = Lists.newArrayList(valObj);
						} else {
							valList = (List) valObj;
						}

						for (int i = 0; i < valList.size(); i++) {
							if (chunksForEntry.size() <= i) {
								chunksForEntry.add(new HashMap<>());
							}
							chunksForEntry.get(i).put(key, valList.get(i));
						}
					}
					if (chunksForEntry.isEmpty()) {
						continue;
					}
					String sourceForEntry = (String) chunksForEntry.get(0).get(VectorDatabaseCSVTable.SOURCE);
					if (!sourceForEntry.equals(sourceAfter)) {
						sourceAfter = sourceForEntry;
						indexAfter = -1;
					}

					// if we can form a chunk_offset-specific search_after then we want to add all
					// results
					// otherwise we may need to start in the middle of a document's chunk array
					int startingIndex = 0;
					if (!canSearchAfter) {
						startingIndex = indexAfter + 1;
					}
					for (int i = startingIndex; i < chunksForEntry.size(); i++) {
						rows.add(chunksForEntry.get(i));
						indexAfter++;
						remainingToFetch--;
						if (remainingToFetch <= 0) {
							break pageProcessing;
						}
					}
				}
				if (totalHits <= batchSize) {
					break;
				}
			} while (remainingToFetch > 0);
		}
		return rows;
	}

	/**
	 * 
	 * @param filters
	 * @return
	 */
	protected JsonObject getFilterObject(List<IQueryFilter> filters) {
		JsonObject filterParent = new JsonObject();
		if (filters.isEmpty()) {
			JsonObject matchAll = new JsonObject();
			filterParent.add("match_all", matchAll);
		} else {
			JsonObject filterBool = new JsonObject();
			{
				// Filtration logic starts here
				// filter contains simple or AND conditions
				JsonArray filter = new JsonArray();

				// should contains OR condition filters
				JsonArray should = new JsonArray();

				// must not contains not equals to filters
				JsonArray must_not = new JsonArray();

				for (IQueryFilter queryFilter : filters) {
					RestVectorQueryFilterTranslationHelper.processFilter(queryFilter, filter, should, must_not);
				}

				// call to process filter
				filterBool.add("filter", filter);
				filterBool.add("should", should);
				filterBool.add("must_not", must_not);

				if (should.size() > 1) {
					filterBool.addProperty("minimum_should_match", 1);
				}
			}
			filterParent.add("bool", filterBool);
		}
		return filterParent;
	}

	/**
	 * Creates the OpenSearch search pipeline used for hybrid (vector + BM25) search.
	 * Applies min-max score normalization and arithmetic mean combination, which is
	 * the standard approach for OpenSearch hybrid search (GA since 2.10).
	 *
	 * Safe to call on every engine startup — PUT is idempotent.
	 */
	private void ensureHybridPipeline() {
		String url = this.clusterUrl + PIPELINES_ENDPOINT + "/" + HYBRID_SEARCH_PIPELINE_NAME;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String response = HttpHelperUtility.putRequestStringBody(url, headersMap, HYBRID_PIPELINE_BODY,
				ContentType.APPLICATION_JSON, null, null, null);
		if (!parseResponseForAcknowledged(response)) {
			classLogger.warn("Did not receive acknowledgement when creating hybrid search pipeline '{}'; "
					+ "hybrid search queries may fail", HYBRID_SEARCH_PIPELINE_NAME);
		} else {
			classLogger.info("Hybrid search pipeline '{}' is ready", HYBRID_SEARCH_PIPELINE_NAME);
		}
	}

	protected String getSearchResponse(String searchBody) {
		return getSearchResponse(searchBody, SEARCH_ENDPOINT);
	}

	protected String getSearchResponse(String searchBody, String searchEndpoint) {
		String url = this.clusterUrl + "/" + this.indexName + searchEndpoint;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		return HttpHelperUtility.postRequestStringBody(url, headersMap, searchBody, ContentType.APPLICATION_JSON, null,
				null, null);
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
	 * @param responseObject
	 * @return JsonArry of hits
	 */
	public JsonArray getSearchResultsWithFilters(JsonObject searchObject) {
		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, searchObject.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray hits = getHitsFromSearch(responseJson);
		return hits;
	}

	/**
	 * https://opensearch.org/docs/latest/search-plugins/knn/knn-index/
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
		Boolean exists = doesIndexExist(specificIndexName);
		if (!exists) {
			createIndex(specificIndexName, embeddings, dimension, methodName, spaceType, engine, efConstruction, m);
		}
	}

	/**
	 * 
	 * @param doesIndexExist
	 * @return
	 */
	private Boolean doesIndexExist(String specificIndexName) {
		String url = this.clusterUrl + "/" + specificIndexName;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		try {
			int status = HttpHelperUtility.headRequestStatus(url, headersMap, null, null, null);
			switch (status) {
			case 200:
				classLogger.info("Recieved 200, indicating that index does exist.");
				return true; // Exists
			case 404:
				classLogger.info("Recieved 404, indicating that index does not exist.");
				return false; // Not exist
			case 401:
			case 403:
				classLogger.error("Auth error checking index existence: HTTP {}", status);
				throw new IllegalStateException("Not authorized to access: " + url);
			default:
				classLogger.warn("Unexpected HTTP status code {} for HEAD {}", status, url);
				return false;
			}
		} catch (Exception e) {
			classLogger.error("Failed HEAD request to {}: {}", url, e.getMessage());
			throw new RuntimeException("Failed to check index existence; see above log.", e);
		}
	}

	/**
	 * https://opensearch.org/docs/latest/search-plugins/knn/knn-index/
	 * 
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
			JsonObject settings = new JsonObject();
			{
				JsonObject index = new JsonObject();
				index.addProperty("knn", true);
				// add to parent
				settings.add("index", index);
			}
			// add to parent
			createIndexJson.add("settings", settings);
			JsonObject mappings = new JsonObject();
			{
				JsonObject properties = new JsonObject();
				{
					JsonObject thisIndex = new JsonObject();
					thisIndex.addProperty("type", "knn_vector");
					thisIndex.addProperty("dimension", dimension);
					{
						JsonObject method = new JsonObject();
						method.addProperty("name", "hnsw");
						method.addProperty("space_type", spaceType);
						method.addProperty("engine", engine);
						{
							JsonObject parameters = new JsonObject();
							parameters.addProperty("ef_construction", efConstruction);
							parameters.addProperty("m", m);
							// add to parent
							method.add("parameters", parameters);
						}
						// add to parent
						thisIndex.add("method", method);
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
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		String response = HttpHelperUtility.putRequestStringBody(url, headersMap, createIndexJson.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		if (!parseResponseForAcknowledged(response)) {
			throw new IllegalArgumentException(
					"Did not receive an acknowledgement from the server for creating the index with the embeddings column");
		}
	}

	/**
	 * https://opensearch.org/docs/latest/search-plugins/knn/knn-index/
	 * 
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
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
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
		String encoding = Base64.getEncoder().encodeToString((this.username + ":" + this.password).getBytes());
		return encoding;
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.OPEN_SEARCH;
	}

}
