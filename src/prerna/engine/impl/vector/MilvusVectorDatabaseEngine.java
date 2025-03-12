package prerna.engine.impl.vector;

import java.io.File;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.io.IOException;
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
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHeaders;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.http.entity.ContentType;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
	private static final String V2_VECTOR_ENDPOINT = "/v2/vectordb";
	private static final String CREATE_INDEX_ENDPOINT = "/indexes/create";
	private static final String DATABASE_LIST_ENDPOINT ="/databases/list";
	private static final String DATABASE_CREATE_ENDPOINT = "/databases/create";
	private static final String DATABASE_DISCRIBE_ENDPOINT = "/databases/describe";
	private static final String DATABASE_DROP_ENDPOINT = "/databases/drop";
	public static final String COLLECTION_NAME = "COLLECTION_NAME";
	private static final String COLLECTION_LIST_ENDPOINT = "/collections/list";
	private static final String COLLECTION_CREATE_ENDPOINT = "/collections/create";
	private static final String COLLECTION_DESCRIBE_ENDPOINT = "/collections/describe";
	private static final String COLLECTION_GET_STATS_ENDPOINT = "/collections/get_stats";
	private static final String COLLECTION_HAS_COLLECTION_ENDPOINT = "/collections/has";
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
	private String apiKey = null;
	private String milvusUrl = null;
	private String databaseName = null;
	private String collectionName = null;
	private String embeddings = "vector";

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

		    if (!doesDatabaseExist()) {
		        createDatabase();
		    }

		    if (!doesCollectionExist()) {
		        createCollection();
		    }
	}

	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters)
			throws Exception {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}

		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);

		vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
		List<JsonObject> entities = new ArrayList<>();

		for (VectorDatabaseCSVRow row : vectorCsvTable.getRows()) {

			// Generate a unique primary key for Milvus Vector Database
			int id = row.getContent().hashCode();

			JsonObject record = new JsonObject();
			record.addProperty("id", id);
			record.addProperty(VectorDatabaseCSVTable.SOURCE, row.getSource());
			record.addProperty(VectorDatabaseCSVTable.MODALITY, row.getModality());
			record.addProperty(VectorDatabaseCSVTable.DIVIDER, row.getDivider());
			record.addProperty(VectorDatabaseCSVTable.PART, row.getPart());
			record.addProperty(VectorDatabaseCSVTable.TOKENS, row.getTokens());
			record.addProperty(VectorDatabaseCSVTable.CONTENT, row.getContent());
			record.add(this.embeddings, convertListNumToJsonArray(row.getEmbeddings()));
			entities.add(record);
		}
		JsonObject requestBody = new JsonObject();
		requestBody.addProperty("collectionName", this.collectionName);
		requestBody.add("data", new Gson().toJsonTree(entities));

		String url = this.milvusUrl + ENTITIES_ENDPOINT + INSERT_ENDPOINT;
		Map<String, String> headers = getHeaders();

		try {
			String response = HttpHelperUtility.postRequestStringBody(url, headers, requestBody.toString(),
					ContentType.APPLICATION_JSON, null, null, null);

			if (response == null || response.trim().isEmpty()) {
				throw new RuntimeException("Failed to insert embeddings into Milvus");
			}

			classLogger.info("Inserted {} records into Milvus Vector collection: {}", entities.size(),
					this.collectionName);
		} catch (Exception e) {
			classLogger.error("Error inserting embeddings into Milvus Vector Database: {}", e.getMessage());
			throw new RuntimeException("Insertion into Milvus Vector Database failed", e);
		}
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws Exception {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR
				+ AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;

		JsonObject deleteRequest = new JsonObject();
		deleteRequest.addProperty("collectionName", this.collectionName);

		// Delete by file name filter
		String filter = fileNames.stream().map(fileName -> "Source like \"" + fileName + "\"")
				.collect(Collectors.joining(" OR "));
		deleteRequest.addProperty("filter", filter);
		String url = this.milvusUrl + ENTITIES_ENDPOINT + DELETE_ENDPOINT;
		Map<String, String> headersMap = getHeaders();
		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, deleteRequest.toString(),
				ContentType.APPLICATION_JSON, null, null, null);

		Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {
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
		List<String> filesToRemoveFromCloud = fileNames.stream()
				.map(name -> new File(DOCUMENT_FOLDER, Paths.get(name).getFileName().toString())).filter(File::exists)
				.peek(file -> {
					try {
						FileUtils.forceDelete(file);
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}).map(File::getAbsolutePath).collect(Collectors.toList());

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
				filterBuilder.append((dbfilter.processMilvusFilter((IQueryFilter) filter)));
			}
			search.addProperty("filter", filterBuilder.toString());
		}
		
		if (parameters.containsKey(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY)) {
			metaFilters = (List<IQueryFilter>) parameters.get(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY);
			for (IQueryFilter metaFilter : metaFilters) {
				filterBuilder.append((dbfilter.processMilvusFilter((IQueryFilter) metaFilter)));
			}
			search.addProperty("filter", filterBuilder.toString());
		}

		String url = this.milvusUrl + ENTITIES_ENDPOINT + SEARCH_ENDPOINT;
		Map<String, String> headersMap = getHeaders();

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
		List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
		List<Map<String, Object>> hits = (List<Map<String, Object>>) responseMap.get("data");
		for (Map<String, Object> match : hits) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put(VectorDatabaseCSVTable.SOURCE, match.get(VectorDatabaseCSVTable.SOURCE));
			retMap.put(VectorDatabaseCSVTable.MODALITY, match.get(VectorDatabaseCSVTable.MODALITY));
			retMap.put(VectorDatabaseCSVTable.DIVIDER, match.get(VectorDatabaseCSVTable.DIVIDER));
			retMap.put(VectorDatabaseCSVTable.PART, match.get(VectorDatabaseCSVTable.PART));
			retMap.put(VectorDatabaseCSVTable.TOKENS, match.get(VectorDatabaseCSVTable.TOKENS));
			retMap.put(VectorDatabaseCSVTable.CONTENT, match.get(VectorDatabaseCSVTable.CONTENT));
			retMap.put("Distance", match.get("distance"));
			vectorSearchResults.add(retMap);
		}
		return vectorSearchResults;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		List<Map<String, Object>> filesInMilvus = new ArrayList<>();

		JsonObject queryRequest = new JsonObject();
		queryRequest.addProperty("collectionName", this.collectionName);
		queryRequest.addProperty("limit", 50);
		queryRequest.addProperty("filter", "");

		JsonArray outputFields = new JsonArray();
		outputFields.add("Source");
		queryRequest.add("outputFields", outputFields);

		String url = this.milvusUrl + ENTITIES_ENDPOINT + QUERY_ENDPOINT;
		Map<String, String> headersMap = getHeaders();

		try {
			String response = HttpHelperUtility.postRequestStringBody(url, headersMap, queryRequest.toString(),
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			String indexClass = this.defaultIndexClass;
			if (parameters.containsKey("indexClass")) {
				indexClass = (String) parameters.get("indexClass");
			}

			File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass
					+ DIR_SEPARATOR + DOCUMENTS_FOLDER_NAME);

			List<Map<String, Object>> results = (List<Map<String, Object>>) responseMap.get("data");

			if (results != null && !results.isEmpty()) {
				Set<String> uniqueFileNames = new HashSet<>();

				filesInMilvus.addAll(results.stream().map(record -> (String) record.get("Source"))
						.filter(uniqueFileNames::add).map(fileName -> {
							Map<String, Object> fileInfo = new HashMap<>();
							fileInfo.put("fileName", fileName);

							// Check if file exists
							File file = new File(documentsDir, fileName);
							if (file.exists() && file.isFile()) {
								long fileSizeInBytes = file.length();
								double fileSizeInKB = (double) fileSizeInBytes / 1024;
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								String lastModified = dateFormat.format(new Date(file.lastModified()));

								// Add file metadata
								fileInfo.put("fileSize", fileSizeInKB);
								fileInfo.put("lastModified", lastModified);
							}
							return fileInfo;
						}).collect(Collectors.toList()));
			}

		} catch (Exception e) {
			throw new RuntimeException("Error while fetching documents from Milvus Vector Database: " + e.getMessage(),
					e);
		}
		return filesInMilvus;
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
	//Indexing has not been implemented based on the intended use.Will implement it accordingly
	private void createIndex() {
	    JsonObject createIndexRequest = new JsonObject();
	    createIndexRequest.addProperty("collectionName", this.collectionName);

	    JsonArray indexParams = new JsonArray();
	    JsonObject indexParamObject = new JsonObject();
	    indexParamObject.addProperty("index_type", "AUTOINDEX"); 
	    indexParamObject.addProperty("metricType", "L2"); 
	    indexParamObject.addProperty("fieldName", "");
	    indexParamObject.addProperty("indexName", ""); 

	    indexParams.add(indexParamObject);
	    createIndexRequest.add("indexParams", indexParams);

	    JsonObject response = sendPostRequest(CREATE_INDEX_ENDPOINT, createIndexRequest);

	    if (response == null) {
	        classLogger.error("Failed to create index on field '{}' in collection '{}'", "", this.collectionName);
	        throw new RuntimeException("Failed to create index in Milvus collection: " + this.collectionName);
	    }

	    classLogger.info("Index '{}' created successfully for collection '{}'", "", this.collectionName);
	}
	
	
	/**
	 * Check if the database exists in Milvus.
	 */
	private boolean doesDatabaseExist() {
	    JsonObject request = new JsonObject();
	    request.addProperty("dbName", this.databaseName);
	    JsonObject response = sendPostRequest(DATABASE_LIST_ENDPOINT, request);

	    if (response != null && response.has("data")) {
	        JsonArray databases = response.getAsJsonArray("data");

	        return StreamSupport.stream(databases.spliterator(), false)
	                .map(JsonElement::getAsString)
	                .anyMatch(db -> db.equalsIgnoreCase(this.databaseName));
	    }
	    classLogger.warn("Database check failed or returned empty response.");
	    return false;
	}

	/**
	 * Create the database if it does not exist.
	 */
	private void createDatabase() {
	    JsonObject createRequest = new JsonObject();
	    createRequest.addProperty("dbName", this.databaseName);
	    JsonObject response = sendPostRequest(DATABASE_CREATE_ENDPOINT, createRequest);

	    if (response == null) {
	        classLogger.error("Failed to create database '{}'", this.databaseName);
	        throw new RuntimeException("Failed to create Milvus database: " + this.databaseName);
	    }

	    classLogger.info("Milvus database '{}' created successfully", this.databaseName);
	}

	/**
	 * 
	 * @return
	 */
	private boolean doesCollectionExist() {
		JsonObject createRequest = new JsonObject();
		createRequest.addProperty("dbName", this.databaseName);
		JsonObject responseObject = sendPostRequest(COLLECTION_LIST_ENDPOINT, createRequest);

		if (responseObject != null && responseObject.has("data")) {
			JsonArray collections = responseObject.getAsJsonArray("data");

			return StreamSupport.stream(collections.spliterator(), false).map(JsonElement::getAsString)
					.anyMatch(name -> name.equalsIgnoreCase(this.collectionName));
		}
		 classLogger.warn("Collection check failed or returned empty response.");
		  return false;
	}

	/**
	 * 
	 * @param endpoint
	 * @param requestBody
	 * @return
	 */
	private JsonObject sendPostRequest(String endpoint, JsonObject requestBody) {
		String url = this.milvusUrl + V2_VECTOR_ENDPOINT + endpoint;
		Map<String, String> headers = getHeaders();

		try {
			String response = HttpHelperUtility.postRequestStringBody(url, headers, requestBody.toString(),
					ContentType.APPLICATION_JSON, null, null, null);

			if (response != null && !response.trim().isEmpty()) {
				return JsonParser.parseString(response).getAsJsonObject();
			}
		} catch (Exception e) {
			classLogger.error("Error in request to endpoint {}: {}", endpoint, e.getMessage());
		}
		return null;
	}
   
	private void createCollection() {
		JsonObject createRequest = new JsonObject();
		createRequest.addProperty("dbName", this.databaseName);
		createRequest.addProperty("collectionName", this.collectionName);
		createRequest.addProperty("dimension", 1024);
		createRequest.addProperty("metricType", "COSINE");
		createRequest.addProperty("vectorField", this.embeddings);

		JsonObject response = sendPostRequest(COLLECTION_CREATE_ENDPOINT, createRequest);

		if (response == null) {
			throw new RuntimeException("Failed to create Milvus Vector collection");
		}

		classLogger.info("Milvus Vector collection '{}' created successfully", this.collectionName);
	}
	
	/**
	 * 
	 * @param databaseName
	 * @return
	 */
	public JsonObject describeDatabase(String databaseName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		return sendPostRequest(DATABASE_DISCRIBE_ENDPOINT, request);
	}
	
	/**
	 * 
	 * @param databaseName
	 * @return
	 */
	public JsonObject dropDatabase(String databaseName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		return sendPostRequest(DATABASE_DROP_ENDPOINT, request);
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	public JsonObject describeCollection(String collectionName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		request.addProperty("collectionName", collectionName);
		return sendPostRequest(COLLECTION_DESCRIBE_ENDPOINT, request);
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	public JsonObject getCollectionStats(String collectionName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		request.addProperty("collectionName", collectionName);
		return sendPostRequest(COLLECTION_GET_STATS_ENDPOINT, request);
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	public JsonObject hasCollection(String collectionName) {
		JsonObject request = new JsonObject();
		request.addProperty("collectionName", collectionName);
		return sendPostRequest(COLLECTION_HAS_COLLECTION_ENDPOINT, request);
	}

	/**
	 * 
	 * @param collectionName
	 * @param partitionNames
	 * @return
	 */
	public JsonObject getCollectionLoadState(String collectionName, List<String> partitionNames) {
		JsonObject request = new JsonObject();
		request.addProperty("collectionName", collectionName);
		request.addProperty("dbName", this.databaseName);
		JsonArray partitions = new JsonArray();
		partitionNames.forEach(partitions::add);
		request.add("partitionNames", partitions);

		return sendPostRequest(COLLECTION_GET_LOAD_STATE_ENDPOINT, request);
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	public JsonObject loadCollection(String collectionName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		request.addProperty("collectionName", collectionName);
		return sendPostRequest(COLLECTION_LOAD_ENDPOINT, request);
	}

	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	public JsonObject releaseCollection(String collectionName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		request.addProperty("collectionName", collectionName);
		return sendPostRequest(COLLECTION_RELEASE_ENDPOINT, request);
	}

	/**
	 * 
	 * @param collectionName
	 * @param newCollectionName
	 * @return
	 */
	public JsonObject renameCollection(String collectionName, String newCollectionName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		request.addProperty("collectionName", collectionName);
		request.addProperty("newCollectionName", newCollectionName);
		return sendPostRequest(COLLECTION_RENAME_ENDPOINT, request);
	}
	
	/**
	 * 
	 * @param collectionName
	 * @return
	 */
	public JsonObject dropCollection(String collectionName) {
		JsonObject request = new JsonObject();
		request.addProperty("dbName", this.databaseName);
		request.addProperty("collectionName", collectionName);
		return sendPostRequest(COLLECTION_DROP_ENDPOINT, request);
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

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.MILVUS;
	}
}
