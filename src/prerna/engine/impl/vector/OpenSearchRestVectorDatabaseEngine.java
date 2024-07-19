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
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
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
	private static final String ADDITIONAL_MAPPINGS = "ADDITIONAL_MAPPINGS";

	private String clusterUrl = null;
	private String username = null;
	private String password = null;

	private String indexName = null;
	
	private String embeddings = "embeddings";
	private int dimension = 1024;
	private String methodName = "hnsw";
	private String spaceType = "l2";
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
		
		this.indexName = this.smssProp.getProperty(INDEX_NAME);
		String customEmbeddingsName = this.smssProp.getProperty(EMBEDDINGS_COLUMN);
		if(customEmbeddingsName != null && !(customEmbeddingsName=customEmbeddingsName.trim()).isEmpty()) {
			this.embeddings = customEmbeddingsName;
		}
		String additionalMappingsStr = this.smssProp.getProperty(ADDITIONAL_MAPPINGS);
		if(additionalMappingsStr != null && !(additionalMappingsStr=additionalMappingsStr.trim()).isEmpty()) {
			this.otherPropsToType = new Gson().fromJson(additionalMappingsStr, new TypeToken<Map<String, String>>() {}.getType());
		}
		
		// we need to store our stuff
		this.otherPropsToType.put(VectorDatabaseCSVTable.SOURCE, KEYWORD_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.MODALITY, KEYWORD_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.DIVIDER, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.PART, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.TOKENS, INT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.CONTENT, TEXT_DATATYPE);

		// TODO: all inputs to be parameterized
		getIndex(this.indexName, this.embeddings, this.dimension, this.methodName, this.spaceType, this.indexEngine, this.efConstruction, this.m);
		updateIndexMapping(this.indexName, this.otherPropsToType);		
	}

	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception {
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
		
		Map<String, Integer> sourceId = new HashMap<>();
		for (VectorDatabaseCSVRow row: vectorCsvTable.getRows()) {
			String source = row.getSource();
			int index = 0;
			if(sourceId.containsKey(source)) {
				index = sourceId.get(source);
				sourceId.put(source, index+1);
			} else {
				sourceId.put(source, new Integer(0));
			}
			
			// store creation of the index
			{
				JsonObject createIndexJson = new JsonObject();
				JsonObject indexDetails = new JsonObject();
				indexDetails.addProperty("_index", this.indexName);
				indexDetails.addProperty("_id", source+"_"+index);
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
		
		String bulkRequest = String.join("\n", bulkInsert.stream().map(x -> x.toString()).collect(Collectors.toList())) + "\n";
		
		String url = this.clusterUrl + "/" + this.indexName + BULK_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, bulkRequest, ContentType.APPLICATION_JSON, null, null, null);
		if(response == null || (response=response.trim()).isEmpty()) {
			throw new IllegalArgumentException("Received no response from open search endpoint");
		}
		
		Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
        Number insertions = (Number) responseMap.get("took");
        classLogger.info("Inserted " + insertions.intValue() + " bulk inserts (create index + record value) into open search index " + this.indexName);
        
        Boolean errors = (Boolean) responseMap.get("errors");
        if(errors) {
        	classLogger.warn("There were errors with some of the bulk insertions in the open search index " + this.indexName);
        }
	}
	
	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;

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
		
        String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(), ContentType.APPLICATION_JSON, null, null, null);
        Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
        classLogger.info("For " + SmssUtilities.getUniqueName(this.engineName, this.engineId) + " removed " + responseMap.get("deleted") + " docs for files = " + fileNames);
        List<Object> errors = (List<Object>) responseMap.get("failures");
        if(errors != null && !errors.isEmpty()) {
        	classLogger.warn("For " + SmssUtilities.getUniqueName(this.engineName, this.engineId) + " errors = '" + errors + "' when attempting to delete files = " + fileNames);
        }

		// using the search result for the source, we need to delete all the ids we found
		List<String> filesToRemoveFromCloud = new ArrayList<String>();
		for (String document : fileNames) {
			String documentName = Paths.get(document).getFileName().toString();
			// remove the physical documents
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
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
	}

	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		if (!this.modelPropsLoaded) {
			verifyModelProps();
		}
		
		IModelEngine engine = Utility.getModel(this.embedderEngineId);
		EmbeddingsModelEngineResponse embeddingsResponse = engine.embeddings(Arrays.asList(new String[] {searchStatement}), insight, null);

		// construct search query
		JsonObject search = new JsonObject();
		search.addProperty("size", limit);
		{
			JsonObject query = new JsonObject();
			{
				JsonObject knn = new JsonObject();
				{
					JsonObject embedding = new JsonObject();
					embedding.add("vector", convertListNumToJsonArray(embeddingsResponse.getResponse().get(0)));
					embedding.addProperty("k", limit);
					// store key using the field name for the vector in parent
					knn.add(this.embeddings, embedding);
				}
				// add to parent
				query.add("knn", knn);
			}
			// add to parent
			search.add("query", query);
		}
		
		if (parameters.containsKey("filters")) {
			
		}
		
		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		
        String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(), ContentType.APPLICATION_JSON, null, null, null);
        Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());

        List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
        List<Map<String, Object>> hits = (List<Map<String, Object>>) ((Map<String, Object>)responseMap.get("hits")).get("hits");
        for(Map<String, Object> match : hits) {
        	Double score = (Double) match.get("_score");
        	Map<String, Object> sourceMap = (Map<String, Object>) match.get("_source");
        	
        	Map<String, Object> retMap = new HashMap<>();
        	retMap.put(VectorDatabaseCSVTable.SOURCE, sourceMap.get(VectorDatabaseCSVTable.SOURCE));
        	retMap.put(VectorDatabaseCSVTable.MODALITY, sourceMap.get(VectorDatabaseCSVTable.MODALITY));
        	retMap.put(VectorDatabaseCSVTable.DIVIDER, sourceMap.get(VectorDatabaseCSVTable.DIVIDER));
        	retMap.put(VectorDatabaseCSVTable.PART, sourceMap.get(VectorDatabaseCSVTable.PART));
        	retMap.put(VectorDatabaseCSVTable.TOKENS, sourceMap.get(VectorDatabaseCSVTable.TOKENS));
        	retMap.put(VectorDatabaseCSVTable.CONTENT, sourceMap.get(VectorDatabaseCSVTable.CONTENT));
        	retMap.put("Score", score);
        	vectorSearchResults.add(retMap);
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
		
		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT;// + "?search_type=count";
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		
        String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(), ContentType.APPLICATION_JSON, null, null, null);
        Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
        Map<String, Object> aggregations = (Map<String, Object>) responseMap.get("aggregations");
        Map<String, Object> uScores = (Map<String, Object>) aggregations.get(UNIQUE_SOURCES);
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) uScores.get("buckets");
        
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		
		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + DOCUMENTS_FOLDER_NAME);

		List<Map<String, Object>> filesInOpenSearch = new ArrayList<>();
		
		for (Map<String, Object> bucketDetails : buckets) {
			Map<String, Object> fileInfo = new HashMap<>();
			String fileName = (String) bucketDetails.get("key");
			fileInfo.put("fileName", fileName);
			
			File thisF = new File(documentsDir, fileName);
			if(thisF.exists() && thisF.isFile()) {
				long fileSizeInBytes = thisF.length();
				double fileSizeInMB = (double) fileSizeInBytes / (1024);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String lastModified = dateFormat.format(new Date(thisF.lastModified()));

				// add file size and last modified into the map
				fileInfo.put("fileSize", fileSizeInMB);
				fileInfo.put("lastModified", lastModified);
			}
			
			filesInOpenSearch.add(fileInfo);
		}
		
		return filesInOpenSearch;
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
	private void getIndex(String specificIndexName, String embeddings, int dimension, String methodName, String spaceType, String engine, int efConstruction, int m) {
		Boolean exisits = doesIndexExsist(specificIndexName);
		if(!exisits) {
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
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		try {
			HttpHelperUtility.headRequest(url, headersMap, null, null, null);
			return true;
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return false;
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
	private void createIndex(String specificIndexName, String embeddings, int dimension, String methodName, String spaceType, String engine, int efConstruction, int m) {
		JsonObject createIndexJson = new JsonObject();
		{
			JsonObject settings = new JsonObject();
			{
				JsonObject index = new JsonObject();
				index.addProperty("knn", true);
				// add to parent
				settings.add("index", index);
			}
			//add to parent
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
			//add to parent
			createIndexJson.add("mappings", mappings);
		}
		
        String url = this.clusterUrl + "/" + specificIndexName;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        String response = HttpHelperUtility.putRequestStringBody(url, headersMap, createIndexJson.toString(), ContentType.APPLICATION_JSON, null, null, null);
        if(!parseResponseForAcknowledged(response)) {
        	throw new IllegalArgumentException("Did not receive an acknowledgement from the server for creating the index with the embeddings column");
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
			for(String propName : propNameToType.keySet()) {
				String propType = propNameToType.get(propName);
				
				JsonObject type = new JsonObject();
				type.addProperty("type", propType);
				properties.add(propName, type);
			}
			//add to parent
			updateProperties.add("properties", properties);
		}
		
        String url = this.clusterUrl + "/" + this.indexName + UPDATE_MAPPINGS_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		String response = HttpHelperUtility.putRequestStringBody(url, headersMap, updateProperties.toString(), ContentType.APPLICATION_JSON, null, null, null);
        if(!parseResponseForAcknowledged(response)) {
        	throw new IllegalArgumentException("Did not receive an acknowledgement from the server for updating the mappings");
        }
	}
	
	/**
	 * 
	 * @param response
	 * @return
	 */
	private boolean parseResponseForAcknowledged(String response) {
		if(response == null || (response=response.trim()).isEmpty()) {
			return false;
		}
		
		Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
        Boolean valid = (Boolean) responseMap.get("acknowledged");
        if(valid != null && valid) {
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
		for(int i = 0; i < row.size(); i++) {
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
		for(int i = 0; i < row.size(); i++) {
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