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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;

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
	
	private static final String UNIQUE_SOURCES = "unique_sources";

	private static final String EMBEDDINGS_COLUMN = "EMBEDDINGS_COLUMN";
	private static final String DIMENSION_SIZE = "DIMENSION_SIZE";
	private static final String METHOD_NAME = "METHOD_NAME";
	private static final String INDEX_ENGINE = "INDEX_ENGINE";
	private static final String EF_CONSTRUCTION = "EF_CONSTRUCTION";
	private static final String M_VALUE = "M_VALUE";
	private static final String ADDITIONAL_MAPPINGS = "ADDITIONAL_MAPPINGS";

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
	
	protected String customTemplateQuery = null;
	protected String customResultsPath = null;
	protected List<String> highlightFieldKeys = null;

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
		String dimensionInput = this.smssProp.getProperty(DIMENSION_SIZE);
		if(dimensionInput != null && !(dimensionInput=dimensionInput.trim()).isEmpty()) {
			try {
				this.dimension = ((Number) Double.parseDouble(dimensionInput)).intValue();
			} catch(NumberFormatException e) {
				classLogger.warn("Invalid string value for dimension '"+dimensionInput+"'. Must be an integer value");
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		String methodNameInput = this.smssProp.getProperty(METHOD_NAME);
		if(methodNameInput != null && !(methodNameInput=methodNameInput.trim()).isEmpty()) {
			this.methodName = methodNameInput;
		}
		String indexEngineInput = this.smssProp.getProperty(INDEX_ENGINE);
		if(indexEngineInput != null && !(indexEngineInput=indexEngineInput.trim()).isEmpty()) {
			this.indexEngine = indexEngineInput;
		}
		String efConstructionInput = this.smssProp.getProperty(EF_CONSTRUCTION);
		if(efConstructionInput != null && !(efConstructionInput=efConstructionInput.trim()).isEmpty()) {
			try {
				this.efConstruction = ((Number) Double.parseDouble(efConstructionInput)).intValue();
			} catch(NumberFormatException e) {
				classLogger.warn("Invalid string value for ef construction '"+efConstructionInput+"'. Must be an integer value");
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		String mValueInput = this.smssProp.getProperty(M_VALUE);
		if(mValueInput != null && !(mValueInput=mValueInput.trim()).isEmpty()) {
			try {
				this.m = ((Number) Double.parseDouble(mValueInput)).intValue();
			} catch(NumberFormatException e) {
				classLogger.warn("Invalid string value for m value '"+mValueInput+"'. Must be an integer value");
				classLogger.error(Constants.STACKTRACE, e);
			}
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

		getIndex(this.indexName, this.embeddings, this.dimension, this.methodName, this.distanceMethod, this.indexEngine, this.efConstruction, this.m);
		updateIndexMapping(this.indexName, this.otherPropsToType);	
		
//		try {
//			this.customTemplateQuery = JsonParser.parseString(this.smssProp.getProperty(Constants.CUSTOM_TEMPLATE_QUERY)).getAsJsonObject();
//		} catch (NullPointerException e) {
//			classLogger.warn("No json template found");
//		}
		this.customTemplateQuery = this.smssProp.getProperty(Constants.CUSTOM_TEMPLATE_QUERY);
		this.customResultsPath = this.smssProp.getProperty(Constants.CUSTOM_RESULTS_PATH);
	}
	
	@Override
	protected String getDefaultDistanceMethod() {
		return "cosinesimil";
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception {
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
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred creating the embeddings for the generated chunks. Detailed error message = " + e.getMessage());
		}

		List<JsonObject> bulkInsert = new ArrayList<>();
		Map<String, Integer> fileRecordCountMap = new HashMap<>();
		Set<String> fileNamesSet = new HashSet<>();
		Map<String, Integer> sourceId = new HashMap<>();
		for (VectorDatabaseCSVRow row: vectorCsvTable.getRows()) {
			String source = row.getSource();
			fileRecordCountMap.put(source, fileRecordCountMap.getOrDefault(source, 0) + 1);
			int index = 0;
			if(sourceId.containsKey(source)) {
				index = sourceId.get(source);
				sourceId.put(source, index++);
			} else {
				sourceId.put(source, 0);
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
	        }
	        else if (inserted == 0) {
	        	status = "FAILED";
	        }
	        else {
	        	status = "PARTIAL";
	        }

	        fileStatusList.add(new FileEmbeddingStatus(fileName, status, inserted, failed, total));
	    }
	    
	    Boolean errors = (Boolean) responseMap.get("errors");
		if(errors) {
			classLogger.warn("There were errors with some of the bulk insertions in the open search index " + this.indexName);
		} else {
			classLogger.info("All records inserted successfully into OpenSearchRest index '{}'", this.indexName);
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
		for(String document : fileNames) {
			String documentName = FilenameUtils.getName(document);
			File f = new File(document);
			if(f.exists() && f.getName().endsWith(".csv")) {
				sourceNames.addAll(VectorDatabaseCSVTable.pullSourceColumn(f));
			} else {
				sourceNames.add(documentName);
			}
		}

		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;

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
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		classLogger.info("For " + SmssUtilities.getUniqueName(this.engineName, this.engineId) + " removed " + responseJson.get("deleted") + " docs for files = " + fileNames);
		JsonArray errors = responseJson.get("failures").getAsJsonArray();
		if(errors != null && !errors.isEmpty()) {
			classLogger.warn("For " + SmssUtilities.getUniqueName(this.engineName, this.engineId) + " errors = '" + errors + "' when attempting to delete files = " + fileNames);
		}

		// using the search result for the source, we need to delete all the ids we found
		List<String> filesToRemoveFromCloud = new ArrayList<String>();
		for (String document : sourceNames) {
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
	
	public List<Map<String, Object>> customNearestNeighborSearch(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters, boolean meta) {
		
		/**
		 * Flow:
		 * 1. From SMSS, check custom json template or not
		 * 1a. If not, pass this on to the original nearest neighbor call function.
		 * 1b. If so, use the custom template and string substitute the search phrase into the json template
		 * 2. Call <index>/_search
		 * 3. From SMSS, check to see if there's a custom results path or not
		 * 3a. If not, get via parent hits.hits
		 * 3b. If so, get via the custom results path
		 * 4. Are we asking for metadata through the meta param?
		 * 4a. If not, done
		 * 4b. If so, iterate over hits.hits._source and create map for each doc, then done
		 */
		
		/**
		 * 1
		 */
		JsonObject search = null;
		if (this.customTemplateQuery != null) {
			classLogger.info("Found a custom template query for this engine");
			this.customTemplateQuery = this.customTemplateQuery.replaceAll("%QUERY_PLACEHOLDER%", searchStatement);
			try {
				search = JsonParser.parseString(this.customTemplateQuery).getAsJsonObject();
				// From the custom query template, get the metadata field keys under highlight > fields
				if (highlightFieldKeys == null) {
					highlightFieldKeys = new ArrayList<>();
					JsonObject highlightFields = search.getAsJsonObject("highlight").getAsJsonObject("fields");
					for (Map.Entry<String, JsonElement> entry : highlightFields.entrySet()) {
						String fieldName = entry.getKey();
						highlightFieldKeys.add(fieldName);
					}
				}
				classLogger.info(highlightFieldKeys);
			} catch (Exception e) {
				classLogger.error(e);
				throw e;
			}
		} else {
			search = getNearestNeighborSearchJson(insight, searchStatement, limit, parameters);
		}
		
		classLogger.info(search);
		
		/**
		 * 2
		 */
		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(), ContentType.APPLICATION_JSON, null, null, null);
		
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		
		JsonArray hits = getHitsFromSearch(responseJson);
		
		classLogger.info(hits);
		
		List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
		
		/**
		 * 3
		 */

		for(JsonElement e : hits) {
			JsonObject hitJson = e.getAsJsonObject();
			Double score = (Double) hitJson.get("_score").getAsDouble();
			
			// If we have custom results path, use it to get sourceDetails
			if (this.customResultsPath != null) {
				classLogger.info("Using custom results path: " + this.customResultsPath);
				try {
					Configuration configuration = Configuration.builder()
						.jsonProvider(new GsonJsonProvider())
						.build();
					DocumentContext jsonContext = JsonPath.using(configuration).parse(hitJson);
					JsonArray sourceDetailsArray = jsonContext.read(this.customResultsPath);
					
					// Create a separate match for each element in sourceDetailsArray
					for (JsonElement sd : sourceDetailsArray) {
						Map<String, Object> thisMatch = new HashMap<>();
						vectorSearchResults.add(thisMatch);
						
						// Add base information to each match
						thisMatch.put("Score", score);
						
						JsonObject sourceDetails = sd.getAsJsonObject();
						thisMatch.put(VectorDatabaseCSVTable.SOURCE, sourceDetails.get(VectorDatabaseCSVTable.SOURCE).getAsString());
						thisMatch.put(VectorDatabaseCSVTable.MODALITY, sourceDetails.get(VectorDatabaseCSVTable.MODALITY).getAsString());
						thisMatch.put(VectorDatabaseCSVTable.DIVIDER, sourceDetails.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
						thisMatch.put(VectorDatabaseCSVTable.PART, sourceDetails.get(VectorDatabaseCSVTable.PART).getAsString());
						thisMatch.put(VectorDatabaseCSVTable.TOKENS, sourceDetails.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
						
						/**
						 * 4 - Add metadata if requested
						 */
						if (meta) {
							addMetadataToMatch(thisMatch, hitJson);
						}
					}
				} catch (Exception e1) {
					throw new RuntimeException("Failed to parse results using custom path: " + this.customResultsPath, e1);
				}
			} else {
				// Standard path - single match per hit
				Map<String, Object> thisMatch = new HashMap<>();
				vectorSearchResults.add(thisMatch);
				
				// Add base information
				thisMatch.put("Score", score);
				
				JsonObject sourceDetails = hitJson.get("_source").getAsJsonObject();
				thisMatch.put(VectorDatabaseCSVTable.CONTENT, sourceDetails.get(VectorDatabaseCSVTable.CONTENT).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.SOURCE, sourceDetails.get(VectorDatabaseCSVTable.SOURCE).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.MODALITY, sourceDetails.get(VectorDatabaseCSVTable.MODALITY).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.DIVIDER, sourceDetails.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.PART, sourceDetails.get(VectorDatabaseCSVTable.PART).getAsString());
				thisMatch.put(VectorDatabaseCSVTable.TOKENS, sourceDetails.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
			}
		}

		return vectorSearchResults;
	}
	
	private void addMetadataToMatch(Map<String, Object> thisMatch, JsonObject hitJson) {
		if (highlightFieldKeys != null && !highlightFieldKeys.isEmpty()) {
			JsonObject sourceDetails = hitJson.get("_source").getAsJsonObject();
			for (String fieldKey : highlightFieldKeys) {
				try {
					JsonElement fieldElement = sourceDetails.get(fieldKey);
					if (fieldElement != null && !fieldElement.isJsonNull()) {
						if (fieldElement.isJsonArray()) {
							thisMatch.put(fieldKey, fieldElement.getAsJsonArray().toString());
						} else if (fieldElement.isJsonPrimitive()) {
							thisMatch.put(fieldKey, fieldElement.getAsString());
						} else if (fieldElement.isJsonObject()) {
							thisMatch.put(fieldKey, fieldElement.getAsJsonObject().toString());
						} else {
							thisMatch.put(fieldKey, fieldElement.toString());
						}
					} else {
						thisMatch.put(fieldKey, null);
					}
				} catch (Exception e) {
					classLogger.warn("Failed to extract metadata field '{}' from hit: {}", fieldKey, e.getMessage());
					thisMatch.put(fieldKey, null);
				}
			}
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
		
		JsonObject search = getNearestNeighborSearchJson(insight, searchStatement, limit, parameters);
		
		classLogger.debug("OPENSEARCH FINAL SEARCH QUERY : " + search.toString());

		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(), ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		
		JsonArray hits = getHitsFromSearch(responseJson);
		
		List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
		for(JsonElement e : hits) {
			Map<String, Object> thisMatch = new HashMap<>();
			vectorSearchResults.add(thisMatch);

			JsonObject hitJson = e.getAsJsonObject();
			Double score = (Double) hitJson.get("_score").getAsDouble();
			thisMatch.put("Score", score);
			
			JsonObject sourceDetails = hitJson.get("_source").getAsJsonObject();
			thisMatch.put(VectorDatabaseCSVTable.SOURCE, sourceDetails.get(VectorDatabaseCSVTable.SOURCE).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.MODALITY, sourceDetails.get(VectorDatabaseCSVTable.MODALITY).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.DIVIDER, sourceDetails.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.PART, sourceDetails.get(VectorDatabaseCSVTable.PART).getAsString());
			thisMatch.put(VectorDatabaseCSVTable.TOKENS, sourceDetails.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
			thisMatch.put(VectorDatabaseCSVTable.CONTENT, sourceDetails.get(VectorDatabaseCSVTable.CONTENT).getAsString());
		}
		return vectorSearchResults;
	}

	
	
	protected JsonObject getNearestNeighborSearchJson(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		IModelEngine engine = Utility.getModel(this.embedderEngineId);
		EmbeddingsModelEngineResponse embeddingsResponse = engine.embeddings(Arrays.asList(new String[] {searchStatement}), insight, null);
		List<Double> searchStatementEmbedding = embeddingsResponse.getResponse().get(0);
		
		JsonObject search = new JsonObject();
		search.addProperty("size", limit);
		{
			JsonObject query = new JsonObject();
			{
				JsonObject knn = new JsonObject();
				{
					JsonObject embedding = new JsonObject();
					embedding.add("vector", convertListNumToJsonArray(searchStatementEmbedding));
					embedding.addProperty("k", limit);
					knn.add(this.embeddings, embedding);
				}
				
				if (!parameters.containsKey("filters")) {
					query.add("knn", knn);
				} else {
					JsonObject bool = new JsonObject();
					{
						JsonArray must = new JsonArray();
						{
							JsonObject knnParent = new JsonObject();
							knnParent.add("knn", knn);
							must.add(knnParent);
						}
						bool.add("must", must);
						
						JsonObject filter = getFilterAggregation(parameters);
						bool.add("filter", filter);
					}
					query.add("bool", bool);
				}
				search.add("query", query);
			}
		}
		return search;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		
		JsonObject search = getListDocumentSearchJson(parameters);

		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT;// + "?search_type=count";
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(), ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray bucketsArr = responseJson.getAsJsonObject("aggregations").getAsJsonObject(UNIQUE_SOURCES).getAsJsonArray("buckets");
		
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + DOCUMENTS_FOLDER_NAME);

		List<Map<String, Object>> returnSources = new ArrayList<>();
		for (JsonElement bucket : bucketsArr) {
			JsonObject bucketDetails = bucket.getAsJsonObject();
			Map<String, Object> fileInfo = new HashMap<>();
			String fileName = bucketDetails.get("key").getAsString();
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

			returnSources.add(fileInfo);
		}

		return returnSources;
	}

	protected JsonObject getListDocumentSearchJson(Map<String, Object> parameters) {
		JsonObject search = new JsonObject();
		search.addProperty("size", 0);
		{
			if (parameters.containsKey("filters")) {
				JsonObject filter = getFilterAggregation(parameters);
				search.add("query", filter);
			}
			
			JsonObject aggs = new JsonObject();
			{
				JsonObject uniqueSources = new JsonObject();
				{
					JsonObject terms = new JsonObject();
					terms.addProperty("field", VectorDatabaseCSVTable.SOURCE);
					terms.addProperty("min_doc_count", 1);
					// Pull upto 9999 unique terms for the aggregation
					terms.addProperty("size", 9999);
					uniqueSources.add("terms", terms);
				}
				aggs.add(UNIQUE_SOURCES, uniqueSources);
			}
			search.add("aggs", aggs);
		}
		return search;
	}

	@SuppressWarnings("unchecked")
	protected JsonObject getFilterAggregation(Map<String, Object> parameters) {
		JsonObject filterParent = new JsonObject();
		{
			JsonObject filterBool = new JsonObject();
			{
				//Filtration logic starts here
				//filter contains simple or AND conditions
				JsonArray filter = new JsonArray();

				//should contains OR condition filters
				JsonArray should = new JsonArray();

				//must not contains not equals to filters
				JsonArray must_not = new JsonArray();

				List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove("filters");
				for(IQueryFilter queryFilter : filters) {
					RestVectorQueryFilterTranslationHelper.processFilter(queryFilter, filter, should, must_not);
				}

				//call to process filter
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
  
  public List<Map<String, Object>> listAllRecords() {
		return listAllRecords(null);
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		JsonObject search = getListAllRecordsSearchJson(parameters);

		String url = this.clusterUrl + "/" + this.indexName + SEARCH_ENDPOINT + "?size=10000";
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(), ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray hits = getHitsFromSearch(responseJson);
		
		List<Map<String, Object>> allDocuments = new ArrayList<>();
		for(JsonElement e : hits) {
			Map<String, Object> thisDocument = new HashMap<>();
			allDocuments.add(thisDocument);

			JsonObject fields = e.getAsJsonObject().get("fields").getAsJsonObject();
			thisDocument.put(VectorDatabaseCSVTable.SOURCE, fields.get(VectorDatabaseCSVTable.SOURCE).getAsString());
			thisDocument.put(VectorDatabaseCSVTable.MODALITY, fields.get(VectorDatabaseCSVTable.MODALITY).getAsString());
			thisDocument.put(VectorDatabaseCSVTable.DIVIDER, fields.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
			thisDocument.put(VectorDatabaseCSVTable.PART, fields.get(VectorDatabaseCSVTable.PART).getAsString());
			thisDocument.put(VectorDatabaseCSVTable.TOKENS, fields.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
			thisDocument.put(VectorDatabaseCSVTable.CONTENT, fields.get(VectorDatabaseCSVTable.CONTENT).getAsString());
		}
		
		return allDocuments;
	}

	protected JsonObject getListAllRecordsSearchJson(Map<String, Object> parameters) {
		JsonObject search = new JsonObject();
		{
			if (parameters.containsKey("filters")) {
				JsonObject filter = getFilterAggregation(parameters);
				search.add("query", filter);
			}
			
			JsonArray fields = new JsonArray();
			{	
				fields.add(VectorDatabaseCSVTable.SOURCE);
				fields.add(VectorDatabaseCSVTable.MODALITY);
				fields.add(VectorDatabaseCSVTable.DIVIDER);
				fields.add(VectorDatabaseCSVTable.PART);
				fields.add(VectorDatabaseCSVTable.TOKENS);
				fields.add(VectorDatabaseCSVTable.CONTENT);
			}
			search.add("fields", fields);
			search.addProperty("_source", false);
		}
		return search;
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
		Boolean exisits = doesIndexExist(specificIndexName);
		if(!exisits) {
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
	            	return true;   // Exists
	            case 404: 
	            	classLogger.info("Recieved 404, indicating that index does not exist.");
	            	return false;  // Not exist
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
