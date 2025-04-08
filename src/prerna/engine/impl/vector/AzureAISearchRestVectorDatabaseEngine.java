package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import net.snowflake.client.jdbc.internal.apache.commons.io.FilenameUtils;
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

public class AzureAISearchRestVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureAISearchRestVectorDatabaseEngine.class);

	public static final String INDEX_NAME = "INDEX_NAME";

	private static final String TEXT_DATATYPE = "Edm.String";
	private static final String INT_DATATYPE = "Edm.Int64";
	private static final String EMBEDDINGS_DATATYPE = "Collection(Edm.Single)";

	private static final String CREATE_INDEX = "indexes";
	private static final String SEARCH_ENDPOINT = "indexes/{{INDEX_NAME}}/docs/search";
	private static final String BULK_ENDPOINT = "indexes/{{INDEX_NAME}}/docs/index";
	private static final String DELETE_BY_QUERY_ENDPOINT = "indexes/{{INDEX_NAME}}/docs/index";
	private static final String DELETE_ENDPOINT_STRING = "indexes/{{INDEX_NAME}}";
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
	private static final String INDEX_NAME_PATTERN="^[a-z0-9](?:[a-z0-9-]{0,126}[a-z0-9])?$";

	private String clusterUrl = null;
	private String apiKey = null;
	private String apiVersion = null;
	//TODO: move this into enum for apiKey/Creds
	private String authorizationMethod = null;

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
		this.apiKey = this.smssProp.getProperty(Constants.API_KEY);
		this.apiVersion = this.smssProp.getProperty(Constants.API_VERSION);
		
		if (this.apiKey != null && !this.apiKey.trim().isEmpty() && this.apiVersion != null && !this.apiVersion.trim().isEmpty()) {
			this.authorizationMethod = "API_KEY";
		}
//		else if(this.username != null && this.password != null && !this.username.trim().isEmpty() && !this.password.trim().isEmpty()) {
//			this.authorizationMethod = "TOKEN";
//		} 
		else {
			classLogger.error("ApiKey is required");
			throw new IllegalArgumentException("ApiKey is required");
		}
		this.indexName = this.smssProp.getProperty(INDEX_NAME);
		if (!this.indexName.matches(INDEX_NAME_PATTERN)) {
			throw new IllegalArgumentException(
					"Index name must only contain lowercase letters, digits or dashes, cannot start or end with dashes and is limited to 128 characters");
		}
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
		
		String distanceMethodInput = this.smssProp.getProperty(DISTANCE_METHOD);
		if(distanceMethodInput != null && !(distanceMethodInput=distanceMethodInput.trim()).isEmpty()) {
			this.distanceMethod = distanceMethodInput;
		}
		String spaceTypeInput = this.smssProp.getProperty(SPACE_TYPE);
		if(spaceTypeInput != null && !(spaceTypeInput=spaceTypeInput.trim()).isEmpty()) {
			this.spaceType = spaceTypeInput;
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
		if (mValueInput != null && !(mValueInput = mValueInput.trim()).isEmpty()) {
			try {
				this.m = ((Number) Double.parseDouble(mValueInput)).intValue();
				if (!(this.m > 3 && this.m <11)) {
					throw new IllegalArgumentException("M_VALUE should be between 4 and 10");
				}
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid string value for m value '" + mValueInput + "'. Must be an integer value");
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		String additionalMappingsStr = this.smssProp.getProperty(ADDITIONAL_MAPPINGS);
		if(additionalMappingsStr != null && !(additionalMappingsStr=additionalMappingsStr.trim()).isEmpty()) {
			this.otherPropsToType = new Gson().fromJson(additionalMappingsStr, new TypeToken<Map<String, String>>() {}.getType());
		}

		// we need to store our stuff
		this.otherPropsToType.put(Constants.AZURE_AI_SEARCH_VECTOR_ID, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.SOURCE, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.MODALITY, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.DIVIDER, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.PART, TEXT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.TOKENS, INT_DATATYPE);
		this.otherPropsToType.put(VectorDatabaseCSVTable.CONTENT, TEXT_DATATYPE);
		this.otherPropsToType.put(this.embeddings, EMBEDDINGS_DATATYPE);

		getIndex(this.indexName, this.embeddings, this.dimension, this.methodName, this.spaceType, this.indexEngine, this.efConstruction, this.m);
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

		JsonObject bulkInsert = new JsonObject();
		{
			JsonArray value = new JsonArray();

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
//				{
//					JsonObject createIndexJson = new JsonObject();
//					JsonObject indexDetails = new JsonObject();
//					indexDetails.addProperty("_index", this.indexName);
//					indexDetails.addProperty("_id", source+"_"+index);
//					createIndexJson.add("index", indexDetails);
//					bulkInsert.add(createIndexJson);
//				}
				// store the actual index details
				{
					JsonObject record = new JsonObject();
					record.addProperty(Constants.AZURE_AI_SEARCH_VECTOR_ID, this.sanitizeKey(source) + "_" + index);
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

//		String valueString = String.join("\n", value.stream().map(x -> x.toString()).collect(Collectors.toList())) + "\n";

		String url = this.clusterUrl + "/" + BULK_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName) + "?" + this.getMustQueryParamString();
		System.out.println("AZURE_AI_SEARCH :: BULK Insert URL >> " + url);
		System.out.println("AZURE_AI_SEARCH :: BULK Insert >> " + bulkInsert);
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(Constants.AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, bulkInsert.toString(), ContentType.APPLICATION_JSON, null, null, null);
		System.out.println("AUZRE_AI_SEARCH :: " + response);
		if(response == null || (response=response.trim()).isEmpty()) {
			throw new IllegalArgumentException("Received no response from azure ai search endpoint");
		}

		Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
		ArrayList<Object> insertions = (ArrayList<Object>) responseMap.get("value");
		classLogger.info("Inserted " + insertions.size() + " bulk inserts (create index + record value) into azure ai search index " + this.indexName);

		Boolean errors = (Boolean) responseMap.get("errors");
		if(errors != null && errors) {
			classLogger.warn("There were errors with some of the bulk insertions in the azure ai search index " + this.indexName);
		}
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
		
		//Logic to retrieve Id's against all file names
		JsonObject getIdRq = new JsonObject();
		String searchArr= new String();
		
		//Get the file name and join it with comma and add to string ex: file1.pdf , file2.pdf , file3.pdf
		searchArr=String.join(",",sourceNames);
		System.out.println("Search Arr "+searchArr);
		getIdRq.addProperty("select", "id,Source");
		getIdRq.addProperty("search", searchArr);
		getIdRq.addProperty("searchFields","Source" );
		getIdRq.addProperty("count",true);
		
		classLogger.info("Retriving ids against file name :: Request :: "+getIdRq);
		
		String url = this.clusterUrl + "/" + SEARCH_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName)+ "?" + this.getMustQueryParamString();
		
		
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(Constants.AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		
		String responseSearchId = HttpHelperUtility.postRequestStringBody(url, headersMap, getIdRq.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJsonSearchId = JsonParser.parseString(responseSearchId).getAsJsonObject();
		JsonArray sourceArrId = responseJsonSearchId.getAsJsonObject().getAsJsonArray("value");
		classLogger.info("Response source ids :: "+sourceArrId);		
		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;
		//Delete Rq
		JsonArray valueArr = new JsonArray();
				
		//loop over this
		for(JsonElement el : sourceArrId) {
			String source = el.getAsJsonObject().get("Source").getAsString();
			classLogger.info("Response :: Source ::  "+source +" fileNames in Para "+sourceNames);
			if(sourceNames.contains(source)) {
				String fId=el.getAsJsonObject().get("id").getAsString();
				JsonObject sourceRq = new JsonObject();
				sourceRq.addProperty("@search.action","delete");
				sourceRq.addProperty("id", fId);
				valueArr.add(sourceRq);	
			}
		}
		
		classLogger.info("Request Object for Deleting ::isEmpty Value   "+valueArr.isEmpty());
		
		if(!valueArr.isEmpty()) {
			JsonObject delRq = new JsonObject();
			delRq.add("value",valueArr);
			System.out.println("value of final delete request"+delRq);
			String urlDel = this.clusterUrl + "/" + DELETE_BY_QUERY_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName)+ "?" + this.getMustQueryParamString();
			headersMap.put(Constants.AZURE_AI_API_KEY, this.apiKey);
			headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

			String response = HttpHelperUtility.postRequestStringBody(urlDel, headersMap, delRq.toString(), ContentType.APPLICATION_JSON, null, null, null);
			JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
			classLogger.info("For " + SmssUtilities.getUniqueName(this.engineName, this.engineId) + " removed " +" docs for files = " + fileNames);
			JsonArray responseArr = responseJson.get("value").getAsJsonArray();
			JsonArray errors = new JsonArray();
			for(JsonElement el : responseArr) {
				JsonObject respObj = el.getAsJsonObject();
				
				if(!respObj.get("errorMessage").isJsonNull()) {
					errors.add(respObj.get("errorMessage").getAsString());
				}
			}
			
			classLogger.info("Errors Array :: isEmpty "+errors);
			
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
//					vectorQuery.addProperty("exhaustive", false);
				}
				vectorQueries.add(vectorQuery);
			}
			search.add("vectorQueries", vectorQueries);
		}

		String url = this.clusterUrl + "/" + SEARCH_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName) + "?" + this.getMustQueryParamString();
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(Constants.AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");

		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(), ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray hits = getHitsFromSearch(responseJson);
		
		List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
		for(JsonElement e : hits) {
			Map<String, Object> thisMatch = new HashMap<>();
			vectorSearchResults.add(thisMatch);

			JsonObject hitJson = e.getAsJsonObject();
			Double score = (Double) hitJson.get("@search.score").getAsDouble();
			thisMatch.put("Score", score);
			
//			JsonObject sourceDetails = hitJson.get("_source").getAsJsonObject();
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
		String url = this.clusterUrl + "/" + SEARCH_ENDPOINT.replace("{{INDEX_NAME}}", this.indexName)
				+ "?" + this.getMustQueryParamString();
 
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(Constants.AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
 
		JsonObject search = new JsonObject();
		{
			JsonArray facets = new JsonArray();
			facets.add("Source");
			search.addProperty("search", "*");
			search.addProperty("select", "Source");
			search.add("facets", facets);
		}
		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, search.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		JsonArray sourceArr = responseJson.getAsJsonObject("@search.facets").getAsJsonArray("Source");
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
 
		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR
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
		headersMap.put(Constants.AZURE_AI_API_KEY, this.apiKey);
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
		System.out.println("AZURE_AI_SEARCH :: Checking does index exists.");
		String url = this.clusterUrl + "/" + DOES_INDEX_EXISTS.replace("{{INDEX_NAME}}", specificIndexName) + "?" + this.getMustQueryParamString() ;
		System.out.println("AZURE_AI_SEARCH :: " + url);
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(Constants.AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		try {
			HttpHelperUtility.getRequest(url, headersMap, null, null, null);
			System.out.println("AZURE_AI_SEARCH :: Index already exists...");
			return true;
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
	private void createIndex(String specificIndexName, String embeddings, int dimension, String methodName, String spaceType, String engine, int efConstruction, int m) {
		System.out.println("AZURE_AI_SEARCH :: Creating new Index...");
		JsonObject createIndexJson = new JsonObject();
		{
			createIndexJson.addProperty("name", specificIndexName);
			JsonArray fields = new JsonArray();
			{
				for(String propName : this.otherPropsToType.keySet()) {
					String propType = this.otherPropsToType.get(propName);

					JsonObject field = new JsonObject();
					field.addProperty("name", propName);
					field.addProperty("type", propType);
					if (propType.equals(TEXT_DATATYPE)) {
						field.addProperty("searchable", true);
					}
					if (propName.equals(Constants.AZURE_AI_SEARCH_VECTOR_ID)) {
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
			//add to parent
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
			//add to parent
			createIndexJson.add("vectorSearch", vectorSearch);
		}

		String url = this.clusterUrl + "/" + CREATE_INDEX + "?" + this.getMustQueryParamString();
		System.out.println("AZURE_AI_SEARCH URL :: " + url);
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(Constants.AZURE_AI_API_KEY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, createIndexJson.toString(), ContentType.APPLICATION_JSON, null, null, null);
		System.out.println("AZURE_AI_search :: " + response);
		if(!parseResponseForAcknowledged(response, specificIndexName)) {
			throw new IllegalArgumentException("Did not receive an acknowledgement from the server for creating the index with the embeddings column");
		}
	}

	/**
	 * 
	 * @param response
	 * @return
	 */
	private boolean parseResponseForAcknowledged(String response, String specificIndexName) {
		if(response == null || (response=response.trim()).isEmpty()) {
			return false;
		}

		Map<String, Object> responseMap = new Gson().fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
		String createdIndexName = (String) responseMap.get("name");
		if(createdIndexName.equals(specificIndexName)) {
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

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.AZURE_AI_SEARCH;
	}
	
	public String getMustQueryParamString () {
		return Constants.AZURE_API_VERSION + "=" + this.apiVersion;
	}

	public String sanitizeKey(String key) {
        // Regular expression to match allowed characters
        String regex = "[^a-zA-Z0-9_\\-=]";
        // Replace all characters not matching the regex with an underscore
        return key.replaceAll(regex, "_");
    }

}