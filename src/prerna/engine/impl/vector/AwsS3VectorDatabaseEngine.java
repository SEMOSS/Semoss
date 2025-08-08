package prerna.engine.impl.vector;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class AwsS3VectorDatabaseEngine extends AbstractVectorDatabaseEngine {
	private static final Logger classLogger = LogManager.getLogger(AwsS3VectorDatabaseEngine.class);
	public static final String VECTOR_BUCKET_NAME = "VECTOR_BUCKET_NAME";
	public static final String VECTOR_TYPE = "VECTOR_TYPE";
	public static final String NON_FILTERABLE_KEYS = "NON_FILTERABLE_KEYS";
	private static final String INDEX_NAME = "INDEX_NAME";
	private static final String DATA_TYPE = "DATA_TYPE";
	private static final String DIMENSION = "DIMENSION";
	private static final String DISTANCE_METRIC = "DISTANCE_METRIC";
	private static final String INDEX_NAME_PATTERN="^[a-z0-9](?:[a-z0-9-]{0,126}[a-z0-9])?$";
	private static final String API_KEY = "API_KEY";
	private static final String SECRET_KEY = "SECRET_KEY";
	private static final String CREATE_INDEX_ENDPOINT = "/CreateIndex";
	private static final String GET_INDEX_ENDPOINT = "/GetIndex";
	private static final String PUT_VECTORS_ENDPOINT = "/PutVectors";
	private static final String LIST_VECTORS_ENDPOINT = "/ListVectors";
	private static final String DELETE_VECTORS_ENDPOINT ="/DeleteVectors";
	private static final String QUERY_VECTORS_ENDPOINT ="/QueryVectors";
	private String apiKey = null;
	private String secretKey = null;
	private String indexName = null;
	private String distanceMetric = null;
	private String vectorBucketName = null;
	private String dataType = "float32";
	private int dimension = 1024;
	private String awsS3VectorUrl= null;
	
	
	@Override
	public void open(Properties smssProp) throws Exception {
	    super.open(smssProp);
	    this.awsS3VectorUrl = this.smssProp.getProperty(Constants.HOSTNAME);
	
	    this.apiKey = smssProp.getProperty(API_KEY);
		if (this.apiKey == null || (this.apiKey = this.apiKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the api key");
		}
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		if (this.secretKey == null || (this.secretKey = this.secretKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the secret key");
		}
	    
	    this.indexName = this.smssProp.getProperty(INDEX_NAME);
		if (!this.indexName.matches(INDEX_NAME_PATTERN)) {
			throw new IllegalArgumentException(
					"Index name must only contain lowercase letters, digits or dashes, cannot start or end with dashes and is limited to 128 characters");
		}
	
	    String dimensionStr = smssProp.getProperty(DIMENSION);
	    if (dimensionStr == null || dimensionStr.trim().isEmpty()) {
	        throw new IllegalArgumentException("Dimension is required");
	    }
	    try {
	        this.dimension = Integer.parseInt(dimensionStr.trim());
	        if (this.dimension < 1 || this.dimension > 4096) {
	            throw new IllegalArgumentException("Dimension must be between 1 and 4096");
	        }
	    } catch (NumberFormatException e) {
	        throw new IllegalArgumentException("Dimension must be an integer", e);
	    }
	
	    String dataTypeInput = smssProp.getProperty(DATA_TYPE);
	    if(dataTypeInput != null && !(dataTypeInput=dataTypeInput.trim()).isEmpty()) {
			this.dataType = dataTypeInput;
		}
	
	    this.distanceMetric = smssProp.getProperty(DISTANCE_METRIC);
	    if (this.distanceMetric == null || !(this.distanceMetric = this.distanceMetric.trim()).matches("(?i)(cosine|euclidean)")) {
	        throw new IllegalArgumentException("distanceMetric must be either 'cosine' or 'euclidean'");
	    }
	
	    this.vectorBucketName = smssProp.getProperty(VECTOR_BUCKET_NAME);
	    if (this.vectorBucketName == null || (this.vectorBucketName = this.vectorBucketName.trim()).isEmpty()) {
	        classLogger.error("S3 Vector Bucket name is required");
	        throw new IllegalArgumentException("S3 Vector Bucket name is required");
	    }
	   
	    // Optional: metadataConfiguration.nonFilterableMetadataKeys
	    String metadataKeys = smssProp.getProperty(NON_FILTERABLE_KEYS);
	    List<String> nonFilterableKeys = new ArrayList<>();
	    if (metadataKeys != null && !metadataKeys.trim().isEmpty()) {
	        nonFilterableKeys = Arrays.stream(metadataKeys.split(","))
	                                  .map(String::trim)
	                                  .collect(Collectors.toList());
	    }
	 // If index doesn't exist, we create it
	    getOrCreateIndexIfNotExists();
	}
		
		
	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception {
	    if (!modelPropsLoaded) {
	        verifyModelProps();
	    }

	    if (insight == null) {
	        throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
	    }

	    IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);

	    // Generate embeddings for all rows
	    vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);

	    List<JsonObject> vectorList = new ArrayList<>();
	    Map<String, Integer> sourceId = new HashMap<>();

	    for (VectorDatabaseCSVRow row : vectorCsvTable.getRows()) {
	        String source = row.getSource();
	        int index = sourceId.getOrDefault(source, 0);
	        sourceId.put(source, index + 1);
	        JsonObject vectorObject = new JsonObject();
	        // Unique vector key
	        String vectorKey = sanitizeKey(source) + "_" + index;
	        vectorObject.addProperty("key", vectorKey);

	        // Add vector data (float32)
	        JsonArray embeddingArray = convertListNumToJsonArray(row.getEmbeddings());
	        JsonObject data = new JsonObject();
	        data.add("float32", embeddingArray);
	        vectorObject.add("data", data);

	        // Metadata
	        JsonObject metadata = new JsonObject();
	        metadata.addProperty(VectorDatabaseCSVTable.SOURCE, row.getSource());
	        metadata.addProperty(VectorDatabaseCSVTable.MODALITY, row.getModality());
	        metadata.addProperty(VectorDatabaseCSVTable.DIVIDER, row.getDivider());
	        metadata.addProperty(VectorDatabaseCSVTable.PART, row.getPart());
	        metadata.addProperty(VectorDatabaseCSVTable.TOKENS, row.getTokens());
	        metadata.addProperty(VectorDatabaseCSVTable.CONTENT, row.getContent());
	        vectorObject.add("metadata", metadata);

	        vectorList.add(vectorObject);
	    }

	    // Final request payload
	    JsonObject getRequest = new JsonObject();
	    getRequest.addProperty("indexName", this.indexName);
	    getRequest.addProperty("vectorBucketName", this.vectorBucketName);

	    JsonArray vectors = new JsonArray();
	    for (JsonObject vectorObj : vectorList) {
	        vectors.add(vectorObj);
	    }
	    getRequest.add("vectors", vectors);
	    String url = this.awsS3VectorUrl + PUT_VECTORS_ENDPOINT;
	    // Send POST request
	    String response = HttpHelperUtility.postRequestStringBody(
	        url,
	        generateHeaders(getRequest.toString(), PUT_VECTORS_ENDPOINT),
	        getRequest.toString(),
	        ContentType.APPLICATION_JSON,
	        null, null, null
	    );

	    if (response == null || (response = response.trim()).isEmpty()) {
	        throw new IllegalArgumentException("No response received from AWS S3 Vectors endpoint");
	    }

	    classLogger.info("Successfully inserted " + vectorList.size() + " vectors into AWS S3 vector index " + this.indexName);
	}
   
	
	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws Exception {
	    String indexClass = this.defaultIndexClass;
	    if (parameters.containsKey("indexClass")) {
	        indexClass = (String) parameters.get("indexClass");
	    }

	    final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR
	            + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;

	    // 1. List vectors from S3 Vector DB
	    JsonObject listRequest = new JsonObject();
	    listRequest.addProperty("indexName", this.indexName);
	    listRequest.addProperty("vectorBucketName", this.vectorBucketName);
	    listRequest.addProperty("returnMetadata", true);
	    listRequest.addProperty("returnData", false);
	    listRequest.addProperty("topK", 10000);

	    String listUrl = this.awsS3VectorUrl + LIST_VECTORS_ENDPOINT;
	    String listResponse = HttpHelperUtility.postRequestStringBody(
	        listUrl,
	        generateHeaders(listRequest.toString(), LIST_VECTORS_ENDPOINT),
	        listRequest.toString(),
	        ContentType.APPLICATION_JSON,
	        null, null, null
	    );

	    List<String> keysToDelete = new ArrayList<>();
	    if (listResponse != null && !listResponse.trim().isEmpty()) {
	        JsonObject responseJson = JsonParser.parseString(listResponse).getAsJsonObject();
	        JsonArray vectors = responseJson.getAsJsonArray("vectors");

	        for (JsonElement vectorElement : vectors) {
	            JsonObject vectorObj = vectorElement.getAsJsonObject();
	            String key = vectorObj.get("key").getAsString();
	            JsonObject metadata = vectorObj.getAsJsonObject("metadata");
	            if (metadata != null && metadata.has("Source")) {
	                String sourceFileName = metadata.get("Source").getAsString();
	                for (String inputFileName : fileNames) {
	                    if (sourceFileName.equalsIgnoreCase(inputFileName)) {
	                        keysToDelete.add(key);
	                        break;
	                    }
	                }
	            }
	        }
	    }

	    // 2. Delete matching keys
	    if (!keysToDelete.isEmpty()) {
	        JsonObject deleteRequest = new JsonObject();
	        deleteRequest.addProperty("indexName", this.indexName);
	        deleteRequest.addProperty("vectorBucketName", this.vectorBucketName);

	        JsonArray keysArray = new JsonArray();
	        for (String key : keysToDelete) {
	            keysArray.add(key);
	        }
	        deleteRequest.add("keys", keysArray);

	        String deleteUrl = this.awsS3VectorUrl + DELETE_VECTORS_ENDPOINT;
	        String deleteResponse = HttpHelperUtility.postRequestStringBody(
	            deleteUrl,
	            generateHeaders(deleteRequest.toString(), DELETE_VECTORS_ENDPOINT),
	            deleteRequest.toString(),
	            ContentType.APPLICATION_JSON,
	            null, null, null
	        );

	        if (deleteResponse.trim().isEmpty()) {
	            classLogger.info("Successfully deleted vectors from AWS S3 Vector database.");
	        } else {
	            classLogger.warn("Unexpected response while deleting vectors: " + deleteResponse);
	        }
	    } else {
	        classLogger.warn("No matching keys found for files: " + fileNames);
	    }

	    // 3. Delete files from local storage
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

	    // 4. Delete from cloud if in cluster
	    if (ClusterUtil.IS_CLUSTER) {
	        Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(
	            engineId,
	            this.getCatalogType(),
	            filesToRemoveFromCloud.toArray(new String[0])
	        ));
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
	
		    // Generate query vector using your embedder engine
		    IModelEngine engine = Utility.getModel(this.embedderEngineId);
		    EmbeddingsModelEngineResponse embeddingsResponse = engine.embeddings(Arrays.asList(searchStatement), insight, null);
		    List<Double> vector = embeddingsResponse.getResponse().get(0);
		 
		    // Build request body for /QueryVectors
		    JsonObject requestBody = new JsonObject();
	        requestBody.addProperty("indexName", this.indexName);
	        requestBody.addProperty("vectorBucketName", this.vectorBucketName);
	
		    requestBody.addProperty("topK", limit.intValue());
		    requestBody.addProperty("returnMetadata", true);
		    requestBody.addProperty("returnDistance", true);
	
		    // Query vector array
		    JsonArray queryVectorArray = new JsonArray();
		    for (Number val : vector) {
		        queryVectorArray.add(val.floatValue());
		    }
	
		    JsonObject queryVectorObj = new JsonObject();
		    queryVectorObj.add("float32", queryVectorArray);
		    requestBody.add("queryVector", queryVectorObj);
		    
		    AwsVectorQueryFilterTranslationHelper filterBuilder = new AwsVectorQueryFilterTranslationHelper();
		    if (parameters.containsKey(AbstractVectorDatabaseEngine.FILTERS_KEY)) {
		        List<IQueryFilter> filters = (List<IQueryFilter>) parameters.get(AbstractVectorDatabaseEngine.FILTERS_KEY);

		        IQueryFilter combinedFilter = filters.size() == 1
		            ? filters.get(0)
		            : new AndQueryFilter(filters);  // Combine multiple filters with AND

		        StringBuilder filter = filterBuilder.processAwsFilter(combinedFilter);
		        
		        // Parse the filter string into a JsonObject
		        JsonParser parser = new JsonParser();
		        JsonObject filterObject = parser.parse(filter.toString()).getAsJsonObject();

		        //Add filter as a JSON object
		        requestBody.add("filter", filterObject);

		    }

		    if (parameters.containsKey(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY)) {
		        List<IQueryFilter> metaFilters = (List<IQueryFilter>) parameters.get(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY);

		        IQueryFilter combinedMetaFilter = metaFilters.size() == 1
		            ? metaFilters.get(0)
		            : new AndQueryFilter(metaFilters); 

		        StringBuilder filter = filterBuilder.processAwsFilter(combinedMetaFilter);
		        // Parse the filter string into a JsonObject
		        JsonParser parser = new JsonParser();
		        JsonObject filterObject = parser.parse(filter.toString()).getAsJsonObject();

		        //Add filter as a JSON object
		        requestBody.add("filter", filterObject);
		    }
	
		    // Call AWS S3 Vector endpoint
		    String url = this.awsS3VectorUrl + QUERY_VECTORS_ENDPOINT;
				String response = null;
				try {
					response = HttpHelperUtility.postRequestStringBody(
					    url,
					    generateHeaders(requestBody.toString(), QUERY_VECTORS_ENDPOINT),
					    requestBody.toString(),
					    ContentType.APPLICATION_JSON,
					    null,
					    null,
					    null
					);
				} catch (Exception e) {
					e.printStackTrace();
				}
	
		    JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
		    JsonArray vectors = responseJson.getAsJsonArray("vectors");
	
		    List<Map<String, Object>> vectorSearchResults = new ArrayList<>();
		    for (JsonElement resultEl : vectors) {
		        JsonObject vectorObj = resultEl.getAsJsonObject();
		        JsonObject metadata = vectorObj.getAsJsonObject("metadata");
	
		        Map<String, Object> resultMap = new HashMap<>();
		        resultMap.put(VectorDatabaseCSVTable.SOURCE, getJsonValue(metadata.get("Source")));
		        resultMap.put(VectorDatabaseCSVTable.MODALITY, getJsonValue(metadata.get("Modality")));
		        resultMap.put(VectorDatabaseCSVTable.DIVIDER, getJsonValue(metadata.get("Divider")));
		        resultMap.put(VectorDatabaseCSVTable.PART, getJsonValue(metadata.get("Part")));
		        resultMap.put(VectorDatabaseCSVTable.TOKENS, getJsonValue(metadata.get("Tokens")));
		        resultMap.put(VectorDatabaseCSVTable.CONTENT, getJsonValue(metadata.get("Content")));
		        resultMap.put("Score", vectorObj.get("distance"));
	
		        vectorSearchResults.add(resultMap);
		    }
	
		    return vectorSearchResults;
	}


	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
	
		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR
				+ DOCUMENTS_FOLDER_NAME);
	
		List<Map<String, Object>> filesInS3Vector = new ArrayList<>();
		Set<String> uniqueSources = new HashSet<>();
	
		JsonObject requestBody = new JsonObject();
		requestBody.addProperty("vectorBucketName", this.vectorBucketName);
		requestBody.addProperty("indexName", this.indexName);
		requestBody.addProperty("returnMetadata", true);
		String url = this.awsS3VectorUrl + LIST_VECTORS_ENDPOINT;
		String response;
		try {
			response = HttpHelperUtility.postRequestStringBody(url,
					generateHeaders(requestBody.toString(), LIST_VECTORS_ENDPOINT), requestBody.toString(),
					ContentType.APPLICATION_JSON, null, null, null);
	
			JsonObject jsonObject = JsonParser.parseString(response).getAsJsonObject();
			JsonArray vectors = jsonObject.getAsJsonArray("vectors");
	
			for (JsonElement vectorEle : vectors) {
				JsonObject vec = vectorEle.getAsJsonObject();
				JsonObject metadata = vec.getAsJsonObject("metadata");
				if (metadata != null && metadata.has(VectorDatabaseCSVTable.SOURCE)) {
					String source = metadata.get(VectorDatabaseCSVTable.SOURCE).getAsString();
	
					if (uniqueSources.contains(source))
						continue;
					uniqueSources.add(source);
	
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
					filesInS3Vector.add(fileInfo);
				}
			}
	
		} catch (Exception e) {
			e.printStackTrace();
	
		}
		return filesInS3Vector;
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
	    List<Map<String, Object>> documentsList = new ArrayList<>();
	    String nextToken = null;
	    JsonObject requestBody = new JsonObject();
	    requestBody.addProperty("vectorBucketName", this.vectorBucketName);
	    requestBody.addProperty("indexName", this.indexName);
	    requestBody.addProperty("maxResults", 500);
	    requestBody.addProperty("returnMetadata", true);
	    requestBody.addProperty("returnData", true);
	    String url = this.awsS3VectorUrl + LIST_VECTORS_ENDPOINT;
	    do {
	    	 if (nextToken != null && !nextToken.isEmpty()) {
		            requestBody.addProperty("nextToken", nextToken);
		        }
	
		   
	        String response;
			try {
				response = HttpHelperUtility.postRequestStringBody(url,
						generateHeaders(requestBody.toString(), LIST_VECTORS_ENDPOINT), requestBody.toString(),
						ContentType.APPLICATION_JSON, null, null, null);
			
	        JsonObject responseJson = JsonParser.parseString(response).getAsJsonObject();
	        JsonArray vectors = responseJson.getAsJsonArray("vectors");
	
	        if (vectors != null) {
	            for (JsonElement ele : vectors) {
	                JsonObject vectorObj = ele.getAsJsonObject();
	                Map<String, Object> document = new HashMap<>();
	
	                String key = vectorObj.has("key") ? vectorObj.get("key").getAsString() : null;
	                document.put("AWS_S3_Key", key);
	
	                JsonObject metadata = vectorObj.has("metadata") && vectorObj.get("metadata").isJsonObject()
	                        ? vectorObj.getAsJsonObject("metadata")
	                        : null;
	
	                if (metadata != null) {
	                    document.put(VectorDatabaseCSVTable.SOURCE, getJsonValue(metadata.get(VectorDatabaseCSVTable.SOURCE)));
	                    document.put(VectorDatabaseCSVTable.MODALITY, getJsonValue(metadata.get(VectorDatabaseCSVTable.MODALITY)));
	                    document.put(VectorDatabaseCSVTable.DIVIDER, getJsonValue(metadata.get(VectorDatabaseCSVTable.DIVIDER)));
	                    document.put(VectorDatabaseCSVTable.PART, getJsonValue(metadata.get(VectorDatabaseCSVTable.PART)));
	                    document.put(VectorDatabaseCSVTable.TOKENS, getJsonValue(metadata.get(VectorDatabaseCSVTable.TOKENS)));
	                    document.put(VectorDatabaseCSVTable.CONTENT, getJsonValue(metadata.get(VectorDatabaseCSVTable.CONTENT)));
	                }
	
	                documentsList.add(document);
	            }
	        }
	
	        nextToken = responseJson.has("nextToken") && !responseJson.get("nextToken").isJsonNull()
	                ? responseJson.get("nextToken").getAsString()
	                : null;
	        
			} catch (Exception e) {
				e.printStackTrace();
			}
	
	    } while (nextToken != null && !nextToken.isEmpty());
	
	    return documentsList;
	}

	
	
	/**
	 * 
	 * @throws Exception
	 */
	private void createIndexInAwsS3() throws Exception {
		JsonObject request = new JsonObject();
		request.addProperty("dataType", this.dataType);
		request.addProperty("distanceMetric", this.distanceMetric);
		request.addProperty("indexName", this.indexName);
		request.addProperty("vectorBucketName", this.vectorBucketName);
		request.addProperty("dimension", this.dimension);

		JsonObject metadataConfig = new JsonObject();
		JsonArray nonFilterableKey = new JsonArray();
		nonFilterableKey.add("string");
		metadataConfig.add("nonFilterableMetadataKeys", nonFilterableKey);

		request.add("metadataConfiguration", metadataConfig);

		String url = this.awsS3VectorUrl + CREATE_INDEX_ENDPOINT;

		HttpHelperUtility.postRequestStringBody(
			url,
			generateHeaders(request.toString(), CREATE_INDEX_ENDPOINT),
			request.toString(),
			ContentType.APPLICATION_JSON,
			null, null, null
		);

		classLogger.info("Created index in AWS S3 Vector DB: " + this.indexName);
	}

   /**
    * 
    * @throws Exception
    */
	private void getOrCreateIndexIfNotExists() throws Exception {
		String url = this.awsS3VectorUrl + GET_INDEX_ENDPOINT;

		JsonObject getRequest = new JsonObject();
		getRequest.addProperty("indexName", this.indexName);
		getRequest.addProperty("vectorBucketName", this.vectorBucketName);

		try {
			HttpHelperUtility.postRequestStringBody(
				url,
				generateHeaders(getRequest.toString(), GET_INDEX_ENDPOINT),
				getRequest.toString(),
				ContentType.APPLICATION_JSON,
				null, null, null
			);
			classLogger.info("Index already exists: " + this.indexName);
		} catch (Exception e) {
			// If index doesn't exist, we create it
			classLogger.warn("Index not found. Proceeding to create index: " + this.indexName);
			createIndexInAwsS3();
		}
	}
	
	/**
	 * Generates AWS SigV4 signed headers for making a POST request to a custom AWS S3-compatible vector endpoint.
	 * 
	 * @param body
	 * @param AWS_S3_ENDPOINT
	 * @return
	 * @throws Exception
	 */
	private Map<String, String> generateHeaders(String body, String AWS_S3_ENDPOINT) throws Exception {
	    AwsBasicCredentials awsCreds = AwsBasicCredentials.create(this.apiKey, this.secretKey);
	    Aws4Signer signer = Aws4Signer.create();
	
	    URI uri = URI.create(this.awsS3VectorUrl + AWS_S3_ENDPOINT);
	
	    SdkHttpFullRequest unsignedRequest = SdkHttpFullRequest.builder()
	        .method(SdkHttpMethod.POST)
	        .uri(uri)
	        .encodedPath(AWS_S3_ENDPOINT)
	        .putHeader("Content-Type", "application/json")
	        .putHeader("Host", uri.getHost())
	        .contentStreamProvider(() -> new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)))
	        .build();
	
	    SdkHttpFullRequest signedRequest = signer.sign(unsignedRequest,
	        Aws4SignerParams.builder()
	            .awsCredentials(awsCreds)
	            .signingName("s3vectors")  // AWS service name
	            .signingRegion(Region.US_EAST_1)
	            .build());
	
	    Map<String, String> headers = new HashMap<>();
	    signedRequest.headers().forEach((key, value) -> headers.put(key, String.join(",", value)));
	    return headers;
	}
	
	/**
	 * 
	 * @param list
	 * @return
	 */
	private JsonArray convertListNumToJsonArray(List<? extends Number> list) {
		JsonArray array = new JsonArray();
		for (Number num : list) {
			array.add(num.floatValue()); // Enforce float32
		}
		return array;
	}

	/**
	 * 
	 * @param key
	 * @return
	 */
	public String sanitizeKey(String key) {
		// Regular expression to match allowed characters
		String regex = "[^a-zA-Z0-9_\\-=]";
		// Replace all characters not matching the regex with an underscore
		return key.replaceAll(regex, "_");
	}
	
	/**
	 * Helper method to extract the correct type from JsonElement.
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
		return VectorDatabaseTypeEnum.AWS_S3;
	}


	@Override
	protected String getDefaultDistanceMethod() {
		// TODO Auto-generated method stub
			return null;
		}
	
	}

