package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class PineConeVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LoggerFactory.getLogger(PineConeVectorDatabaseEngine.class);

	private final String NAMESPACE = "NAMESPACE";
	private final String API_UPSERT = "/vectors/upsert";
	private final String API_DELETE = "/vectors/delete";
	private final String API_QUERY = "/query";
	private final String API_KY= "Api-Key";
	private final String LIST_QUERY = "/vectors/list?namespace=";
	private final String HASH = "#";
	
	private final String PREFIX = "&prefix=";
	private final String PAGINATION_TOKEN = "&paginationToken=";
	
	private String hostname = null;
	private String apiKey = null;
	private String defaultNamespace = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.apiKey = smssProp.getProperty(Constants.API_KEY);
		if (this.apiKey == null || (this.apiKey = this.apiKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the api key");
		}

		this.hostname = smssProp.getProperty(Constants.HOSTNAME);
		this.defaultNamespace = this.smssProp.getProperty(NAMESPACE);
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
		try {
			vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred creating the embeddings for the generated chunks. Detailed error message = " + e.getMessage());
		}
		
		// Sample URL:
		// https://docs-quickstart-index3-fiarr5p.svc.aped-4627-b74a.pinecone.io/vectors/upsert;
		String url = this.hostname + API_UPSERT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		JsonArray vectors = new JsonArray();
		// loop through and make the giant json
		int fileCounter = 0;
		String previousFileName = null;
		for (int rowIndex = 0; rowIndex < vectorCsvTable.rows.size(); rowIndex++) {
			VectorDatabaseCSVRow row = vectorCsvTable.getRows().get(rowIndex);
			
			JsonObject metadataJson = new JsonObject();
			metadataJson.addProperty("Source", row.getSource());
			metadataJson.addProperty("Modality", row.getModality());
			metadataJson.addProperty("Divider", row.getDivider());
			metadataJson.addProperty("Part", row.getPart());
			metadataJson.addProperty("Tokens", row.getTokens());
			metadataJson.addProperty("Content", row.getContent());

			List<Double> vector = getEmbeddingsDouble(row.getContent(), insight);
			if (row.getSource().equals(previousFileName)) {
				fileCounter = 0;
			}

			JsonObject thisChunkJson = new JsonObject();
			thisChunkJson.addProperty("id", row.getSource().replaceAll(" ", "_") + "-" + fileCounter++);
			JsonArray thisEmbeddingVector = new JsonArray();
			for(Double d : vector) {
				thisEmbeddingVector.add(d);
			}
			thisChunkJson.add("values", thisEmbeddingVector);
			thisChunkJson.add("metadata", metadataJson);
			vectors.add(thisChunkJson);
		}
		
		JsonObject vectorsMap = new JsonObject();		
		vectorsMap.addProperty("namespace", this.defaultNamespace);
		vectorsMap.add("vectors", vectors);
		HttpHelperUtility.postRequestStringBody(url, headersMap, vectorsMap.toString(), ContentType.APPLICATION_JSON, null, null, null);
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		List<String> filesToRemoveFromCloud = new ArrayList<String>();

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		
		// need to get the source names and then delete it based on the names
		for (int fileIndex = 0; fileIndex < fileNames.size(); fileIndex++) {
			String fileName = fileNames.get(fileIndex);

			boolean firstExecution = true;
			String paginationToken = null;
			while(firstExecution || paginationToken!=null) {
				String listVectorsUrl = this.hostname + LIST_QUERY + this.defaultNamespace;
				if(paginationToken!=null) {
					listVectorsUrl+=PAGINATION_TOKEN+paginationToken;
				} else {
					listVectorsUrl+=PREFIX + fileName.replaceAll(" ", "_") + HASH;
				}
				
				String idListResponse = HttpHelperUtility.getRequest(listVectorsUrl, headersMap, null, null, null);
				Map<String, Object> responseMap = gson.fromJson(idListResponse, new TypeToken<Map<String, Object>>() {}.getType());
				
				List<Map<String, String>> vectors = (List<Map<String, String>>) responseMap.get("vectors");
				executeDelete(this.hostname + API_DELETE, headersMap, vectors);

				// we can only pull 100 at a time
				// we need to check if there is pagination to keep going
				Map<String, String> paginationMap = (Map<String, String>) responseMap.get("pagination");
				if(paginationMap != null && !paginationMap.isEmpty()) {
					paginationToken = paginationMap.get("next");
				} else {
					paginationToken = null;
				}
				
				firstExecution=false;
			}
			
			String documentName = Paths.get(fileName).getFileName().toString();
			// remove the physical documents
			File documentFile = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents", documentName);
			try {
				if (documentFile.exists()) {
					FileUtils.forceDelete(documentFile);
					filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
	}
	
	/**
	 * 
	 * @param url
	 * @param headersMap
	 * @param listReturnVector
	 */
	private void executeDelete(String url, Map<String, String> headersMap, List<Map<String, String>> listReturnVector) {
		if(listReturnVector == null || listReturnVector.isEmpty()) {
			return;
		}
		JsonArray idsJsonArray = new JsonArray();
		for (Map<String, String> v : listReturnVector) {
			idsJsonArray.add(v.get("id"));
		}
		
		JsonObject deleteJson = new JsonObject();
		deleteJson.add("ids", idsJsonArray);
		deleteJson.addProperty("namespace", this.defaultNamespace);
		HttpHelperUtility.postRequestStringBody(url, headersMap, deleteJson.toString(), ContentType.APPLICATION_JSON, null, null, null);
	}

	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		if (limit == null) {
			limit = 3;
		}
		if (!modelPropsLoaded) {
			verifyModelProps();
		}

		String url = this.hostname + API_QUERY;

		JsonObject queryJson = new JsonObject();
		List<Double> vector = getEmbeddingsDouble(searchStatement, insight);
		JsonArray embeddingsJsonArr = new JsonArray();
		for (int i = 0; i < vector.size(); i++) {
			embeddingsJsonArr.add(vector.get(i));
		}
		queryJson.addProperty("topK", limit);
		queryJson.addProperty("includeMetadata", true);
		queryJson.addProperty("includeValues", true);
		queryJson.addProperty("namespace", this.defaultNamespace);
		queryJson.add("vector", embeddingsJsonArr);

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		String nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, headersMap, queryJson.toString(), ContentType.APPLICATION_JSON, null, null, null);
		
		Gson gson = new Gson();
		Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, new TypeToken<Map<String, Object>>() {}.getType());
		List<Map<String, Object>> matches = (List<Map<String, Object>>) responseMap.get("matches");
		
		List<Map<String, Object>> retOut = new ArrayList<>();

		for (int i = 0; i < matches.size(); i++) {
			Map<String, Object> thisMatch = matches.get(i);
			Map<String, Object> metadataMap = (Map<String, Object>) thisMatch.get("metadata");

			Map<String,Object> resultMap = new HashMap<>();
			resultMap.put("Id", matches.get(i).get("id"));
			resultMap.put("Score", matches.get(i).get("score"));
			resultMap.put("Content", metadataMap.get("Content"));
			resultMap.put("Divider", metadataMap.get("Divider"));
			resultMap.put("Modality", metadataMap.get("Modality"));
			resultMap.put("Part", metadataMap.get("Part"));
			resultMap.put("Source", metadataMap.get("Source"));
			resultMap.put("Tokens", metadataMap.get("Tokens"));
			retOut.add(resultMap);
		}

		return retOut;
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.PINECONE;
	}

}