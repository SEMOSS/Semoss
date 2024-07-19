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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class ChromaVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(ChromaVectorDatabaseEngine.class);
	
	public static final String CHROMA_CLASSNAME = "CHROMA_COLLECTION_NAME";
	public static final String DISTANCE_METHOD = "DISTANCE_METHOD";
	public static final String COLLECTION_ID = "COLLECTION_ID";

	private final String API_ADD = "/add";
	private final String API_DELETE = "/delete";
	private final String API_QUERY = "/query";
	
	private String url = null;
	private String apiKey = null;
	private String className = null;
	private String collectionID = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.url = smssProp.getProperty(Constants.HOSTNAME);
		if (!this.url.endsWith("/")) {
			this.url += "/";
		}
		this.apiKey = smssProp.getProperty(Constants.API_KEY);
		this.className = smssProp.getProperty(CHROMA_CLASSNAME);

		// create or fetch collection Id from the Chroma DB
		this.collectionID = createCollection(this.className);
	}

	/**
	 * 
	 * @param collectionName
	 */
	private String createCollection(String collectionName) {
		// check to see if the collection is available
		// if available, get the ID
		// if not create a collection and get the ID
		collectionName = collectionName.replaceAll(" ", "_");
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String nearestNeigborResponse = HttpHelperUtility.getRequest(this.url, null, null, null, null);
		List<Map<String, Object>> responseListMap = gson.fromJson(nearestNeigborResponse,
				new TypeToken<List<Map<String, Object>>>() {}.getType());
//		System.out.println(responseListMap);
		for (Map<String, Object> responseMap : responseListMap) {
			if (responseMap.get("name") != null && responseMap.get("name").toString().equals(collectionName)) {
				return (String) responseMap.get("id");
			}
		}

		// if the collection Name doesn't exist, create it and return the ID
		nearestNeigborResponse = null;
		Map<String, String> collectionNameToCreate = new HashMap<>();
		collectionNameToCreate.put("name", collectionName);
		String body = gson.toJson(collectionNameToCreate);
		nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(this.url, null, body, ContentType.APPLICATION_JSON, null, null, null);
		Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, new TypeToken<Map<String, Object>>() {}.getType());
		return (String) responseMap.get("id");
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

		Map<String, Object> vectors = new HashMap<>();
		List<String> ids = new ArrayList<>();
		List<Float[]> embeddings = new ArrayList<>();
		List<Map<String, Object>> metadatas = new ArrayList<>();

		for (int rowIndex = 0; rowIndex < vectorCsvTable.rows.size(); rowIndex++) {
			VectorDatabaseCSVRow row = vectorCsvTable.getRows().get(rowIndex);
			Map<String, Object> properties = new HashMap<>();
			properties.put("Source", row.getSource());
			properties.put("Modality", row.getModality());
			properties.put("Divider", row.getDivider());
			properties.put("Part", row.getPart());
			properties.put("Tokens", row.getTokens());
			properties.put("Content", row.getContent());

			// Float[] vectorEmbeddings = getEmbeddings(row.getContent(), insight);
			List<? extends Number> embedding = row.getEmbeddings();
			Float[] vectorEmbeddings = new Float[embedding.size()];
			for (int vecIndex = 0; vecIndex < vectorEmbeddings.length; vecIndex++) {
				vectorEmbeddings[vecIndex] = embedding.get(vecIndex).floatValue();
			}

			String currentRowID = row.getSource() + "-" + rowIndex;
			ids.add(currentRowID);
			embeddings.add(vectorEmbeddings);
			metadatas.add(properties);
		}

		vectors.put("ids", ids);
		vectors.put("embeddings", embeddings);
		vectors.put("metadatas", metadatas);

		String body = new Gson().toJson(vectors);
//		System.out.println(body);

		Map<String, String> headersMap = new HashMap<>();
		if (this.apiKey != null && !this.apiKey.isEmpty()) {
			headersMap.put("Api-Key", this.apiKey);
			headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		} else {
			headersMap = null;
		}

		String response = HttpHelperUtility.postRequestStringBody(this.url + this.collectionID + API_ADD, 
				headersMap, body, ContentType.APPLICATION_JSON, null, null, null);
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		List<String> filesToRemoveFromCloud = new ArrayList<String>();

		// need to get the source names and then delete it based on the names
		for (int fileIndex = 0; fileIndex < fileNames.size(); fileIndex++) {
			String fileName = fileNames.get(fileIndex);

			// Delete document in ChromaDB using their ID, but to get the ID we need to find
			// the ID of a document first. Check the delete API call params -
			// http://localhost:5000/api/v1/collections/{}/delete

			Map<String, Object> fileNamesForDelete = new HashMap<>();
			Map<String, String> sourceProperty = new HashMap<>();

			sourceProperty.put("Source", fileName.replaceAll(" ", "_")); // replace spaces with _ since thats how
																			// readCSV creates Source Property.
			fileNamesForDelete.put("where", sourceProperty);

			String body = new Gson().toJson(fileNamesForDelete);
//			System.out.println(body);

			Map<String, String> headersMap = new HashMap<>();
			if (this.apiKey != null && !this.apiKey.isEmpty()) {
				headersMap.put("Api-Key", this.apiKey);
				headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
			} else {
				headersMap = null;
			}

			String response = HttpHelperUtility.postRequestStringBody(this.url + this.collectionID + API_DELETE,
					headersMap, body, ContentType.APPLICATION_JSON, null, null, null);

			String documentName = Paths.get(fileName).getFileName().toString();
			// remove the physical documents
			File documentFile = new File(
					this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents",
					documentName);
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
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId,
					this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
	}

	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit, Map <String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		if (limit == null) {
			limit = 3;
		}
		
		List<Map<String, Object>> retOut = new ArrayList<Map<String, Object>>();
		Gson gson = new Gson();

		List<Double> vector = getEmbeddingsDouble(searchStatement, insight);
		Map<String, Object> query = new HashMap<>();
		List<List<Double>> queryEmbeddings = new ArrayList<>();
		// this is done to put a list of embeddings inside another list otherwise the
		// API throws error.
		queryEmbeddings.add(vector); 
										
		// List<Map<String, Object>> metadatas = new ArrayList<>(); add metadata filter
		query.put("query_texts", searchStatement);
		query.put("n_results", limit);
		query.put("query_embeddings", queryEmbeddings);
		String body = gson.toJson(query);

		Map<String, String> headersMap = new HashMap<>();
		if (this.apiKey != null && !this.apiKey.isEmpty()) {
			headersMap.put("Api-Key", this.apiKey);
			headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		} else {
			headersMap = null;
		}

		String nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(this.url + this.collectionID + API_QUERY,
				headersMap, body, ContentType.APPLICATION_JSON, null, null, null);

		Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, new TypeToken<Map<String, Object>>() {}.getType());
		retOut.add(responseMap);
		return retOut;
	}
	
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.CHROMA;
	}

}
