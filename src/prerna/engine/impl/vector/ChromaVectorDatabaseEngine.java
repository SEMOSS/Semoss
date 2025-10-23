package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class ChromaVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(ChromaVectorDatabaseEngine.class);

	public static final String CHROMA_CLASSNAME = "CHROMA_COLLECTION_NAME";
	public static final String COLLECTION_ID = "COLLECTION_ID";
	public static final String DB_NAME = "DB_NAME";
	public static final String TENANT = "TENANT";

	public static final String DATABASES = "/databases";
	public static final String COLLECTIONS = "/collections";
	public static final String TENANTS = "api/v2/tenants";

	private final String API_TOKEN_KEY = "X-Chroma-Token";

	private final static String DEFAULT_TENANT = "default_tenant";
	private final static String DEFAULT_DATABASE = "default_database";

	private final String API_ADD = "/add";
	private final String API_DELETE = "/delete";
	private final String API_QUERY = "/query";

	private String url = null;
	private String apiKey = null;
	private String tenant = null;
	private String db_name = null;
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
		this.tenant = smssProp.getProperty(TENANT);
		this.db_name = smssProp.getProperty(DB_NAME);

		// create or fetch collection Id from the Chroma DB
		this.collectionID = createCollection(this.className);
	}

	/**
	 * 
	 * @param url
	 * @param tenant
	 * @param database
	 */
	public static String collections(String url, String tenant, String database) {
		if (tenant == null || tenant.isEmpty()) {
			tenant = DEFAULT_TENANT;
		}
		if (database == null || database.isEmpty()) {
			database = DEFAULT_DATABASE;
		}
		return new StringBuilder(url).append(TENANTS).append("/").append(tenant).append(DATABASES).append("/")
				.append(database).append(COLLECTIONS).toString();
	}

	/**
	 * 
	 * @param url
	 * @param tenant
	 * @param database
	 * @param collectionId
	 * @param action
	 */
	public static String collection(String url, String tenant, String database, String collectionId, String action) {
		if (tenant == null || tenant.isEmpty()) {
			tenant = DEFAULT_TENANT;
		}
		if (database == null || database.isEmpty()) {
			database = DEFAULT_DATABASE;
		}
		return new StringBuilder(url).append(TENANTS).append("/").append(tenant).append(DATABASES).append("/")
				.append(database).append(COLLECTIONS).append("/").append(collectionId).append(action).toString();
	}

	/**
	 * 
	 * @param collectionName
	 */
	private String createCollection(String collectionName) {
		collectionName = collectionName.replaceAll(" ", "_");
		Gson gson = new GsonBuilder().setPrettyPrinting().create();

		String url = collections(this.url, this.tenant, this.db_name);

		Map<String, String> headersMap = new HashMap<>();
		if (this.apiKey != null && !this.apiKey.isEmpty()) {
			headersMap.put(API_TOKEN_KEY, this.apiKey);
			headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		} else {
			headersMap = null;
		}
		String nearestNeigborResponse = null;
		try {
			nearestNeigborResponse = HttpHelperUtility.getRequest(url, headersMap, null, null, null);
		} catch (Exception e) {
			classLogger.error("Unable to create connection");
			throw new SemossPixelException("Unable to create connection");
		}
		List<Map<String, Object>> responseListMap = gson.fromJson(nearestNeigborResponse,
				new TypeToken<List<Map<String, Object>>>() {
				}.getType());
		if (responseListMap == null) {
			throw new SemossPixelException("Unexpected response listing collections.");
		}
		for (Map<String, Object> responseMap : responseListMap) {
			Object name = responseMap.get("name");
			if (name != null && name.toString().equals(collectionName)) {
				Object idObj = responseMap.get("id");
				if (idObj == null) {
					throw new SemossPixelException("Collection found but missing id.");
				}
				return (String) idObj;
			}
		}
		nearestNeigborResponse = null;
		Map<String, Object> collectionNameToCreate = new HashMap<>();
		collectionNameToCreate.put("name", collectionName);
		String body = gson.toJson(collectionNameToCreate);
		nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, headersMap, body,
				ContentType.APPLICATION_JSON, null, null, null);
		Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, new TypeToken<Map<String, Object>>() {
		}.getType());
		if (responseMap == null || responseMap.get("id") == null) {
			throw new SemossPixelException("Failed to create collection or missing id in response.");
		}
		return (String) responseMap.get("id");
	}

	@Override
	protected String getDefaultDistanceMethod() {
		return "cosine";
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight,
			Map<String, Object> parameters) throws Exception {
		String url = collection(this.url, this.tenant, this.db_name, this.collectionID, API_ADD);
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
			throw new IllegalArgumentException(
					"Error occurred creating the embeddings for the generated chunks. Detailed error message = "
							+ e.getMessage());
		}
		Map<String, Object> vectors = new HashMap<>();
		List<String> ids = new ArrayList<>();
		List<Float[]> embeddings = new ArrayList<>();
		List<Map<String, Object>> metadatas = new ArrayList<>();
		Map<String, Integer> fileRecordCountMap = new HashMap<>();
		for (int rowIndex = 0; rowIndex < vectorCsvTable.rows.size(); rowIndex++) {
			VectorDatabaseCSVRow row = vectorCsvTable.getRows().get(rowIndex);
			fileRecordCountMap.put(row.getSource(), fileRecordCountMap.getOrDefault(row.getSource(), 0) + 1);
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

		Map<String, String> headersMap = new HashMap<>();
		if (this.apiKey != null && !this.apiKey.isEmpty()) {
			headersMap.put(API_TOKEN_KEY, this.apiKey);
			headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		} else {
			headersMap = null;
		}
		String response = HttpHelperUtility.postRequestStringBody(url, headersMap, body, ContentType.APPLICATION_JSON,
				null, null, null);
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		// TODO: let us add validation by looking at the response
		for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
			String file = entry.getKey();
			int totalRecords = entry.getValue();
			long inserted = 0;
			long failed = 0;
			String status;

			if (response != null && !response.trim().isEmpty()) {
				inserted = totalRecords;
				failed = 0;
			} else {
				inserted = 0;
				failed = totalRecords;
			}

			if (inserted == totalRecords) {
				status = "SUCCESS";
			} else {
				status = "FAILED";
			}
			fileStatusList.add(new FileEmbeddingStatus(file, status, inserted, failed, totalRecords));
		}
		return fileStatusList;
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws IOException {
		String url = collection(this.url, this.tenant, this.db_name, this.collectionID, API_DELETE);
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
		List<String> filesToRemoveFromCloud = new ArrayList<String>();

		// need to get the source names and then delete it based on the names
		for (int fileIndex = 0; fileIndex < sourceNames.size(); fileIndex++) {
			String fileName = fileNames.get(fileIndex);

			// Delete document in ChromaDB using their ID, but to get the ID we need to find
			// the ID of a document first. Check the delete API call params
			// http://localhost:5000/api/v1/collections/{}/delete

			Map<String, Object> fileNamesForDelete = new HashMap<>();
			Map<String, String> sourceProperty = new HashMap<>();

			// replace spaces with _ since thats how
			// readCSV creates Source Property.
			sourceProperty.put("Source", fileName.replaceAll(" ", "_"));
			fileNamesForDelete.put("where", sourceProperty);
			String body = new Gson().toJson(fileNamesForDelete);

			Map<String, String> headersMap = new HashMap<>();
			if (this.apiKey != null && !this.apiKey.isEmpty()) {
				headersMap.put(API_TOKEN_KEY, this.apiKey);
				headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
			} else {
				headersMap = null;
			}
			String response = HttpHelperUtility.postRequestStringBody(url, headersMap, body,
					ContentType.APPLICATION_JSON, null, null, null);

			// TODO: let us add validation by looking at the response
			String documentName = Paths.get(fileName).getFileName().toString();
			// remove the physical documents
			File documentFile = new File(
					this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + "documents",
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
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		String url = collection(this.url, this.tenant, this.db_name, this.collectionID, API_QUERY);
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		if (limit == null) {
			limit = 3;
		}
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
			headersMap.put(API_TOKEN_KEY, this.apiKey);
			headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		} else {
			headersMap = null;
		}
		String nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, headersMap, body,
				ContentType.APPLICATION_JSON, null, null, null);

		Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, new TypeToken<Map<String, Object>>() {
		}.getType());
		if (responseMap == null) {
			throw new RuntimeException("Failed to query Chroma collection.");
		}
		// Retrieve the metadatas, distance and score list response
		List<Map<String, Object>> map = new ArrayList<>();
		List<List<Map<String, Object>>> metadatas = (List<List<Map<String, Object>>>) responseMap.get("metadatas");
		List<List<Double>> distances = (List<List<Double>>) responseMap.get("distances");
		System.out.println(metadatas);
		System.out.println(distances);
		if (metadatas != null && !metadatas.isEmpty() && distances != null && !distances.isEmpty()) {
			List<Map<String, Object>> metadata = (List<Map<String, Object>>) metadatas.get(0);
			List<Double> distance = distances.get(0);
			List<Double> score = new ArrayList<>();
			for (int i = 0; i < distance.size(); i++) {
				double Score = 1 - distance.get(i);
				score.add(Score);
			}
			for (int i = 0; i < metadata.size(); i++) {
				Map<String, Object> retMap = new LinkedHashMap<>();
				retMap.put("Score", score.get(i));
				retMap.put("Distance", distance.get(i));
				retMap.putAll(metadata.get(i));
				map.add(retMap);
			}
		}
		return map;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		// TODO: needs to grab 'Source' from the database
		// TODO: needs to grab 'Source' from the database
		// TODO: needs to grab 'Source' from the database
		// TODO: needs to grab 'Source' from the database
		// TODO: needs to grab 'Source' from the database
		// TODO: needs to grab 'Source' from the database

		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ DOCUMENTS_FOLDER_NAME);

		List<Map<String, Object>> fileList = new ArrayList<>();

		File[] files = documentsDir.listFiles();
		if (files != null) {
			for (File file : files) {
				String fileName = file.getName();
				long fileSizeInBytes = file.length();
				double fileSizeInMB = (double) fileSizeInBytes / (1024);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String lastModified = dateFormat.format(new Date(file.lastModified()));

				Map<String, Object> fileInfo = new HashMap<>();
				fileInfo.put("fileName", fileName);
				fileInfo.put("fileSize", fileSizeInMB);
				fileInfo.put("lastModified", lastModified);
				fileList.add(fileInfo);
			}
		}

		return fileList;
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		throw new IllegalArgumentException("This method has not been implemented yet");
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.CHROMA;
	}

}