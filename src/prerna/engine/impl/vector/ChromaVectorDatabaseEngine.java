package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

public class ChromaVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(ChromaVectorDatabaseEngine.class);
	
	
	public static final String CHROMA_CLASSNAME = "CHROMA_COLLECTION_NAME";
	public static final String COLLECTION_ID = "COLLECTION_ID";
	
	private final String DB_NAME = "DB_NAME";
	private final String TENANT = "TENANT";
	
	private String url = null;
	private String tenant = null;
	private String apiKey = null;
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
		this.db_name = smssProp.getProperty(DB_NAME);
		this.tenant = smssProp.getProperty(TENANT);
		this.apiKey = smssProp.getProperty(Constants.API_KEY);
		this.className = smssProp.getProperty(CHROMA_CLASSNAME);

		this.collectionID = createCollection(this.className);
	}
	
	private String createCollection(String collectionName) throws Exception {
		try {
			collectionName = collectionName.replaceAll(" ", "_");
			checkSocketStatus();

			StringBuilder script = new StringBuilder();

			script.append("vector_database.create_collection(tenant = '").append(this.tenant)
					.append("', collection_name = '").append(collectionName).append("', database_name = '")
					.append(this.db_name).append("', api_key = '").append(this.apiKey).append("')");

			String value = (String) this.pyTranslator.runScript(script.toString());
			return value;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new Exception("An error creating collection. Error message: " + e.getMessage());
		}
	}
	
	@Override
	protected String getDefaultDistanceMethod() {
		return "cosine";
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

		List<String> ids = new ArrayList<>();
		List<String> documents = new ArrayList<>();
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
			documents.add(row.getSource());
		}

		String jsonMetadata = new Gson().toJson(metadatas);
		String idsJson = new Gson().toJson(ids);
		String documentJson = new Gson().toJson(documents);
		String embeddingsJson = new Gson().toJson(embeddings);

		String response = null;
		try {
			checkSocketStatus();

			StringBuilder addScript = new StringBuilder();

			addScript.append("vector_database.add_document_collection(tenant = '").append(this.tenant)
					.append("', collection_name = '").append(this.className).append("', database_name = '")
					.append(this.db_name).append("', api_key = '").append(this.apiKey).append("', idsJson = '")
					.append(idsJson).append("', embeddingsJson = '").append(embeddingsJson)
					.append("', documentJson = '").append(documentJson).append("', jsonMetadatas = '")
					.append(jsonMetadata).append("')");

			response = (String) this.pyTranslator.runScript(addScript.toString());
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
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
			Map<String, String> sourceProperty = new HashMap<>();

			// replace spaces with _ since thats how
			// readCSV creates Source Property.
			sourceProperty.put("Source", fileName.replaceAll(" ", "_"));
			String jsonWhere = new Gson().toJson(sourceProperty);
			try {
				checkSocketStatus();
				
				StringBuilder deleteScript = new StringBuilder();
				
				deleteScript.append("vector_database.delete_document_collection(tenant = '").append(this.tenant)
						.append("', collection_name = '").append(this.className).append("', database_name = '")
						.append(this.db_name).append("', api_key = '").append(this.apiKey).append("', jsonWhere = '")
						.append(jsonWhere).append("')");
				
				String response = (String) this.pyTranslator.runScript(deleteScript.toString());
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
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
		List<List<Double>> queryEmbeddings = new ArrayList<>();
		queryEmbeddings.add(vector);

		String queryEmbeddingJson = gson.toJson(queryEmbeddings);
		String nearestNeigborResponse = null;
		try {
			checkSocketStatus();
			
			StringBuilder queryScript = new StringBuilder();
			
			queryScript.append("vector_database.search_document_collection(tenant = '").append(this.tenant)
					.append("', collection_name = '").append(this.className).append("', database_name = '")
					.append(this.db_name).append("', api_key = '").append(this.apiKey)
					.append("', queryEmbeddingJson = '").append(queryEmbeddingJson).append("', n_results = ")
					.append(limit).append(")");

			nearestNeigborResponse = (String) this.pyTranslator.runScript(queryScript.toString());
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		String JsonResponse = new String(Base64.getDecoder().decode(nearestNeigborResponse), StandardCharsets.UTF_8);
		Map<String, Object> responseMap = gson.fromJson(JsonResponse, new TypeToken<Map<String, Object>>() {
		}.getType());

		// Retrieve the metadatas list and the distances response
		List<Map<String, Object>> map = new ArrayList<>();
		List<List<Double>> distances = (List<List<Double>>) responseMap.get("distances");
		List<List<Map<String, Object>>> metadatas = (List<List<Map<String, Object>>>) responseMap.get("metadatas");
		if (metadatas != null && !metadatas.isEmpty() && distances != null && !distances.isEmpty()) {
			List<Map<String, Object>> metadata = metadatas.get(0);
			List<Double> distance = distances.get(0);
			List<Double> score = new ArrayList<>();
			for (int i = 0; i < distance.size(); i++) {
				double Score = 1 - distance.get(i);
				score.add(Score);
			}
			for (int i = 0; i < metadata.size(); i++) {
				Map<String, Object> retMap = new LinkedHashMap<>();
				retMap.put("Scores", score.get(i));
				retMap.put("Distance", distance.get(i));
				retMap.putAll(metadata.get(i));
				map.add(retMap);
			}
		}
		return map;
	}
	
	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		//TODO: needs to grab 'Source' from the database
		//TODO: needs to grab 'Source' from the database
		//TODO: needs to grab 'Source' from the database
		//TODO: needs to grab 'Source' from the database
		//TODO: needs to grab 'Source' from the database
		//TODO: needs to grab 'Source' from the database
		
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + DOCUMENTS_FOLDER_NAME);

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