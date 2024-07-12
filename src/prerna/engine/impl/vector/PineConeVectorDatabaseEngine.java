package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
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
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class PineConeVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LoggerFactory.getLogger(PineConeVectorDatabaseEngine.class);

	private static final String HASH = "#";
	private static final String PREFIX = "&prefix=";
	
	private static final String LIST_QUERY = "/vectors/list?namespace=";
	private static final String API_QUERY = "/query";
	private static final String API_UPSERT = "/vectors/upsert";
	private static final String API_DELETE = "/vectors/delete";
	
	private static final String API_KY= "Api-Key";
	private static final String NAMESPACE = "NAMESPACE";

	private String apiKey = null;
	private String defaultNamespace = null;
	private String hostname = null;

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.PINECONE;
	}

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
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		String url = this.hostname + API_DELETE;
		String indexClass = this.defaultIndexClass;
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		List<String> filesToRemoveFromCloud = new ArrayList<String>();

		// need to get the source names and then delete it based on the names
		for (int fileIndex = 0; fileIndex < fileNames.size(); fileIndex++) {
			String fileName = fileNames.get(fileIndex);

			String urlforIDList = this.hostname + LIST_QUERY + this.defaultNamespace + PREFIX
					+ fileName.replaceAll(" ", "_") + HASH;
			Map<String, String> headersMap = new HashMap<>();
			headersMap.put(API_KY, this.apiKey);
			headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

			String idListResponse = HttpHelperUtility.getRequest(urlforIDList, headersMap, null, null, null);
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();
			Map<String, Object> responseMap = gson.fromJson(idListResponse, type);
			System.out.println(responseMap);

			List<Map<String, String>> vectors = (List<Map<String, String>>) responseMap.get("vectors");
			List<String> idsToBeDeleted = new ArrayList<String>();
			for (Map<String, String> v : vectors) {
				idsToBeDeleted.add(v.get("id"));
			}

			Map<String, Object> fileNamesForDelete = new HashMap<>();
			fileNamesForDelete.put("ids", idsToBeDeleted);
			fileNamesForDelete.put("namespace", this.defaultNamespace);

			String body = gson.toJson(fileNamesForDelete);
			System.out.println(body);

			HttpHelperUtility.postRequestStringBody(url, headersMap, body, ContentType.APPLICATION_JSON, null, null,
					null);

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
	public List<Map<String, Object>> nearestNeighbor(String searchStatement, Number limit, Map<String, Object> parameters) {
		String url = this.hostname + API_QUERY;
		List<Map<String, Object>> retOut = new ArrayList<Map<String, Object>>();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();

		if (limit == null) {
			limit = 3;
		}

		Insight insight = getInsight(parameters.get(INSIGHT));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		List<Double> vector = getEmbeddingsDouble(searchStatement, insight);

		Map<String, Object> query = new HashMap<>();
		List<Double> queryEmbeddings = new ArrayList<>();
		for (int i = 0; i < vector.size(); i++) {
			queryEmbeddings.add(vector.get(i)); // this is done to put a list of embeddings inside another list
												// otherwise the API throws error.
		}
		query.put("topK", limit);
		query.put("includeMetadata", true);
		query.put("includeValues", true);
		query.put("namespace", this.defaultNamespace);
		query.put("vector", queryEmbeddings);

		String body = gson.toJson(query);

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, headersMap, body,
				ContentType.APPLICATION_JSON, null, null, null);

		Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, new TypeToken<Map<String, Object>>() {}.getType());

		retOut.add(responseMap);

		return retOut;
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

		// Sample URL:
		// "https://docs-quickstart-index3-fiarr5p.svc.aped-4627-b74a.pinecone.io/vectors/upsert";
		String url = this.hostname + API_UPSERT;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put("Api-Key", this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		Map<String, Object> vectorsMap = new HashMap<>();
		List<Map<String, Object>> vectors = new ArrayList<>();
		String namespace = this.defaultNamespace;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.NAMESPACE.getKey())) {
			namespace = (String) parameters.get(VectorDatabaseParamOptionsEnum.NAMESPACE.getKey());
		}
		vectorsMap.put("namespace", namespace);

		// loop through and make the giant json
		int fileCounter = 0;
		String previousFileName = null;

		for (int rowIndex = 0; rowIndex < vectorCsvTable.rows.size(); rowIndex++) {
			VectorDatabaseCSVRow row = vectorCsvTable.getRows().get(rowIndex);
			Map<String, Object> properties = new HashMap<>();
			properties.put("Source", row.getSource());
			properties.put("Modality", row.getModality());
			properties.put("Divider", row.getDivider());
			properties.put("Part", row.getPart());
			properties.put("Tokens", row.getTokens());
			properties.put("Content", row.getContent());

			List<Double> vector = getEmbeddingsDouble(row.getContent(), insight);
			if (row.getSource().equals(previousFileName)) {
				fileCounter = 0;
			}

			Map<String, Object> thisMap = new HashMap<>();
			thisMap.put("id", row.getSource().replaceAll(" ", "_") + "-" + fileCounter++);
			thisMap.put("values", vector);
			thisMap.put("metadata", properties);
			vectors.add(thisMap);
		}

		vectorsMap.put("vectors", vectors);
		String body = new Gson().toJson(vectorsMap);
		System.out.println(body);

		HttpHelperUtility.postRequestStringBody(url, headersMap, body, ContentType.APPLICATION_JSON, null, null, null);
	}
}