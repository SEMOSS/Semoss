/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

	private static final Logger classLogger = LogManager.getLogger(PineConeVectorDatabaseEngine.class);

	private final String NAMESPACE = "NAMESPACE";
	private final String API_UPSERT = "/vectors/upsert";
	private final String API_DELETE = "/vectors/delete";
	private final String API_QUERY = "/query";
	private final String API_KY = "Api-Key";
	private final String LIST_QUERY = "/vectors/list?namespace=";
	private final String FETCH_QUERY = "/vectors/fetch?namespace=";
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
	protected String getDefaultDistanceMethod() {
		// this is stored on the index itself
		// since we dont create it for this engine - this doesn't matter ...
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

		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		// send all the strings to embed in one shot
		try {
			vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Error occurred creating the embeddings for the generated chunks. Detailed error message = "
							+ e.getMessage(),
					e);
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
		Map<String, Integer> fileRecordCountMap = new HashMap<>();
		for (int rowIndex = 0; rowIndex < vectorCsvTable.rows.size(); rowIndex++) {
			VectorDatabaseCSVRow row = vectorCsvTable.getRows().get(rowIndex);
			fileRecordCountMap.put(row.getSource(), fileRecordCountMap.getOrDefault(row.getSource(), 0) + 1);
			JsonObject metadataJson = new JsonObject();
			metadataJson.addProperty(VectorDatabaseCSVTable.SOURCE, row.getSource());
			metadataJson.addProperty(VectorDatabaseCSVTable.MODALITY, row.getModality());
			metadataJson.addProperty(VectorDatabaseCSVTable.DIVIDER, row.getDivider());
			metadataJson.addProperty(VectorDatabaseCSVTable.PART, row.getPart());
			metadataJson.addProperty(VectorDatabaseCSVTable.TOKENS, row.getTokens());
			metadataJson.addProperty(VectorDatabaseCSVTable.CONTENT, row.getContent());

			List<Double> vector = getEmbeddingsDouble(row.getContent(), insight);
			if (row.getSource().equals(previousFileName)) {
				fileCounter = 0;
			}

			JsonObject thisChunkJson = new JsonObject();
			thisChunkJson.addProperty("id", row.getSource().replaceAll(" ", "_") + "-" + fileCounter++);
			JsonArray thisEmbeddingVector = new JsonArray();
			for (Double d : vector) {
				thisEmbeddingVector.add(d);
			}
			thisChunkJson.add("values", thisEmbeddingVector);
			thisChunkJson.add("metadata", metadataJson);
			vectors.add(thisChunkJson);
		}

		JsonObject vectorsMap = new JsonObject();
		vectorsMap.addProperty("namespace", this.defaultNamespace);
		vectorsMap.add("vectors", vectors);
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		long insertedCount = 0;
		try {

			String response = HttpHelperUtility.postRequestStringBody(url, headersMap, vectorsMap.toString(),
					ContentType.APPLICATION_JSON, null, null, null);
			JsonObject json = JsonParser.parseString(response).getAsJsonObject();
			if (json.has("upsertedCount")) {
				insertedCount = json.get("upsertedCount").getAsLong();
			} else {
				classLogger.warn("Pinecone upsert response did not contain 'upsertedCount': {}", response);
			}
			for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
				String file = entry.getKey();
				int totalRecords = entry.getValue();

				long inserted = 0;
				long failed = 0;
				String status;

				if (insertedCount == totalRecords) {
					inserted = totalRecords;
					failed = 0;
					status = "SUCCESS";
					insertedCount -= totalRecords;
				} else if (insertedCount > 0 && insertedCount < totalRecords) {
					inserted = insertedCount;
					failed = totalRecords - insertedCount;
					status = "PARTIAL";
					insertedCount = 0;
				} else {
					inserted = 0;
					failed = totalRecords;
					status = "FAILED";
				}
				fileStatusList.add(new FileEmbeddingStatus(file, status, inserted, failed, totalRecords));
			}

			return fileStatusList;
		} catch (Exception e) {
			for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
				fileStatusList
						.add(new FileEmbeddingStatus(entry.getKey(), "FAILED", 0, entry.getValue(), entry.getValue()));
			}
			return fileStatusList;
		}
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

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		// need to get the source names and then delete it based on the names
		for (int fileIndex = 0; fileIndex < sourceNames.size(); fileIndex++) {
			String fileName = fileNames.get(fileIndex);

			boolean firstExecution = true;
			String paginationToken = null;
			while (firstExecution || paginationToken != null) {
				String listVectorsUrl = this.hostname + LIST_QUERY + this.defaultNamespace;
				if (paginationToken != null) {
					listVectorsUrl += PAGINATION_TOKEN + paginationToken;
				} else {
					listVectorsUrl += PREFIX + fileName.replaceAll(" ", "_") + HASH;
				}

				String idListResponse = HttpHelperUtility.getRequest(listVectorsUrl, headersMap, null, null, null);
				Map<String, Object> responseMap = GSON.fromJson(idListResponse, new TypeToken<Map<String, Object>>() {
				}.getType());

				List<Map<String, String>> vectors = (List<Map<String, String>>) responseMap.get("vectors");
				executeDelete(this.hostname + API_DELETE, headersMap, vectors);

				// we can only pull 100 at a time
				// we need to check if there is pagination to keep going
				Map<String, String> paginationMap = (Map<String, String>) responseMap.get("pagination");
				if (paginationMap != null && !paginationMap.isEmpty()) {
					paginationToken = paginationMap.get("next");
				} else {
					paginationToken = null;
				}

				firstExecution = false;
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
				classLogger.error("Failed to delete document file '{}'", documentFile.getAbsolutePath(), e);
			}
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId,
					this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
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
		if (listReturnVector == null || listReturnVector.isEmpty()) {
			return;
		}
		JsonArray idsJsonArray = new JsonArray();
		for (Map<String, String> v : listReturnVector) {
			idsJsonArray.add(v.get("id"));
		}

		JsonObject deleteJson = new JsonObject();
		deleteJson.add("ids", idsJsonArray);
		deleteJson.addProperty("namespace", this.defaultNamespace);
		HttpHelperUtility.postRequestStringBody(url, headersMap, deleteJson.toString(), ContentType.APPLICATION_JSON,
				null, null, null);
	}

	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
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
		String nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(url, headersMap, queryJson.toString(),
				ContentType.APPLICATION_JSON, null, null, null);

		Map<String, Object> responseMap = GSON.fromJson(nearestNeigborResponse, new TypeToken<Map<String, Object>>() {
		}.getType());
		List<Map<String, Object>> matches = (List<Map<String, Object>>) responseMap.get("matches");

		List<Map<String, Object>> retOut = new ArrayList<>();

		for (int i = 0; i < matches.size(); i++) {
			Map<String, Object> thisMatch = matches.get(i);
			Map<String, Object> metadataMap = (Map<String, Object>) thisMatch.get("metadata");

			Map<String, Object> resultMap = new HashMap<>();
			resultMap.put("Id", matches.get(i).get("id"));
			resultMap.put("Score", matches.get(i).get("score"));
			resultMap.put(VectorDatabaseCSVTable.SOURCE, metadataMap.get(VectorDatabaseCSVTable.SOURCE));
			resultMap.put(VectorDatabaseCSVTable.CONTENT, metadataMap.get(VectorDatabaseCSVTable.CONTENT));
			resultMap.put(VectorDatabaseCSVTable.DIVIDER, metadataMap.get(VectorDatabaseCSVTable.DIVIDER));
			resultMap.put(VectorDatabaseCSVTable.MODALITY, metadataMap.get(VectorDatabaseCSVTable.MODALITY));
			resultMap.put(VectorDatabaseCSVTable.PART, metadataMap.get(VectorDatabaseCSVTable.PART));
			resultMap.put(VectorDatabaseCSVTable.TOKENS, metadataMap.get(VectorDatabaseCSVTable.TOKENS));
			retOut.add(resultMap);
		}

		return retOut;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		/**
		 * Pinecone's API is not useful
		 * https://docs.pinecone.io/reference/api/2025-01/data-plane/list
		 * 
		 * dont have a way to fetch results w/o first listing the index's and after
		 * listing the index's the fetch cant limit the return so will also get back the
		 * vectors
		 * 
		 */

		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		Set<String> sources = new TreeSet<>();

		boolean firstExecution = true;
		String paginationToken = null;
		WHILE_LOOP: while (firstExecution || paginationToken != null) {
			String listVectorsUrl = this.hostname + LIST_QUERY + this.defaultNamespace;
			if (paginationToken != null) {
				listVectorsUrl += PAGINATION_TOKEN + paginationToken;
			}

			String idListResponse = HttpHelperUtility.getRequest(listVectorsUrl, headersMap, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(idListResponse, new TypeToken<Map<String, Object>>() {
			}.getType());

			List<Map<String, String>> vectors = (List<Map<String, String>>) responseMap.get("vectors");
			if (vectors.isEmpty()) {
				break WHILE_LOOP;
			}
			StringBuilder ids = new StringBuilder();
			for (int i = 0; i < vectors.size(); i++) {
				Map<String, String> idMap = vectors.get(i);
				ids.append("&ids=").append(idMap.get("id"));

				if ((i + 1) % 20 == 0) {
					sources.addAll(fetchUniqueSourceValues(ids));
					ids = new StringBuilder();
				}
			}

			if (ids.length() != 0) {
				sources.addAll(fetchUniqueSourceValues(ids));
				ids = new StringBuilder();
			}

			// we can only pull 100 at a time
			// we need to check if there is pagination to keep going
			Map<String, String> paginationMap = (Map<String, String>) responseMap.get("pagination");
			if (paginationMap != null && !paginationMap.isEmpty()) {
				paginationToken = paginationMap.get("next");
			} else {
				paginationToken = null;
			}

			firstExecution = false;
		}

		List<Map<String, Object>> fileList = new ArrayList<>();
		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		if (documentsDir.exists() && documentsDir.isDirectory()) {
			for (String fileName : sources) {
				Map<String, Object> fileInfo = new HashMap<>();
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
				fileList.add(fileInfo);
			}
		}

		return fileList;
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		/**
		 * Pinecone's API is not useful
		 * https://docs.pinecone.io/reference/api/2025-01/data-plane/list
		 * 
		 * don't have a way to fetch results w/o first listing the index's and after
		 * listing the index's the fetch can't limit the return so will also get back
		 * the vectors
		 */

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		List<Map<String, Object>> allRecords = new ArrayList<>();

		boolean firstExecution = true;
		String paginationToken = null;
		WHILE_LOOP: while (firstExecution || paginationToken != null) {
			String listVectorsUrl = this.hostname + LIST_QUERY + this.defaultNamespace;
			if (paginationToken != null) {
				listVectorsUrl += PAGINATION_TOKEN + paginationToken;
			}

			String idListResponse = HttpHelperUtility.getRequest(listVectorsUrl, headersMap, null, null, null);
			Map<String, Object> responseMap = GSON.fromJson(idListResponse, new TypeToken<Map<String, Object>>() {
			}.getType());

			List<Map<String, String>> vectors = (List<Map<String, String>>) responseMap.get("vectors");
			if (vectors.isEmpty()) {
				break WHILE_LOOP;
			}
			StringBuilder ids = new StringBuilder();
			for (int i = 0; i < vectors.size(); i++) {
				Map<String, String> idMap = vectors.get(i);
				ids.append("&ids=").append(idMap.get("id"));

				if ((i + 1) % 20 == 0) {
					allRecords.addAll(fetchAllValues(ids));
					ids = new StringBuilder();
				}
			}

			if (ids.length() != 0) {
				allRecords.addAll(fetchAllValues(ids));
				ids = new StringBuilder();
			}

			// we can only pull 100 at a time
			// we need to check if there is pagination to keep going
			Map<String, String> paginationMap = (Map<String, String>) responseMap.get("pagination");
			if (paginationMap != null && !paginationMap.isEmpty()) {
				paginationToken = paginationMap.get("next");
			} else {
				paginationToken = null;
			}

			firstExecution = false;
		}

		return allRecords;
	}

	/**
	 * 
	 * @param ids
	 * @return
	 */
	private List<Map<String, Object>> fetchAllValues(StringBuilder ids) {
		List<Map<String, Object>> records = new ArrayList<>();

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String fetchUrl = this.hostname + FETCH_QUERY + this.defaultNamespace + ids.toString();
		String fetchResponse = HttpHelperUtility.getRequest(fetchUrl, headersMap, null, null, null);
		JsonObject pineconeHorribleJson = JsonParser.parseString(fetchResponse).getAsJsonObject();
		JsonObject vectorsJson = pineconeHorribleJson.get("vectors").getAsJsonObject();
		for (String uid : vectorsJson.keySet()) {
			JsonObject record = vectorsJson.get(uid).getAsJsonObject();
			JsonObject metadata = record.get("metadata").getAsJsonObject();

			Map<String, Object> recordMap = new HashMap<>();
			recordMap.put(VectorDatabaseCSVTable.SOURCE, metadata.get(VectorDatabaseCSVTable.SOURCE).getAsString());
			recordMap.put(VectorDatabaseCSVTable.MODALITY, metadata.get(VectorDatabaseCSVTable.MODALITY).getAsString());
			recordMap.put(VectorDatabaseCSVTable.DIVIDER, metadata.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
			recordMap.put(VectorDatabaseCSVTable.PART, metadata.get(VectorDatabaseCSVTable.PART).getAsString());
			recordMap.put(VectorDatabaseCSVTable.TOKENS, metadata.get(VectorDatabaseCSVTable.TOKENS).getAsInt());
			recordMap.put(VectorDatabaseCSVTable.CONTENT, metadata.get(VectorDatabaseCSVTable.CONTENT).getAsString());
			records.add(recordMap);
		}

		return records;
	}

	/**
	 * 
	 * @param ids
	 * @return
	 */
	private Set<String> fetchUniqueSourceValues(StringBuilder ids) {
		Set<String> uniqueSources = new HashSet<>();

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(API_KY, this.apiKey);
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

		String fetchUrl = this.hostname + FETCH_QUERY + this.defaultNamespace + ids.toString();
		String fetchResponse = HttpHelperUtility.getRequest(fetchUrl, headersMap, null, null, null);
		JsonObject pineconeHorribleJson = JsonParser.parseString(fetchResponse).getAsJsonObject();
		JsonObject vectorsJson = pineconeHorribleJson.get("vectors").getAsJsonObject();
		for (String uid : vectorsJson.keySet()) {
			JsonObject record = vectorsJson.get(uid).getAsJsonObject();
			JsonObject metadata = record.get("metadata").getAsJsonObject();

			uniqueSources.add(metadata.get(VectorDatabaseCSVTable.SOURCE).getAsString());
		}

		return uniqueSources;
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.PINECONE;
	}

}