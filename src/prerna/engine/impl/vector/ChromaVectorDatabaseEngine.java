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
import java.util.Comparator;
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
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class ChromaVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(ChromaVectorDatabaseEngine.class);
	
	public static final String CHROMA_CLASSNAME = "CHROMA_COLLECTION_NAME";
	public static final String COLLECTION_ID = "COLLECTION_ID";

	/** SMSS keys for the Chroma v2 tenant/database namespace (optional; sensible defaults applied). */
	public static final String TENANT = "TENANT";
	public static final String DB_NAME = "DB_NAME";

	// v2 REST path fragments: {url}api/v2/tenants/{tenant}/databases/{db}/collections[/{id}{action}]
	private static final String TENANTS = "api/v2/tenants";
	private static final String DATABASES = "/databases";
	private static final String COLLECTIONS = "/collections";
	private static final String DEFAULT_TENANT = "default_tenant";
	private static final String DEFAULT_DATABASE = "default_database";

	private final String API_TOKEN_KEY = "X-Chroma-Token";

	private final String API_ADD = "/add";
	private final String API_DELETE = "/delete";
	private final String API_QUERY = "/query";

	private String url = null;
	private String apiKey = null;
	private String tenant = null;
	private String dbName = null;
	private String className = null;
	private String collectionID = null;

	/** SMSS key: enable hybrid (vector + keyword) search. */
	public static final String USE_HYBRID_SEARCH = "USE_HYBRID_SEARCH";
	/** SMSS key: vector weight in RRF (0.0-1.0); keyword weight is {@code 1 - this}. Default 0.5. */
	public static final String HYBRID_VECTOR_WEIGHT = "HYBRID_VECTOR_WEIGHT";
	/** SMSS key: min BM25 score for keyword ranking to apply; below it, results are vector-only. Default 0.0. */
	public static final String HYBRID_KEYWORD_GATE_THRESHOLD = "HYBRID_KEYWORD_GATE_THRESHOLD";

	private static final double DEFAULT_HYBRID_VECTOR_WEIGHT = 0.5;
	private static final double DEFAULT_HYBRID_KEYWORD_GATE_THRESHOLD = 0.0;
	private static final int RRF_K = 60;
	// BM25 tuning constants (Lucene defaults)
	private static final double BM25_K1 = 1.2;
	private static final double BM25_B = 0.75;
	// transient key holding each candidate's vector distance during ranking; stripped before return
	private static final String DISTANCE_KEY = "_distance";

	private boolean useHybridSearch = false;
	private double hybridVectorWeight = DEFAULT_HYBRID_VECTOR_WEIGHT;
	private double hybridKeywordGateThreshold = DEFAULT_HYBRID_KEYWORD_GATE_THRESHOLD;

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
		this.dbName = smssProp.getProperty(DB_NAME);

		this.useHybridSearch = Boolean.parseBoolean(this.smssProp.getProperty(USE_HYBRID_SEARCH, "false"));

		if (this.smssProp.containsKey(HYBRID_VECTOR_WEIGHT)) {
			try {
				double parsedVectorWeight = Double.parseDouble(this.smssProp.getProperty(HYBRID_VECTOR_WEIGHT));
				if (parsedVectorWeight >= 0.0 && parsedVectorWeight <= 1.0) {
					this.hybridVectorWeight = parsedVectorWeight;
				} else {
					classLogger.warn("HYBRID_VECTOR_WEIGHT '{}' must be between 0.0 and 1.0 (inclusive); defaulting to {}",
							parsedVectorWeight, DEFAULT_HYBRID_VECTOR_WEIGHT);
				}
			} catch (NumberFormatException e) {
				classLogger.warn("HYBRID_VECTOR_WEIGHT '{}' is not a valid number; defaulting to {}",
						this.smssProp.getProperty(HYBRID_VECTOR_WEIGHT), DEFAULT_HYBRID_VECTOR_WEIGHT, e);
			}
		}

		if (this.smssProp.containsKey(HYBRID_KEYWORD_GATE_THRESHOLD)) {
			try {
				double parsedGateThreshold = Double.parseDouble(this.smssProp.getProperty(HYBRID_KEYWORD_GATE_THRESHOLD));
				if (parsedGateThreshold >= 0.0) {
					this.hybridKeywordGateThreshold = parsedGateThreshold;
				} else {
					classLogger.warn("HYBRID_KEYWORD_GATE_THRESHOLD '{}' must be >= 0; defaulting to {}",
							parsedGateThreshold, DEFAULT_HYBRID_KEYWORD_GATE_THRESHOLD);
				}
			} catch (NumberFormatException e) {
				classLogger.warn("HYBRID_KEYWORD_GATE_THRESHOLD '{}' is not a valid number; defaulting to {}",
						this.smssProp.getProperty(HYBRID_KEYWORD_GATE_THRESHOLD), DEFAULT_HYBRID_KEYWORD_GATE_THRESHOLD, e);
			}
		}

		// create or fetch collection Id from the Chroma DB
		this.collectionID = createCollection(this.className);
	}

	/** Chroma v2 collections endpoint: {@code {url}api/v2/tenants/{tenant}/databases/{db}/collections}. */
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

	/** Chroma v2 collection action endpoint, e.g. {@code .../collections/{id}/query}. */
	public static String collection(String url, String tenant, String database, String collectionId, String action) {
		return new StringBuilder(collections(url, tenant, database)).append("/").append(collectionId).append(action)
				.toString();
	}

	/** Request headers for a Chroma call, or {@code null} when no API key is configured. */
	private Map<String, String> buildHeaders() {
		if (this.apiKey == null || this.apiKey.isEmpty()) {
			return null;
		}
		Map<String, String> headers = new HashMap<>();
		headers.put(API_TOKEN_KEY, this.apiKey);
		headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		return headers;
	}

	/** Return the id of the named collection, creating it if it does not exist. */
	private String createCollection(String collectionName) {
		collectionName = collectionName.replaceAll(" ", "_");
		Gson gson = new Gson();
		String collectionsUrl = collections(this.url, this.tenant, this.dbName);
		Map<String, String> headersMap = buildHeaders();

		String nearestNeigborResponse = null;
		try {
			nearestNeigborResponse = HttpHelperUtility.getRequest(collectionsUrl, headersMap, null, null, null);
		} catch(Exception e) {
			classLogger.error("Unable to create connection");
			throw new SemossPixelException("Unable to create connection");
		}
		
		List<Map<String, Object>> responseListMap = gson.fromJson(nearestNeigborResponse, new TypeToken<List<Map<String, Object>>>() {}.getType());
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
		nearestNeigborResponse = HttpHelperUtility.postRequestStringBody(collectionsUrl, headersMap, body, ContentType.APPLICATION_JSON, null, null, null);
		Map<String, Object> responseMap = gson.fromJson(nearestNeigborResponse, new TypeToken<Map<String, Object>>() {}.getType());

		return (String) responseMap.get("id");
	}
	
	@Override
	protected String getDefaultDistanceMethod() {
		return "cosine";
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

		String response = HttpHelperUtility.postRequestStringBody(
				collection(this.url, this.tenant, this.dbName, this.collectionID, API_ADD),
				buildHeaders(), body, ContentType.APPLICATION_JSON, null, null, null);
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		//TODO: let us add validation by looking at the response
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
    	for(String document : fileNames) {
			String documentName = FilenameUtils.getName(document);
			File f = new File(document);
			if(f.exists() && f.getName().endsWith(".csv")) {
				sourceNames.addAll(VectorDatabaseCSVTable.pullSourceColumn(f));
			} else {
				sourceNames.add(documentName);
			}
    	}
		
		List<String> filesToRemoveFromCloud = new ArrayList<String>();

		// need to get the source names and then delete it based on the names
		for (int fileIndex = 0; fileIndex < sourceNames.size(); fileIndex++) {
			String fileName = fileNames.get(fileIndex);

			// delete chunks matching the Source metadata (v2 delete API)
			Map<String, Object> fileNamesForDelete = new HashMap<>();
			Map<String, String> sourceProperty = new HashMap<>();

			// replace spaces with _ since thats how
			// readCSV creates Source Property.
			sourceProperty.put("Source", fileName.replaceAll(" ", "_")); 
																			
			fileNamesForDelete.put("where", sourceProperty);

			Gson gson = new Gson();
			String body = gson.toJson(fileNamesForDelete);

			String response = HttpHelperUtility.postRequestStringBody(
					collection(this.url, this.tenant, this.dbName, this.collectionID, API_DELETE),
					buildHeaders(), body, ContentType.APPLICATION_JSON, null, null, null);

			// Chroma returns {"deleted": N}; log it and warn when nothing matched
			String source = sourceProperty.get("Source");
			ChromaDeleteResponse deleteResponse = gson.fromJson(response, ChromaDeleteResponse.class);
			int deleted = (deleteResponse != null && deleteResponse.deleted != null) ? deleteResponse.deleted : 0;
			if (deleted > 0) {
				classLogger.info("Removed {} record(s) from Chroma collection '{}' for source '{}'", deleted,
						this.className, source);
			} else {
				classLogger.warn("No records matched source '{}' in Chroma collection '{}' during delete", source,
						this.className);
			}

			String documentName = Paths.get(fileName).getFileName().toString();
			// remove the physical documents
			File documentFile = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + "documents", documentName);
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

		List<Double> vector = getEmbeddingsDouble(searchStatement, insight);
		Map<String, Object> where = buildWhere(parameters);

		if (this.useHybridSearch) {
			return executeHybridRrfSearch(searchStatement, vector, limit.intValue(), where);
		}

		// vector-only: return each candidate with its vector Score + raw Distance, metadata after
		List<Map<String, Object>> candidates = queryVectors(vector, limit.intValue(), where);
		List<Map<String, Object>> results = new ArrayList<>(candidates.size());
		for (Map<String, Object> candidate : candidates) {
			Object distance = candidate.remove(DISTANCE_KEY);
			Map<String, Object> row = new LinkedHashMap<>();
			if (distance instanceof Number) {
				double d = ((Number) distance).doubleValue();
				row.put("Score", toScore(d));
				row.put("Distance", d);
			}
			row.putAll(candidate);
			results.add(row);
		}
		return results;
	}

	/** Higher-is-better similarity score from a Chroma distance: {@code 1 - d} for cosine, else {@code 1/(1+d)}. */
	private double toScore(double distance) {
		if (this.distanceMethod != null && this.distanceMethod.toLowerCase().contains("cosine")) {
			return 1.0 - distance;
		}
		return 1.0 / (1.0 + distance);
	}

	/** Build a Chroma {@code where} clause from the {@code filters}/{@code metaFilters} params (AND-combined), or {@code null}. */
	private Map<String, Object> buildWhere(Map<String, Object> parameters) {
		if (parameters == null) {
			return null;
		}
		List<IQueryFilter> allFilters = new ArrayList<>();
		collectFilters(parameters.get(AbstractVectorDatabaseEngine.FILTERS_KEY), allFilters);
		collectFilters(parameters.get(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY), allFilters);
		return ChromaVectorQueryFilterTranslationHelper.toWhere(allFilters);
	}

	/** Append any {@link IQueryFilter}s in {@code value} (if it is a list) to {@code target}. */
	private void collectFilters(Object value, List<IQueryFilter> target) {
		if (!(value instanceof List)) {
			return;
		}
		for (Object element : (List<?>) value) {
			if (element instanceof IQueryFilter) {
				target.add((IQueryFilter) element);
			}
		}
	}

	/**
	 * Re-rank a vector candidate pool by fusing vector similarity with an in-app BM25 keyword score
	 * via weighted RRF: {@code score = vw/(k+vRank) + kw/(k+kRank)}, k=60. Keyword ranking is skipped
	 * when no candidate matches the query. Vector-first: a keyword-only match outside the pool is not
	 * surfaced (same trade-off as the PGVector hybrid).
	 */
	private List<Map<String, Object>> executeHybridRrfSearch(String searchStatement, List<Double> vector, int limit,
			Map<String, Object> where) {
		int candidateLimit = Math.max(limit * 10, 100);

		List<Map<String, Object>> candidates = queryVectors(vector, candidateLimit, where);
		if (candidates.isEmpty()) {
			return candidates;
		}
		int n = candidates.size();

		double[] keywordScores = bm25Scores(searchStatement, candidates);

		// rank by vector distance (asc) and keyword score (desc); rank 0 = best
		List<Integer> byVector = sortedIndices(n,
				(a, b) -> Double.compare(distanceOf(candidates.get(a)), distanceOf(candidates.get(b))));
		List<Integer> byKeyword = sortedIndices(n, (a, b) -> Double.compare(keywordScores[b], keywordScores[a]));
		int[] vectorRank = toRanks(byVector);
		int[] keywordRank = toRanks(byKeyword);

		// skip keyword signal when nothing matched the query text
		double maxKeywordScore = keywordScores[byKeyword.get(0)];
		boolean useKeyword = maxKeywordScore > this.hybridKeywordGateThreshold;

		double vectorWeight = this.hybridVectorWeight;
		double keywordWeight = 1.0 - vectorWeight;
		classLogger.debug("Chroma hybrid RRF for query '{}': candidates={}, vectorWeight={}, keywordWeight={}, useKeyword={}, maxKeywordScore={}",
				searchStatement, n, vectorWeight, keywordWeight, useKeyword, maxKeywordScore);

		double[] rrfScores = new double[n];
		for (int i = 0; i < n; i++) {
			rrfScores[i] = vectorWeight / (RRF_K + vectorRank[i] + 1.0);
			if (useKeyword) {
				rrfScores[i] += keywordWeight / (RRF_K + keywordRank[i] + 1.0);
			}
		}

		// top-N by fused RRF score
		List<Integer> ranked = sortedIndices(n, (a, b) -> Double.compare(rrfScores[b], rrfScores[a]));
		int resultCount = Math.min(limit, n);
		List<Map<String, Object>> results = new ArrayList<>(resultCount);
		for (int i = 0; i < resultCount; i++) {
			Map<String, Object> row = candidates.get(ranked.get(i));
			row.put("Score", rrfScores[ranked.get(i)]);
			row.remove(DISTANCE_KEY);
			results.add(row);
		}
		return results;
	}

	/** Indices {@code [0, n)} sorted by {@code order} (position 0 = best). */
	private static List<Integer> sortedIndices(int n, Comparator<Integer> order) {
		List<Integer> indices = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			indices.add(i);
		}
		indices.sort(order);
		return indices;
	}

	/** Invert a best-first index ordering into a per-index rank array (rank 0 = best). */
	private static int[] toRanks(List<Integer> ordered) {
		int[] rank = new int[ordered.size()];
		for (int position = 0; position < ordered.size(); position++) {
			rank[ordered.get(position)] = position;
		}
		return rank;
	}

	/**
	 * Run a vector query and return the first result set's rows, each tagged with its vector distance
	 * under {@link #DISTANCE_KEY}. Shared by the vector-only and hybrid paths. Empty if no hits.
	 */
	private List<Map<String, Object>> queryVectors(List<Double> vector, int nResults, Map<String, Object> where) {
		Gson gson = new Gson();
		Map<String, Object> query = new HashMap<>();
		List<List<Double>> queryEmbeddings = new ArrayList<>();
		// nest the embedding inside a list as the API expects
		queryEmbeddings.add(vector);
		query.put("n_results", nResults);
		query.put("query_embeddings", queryEmbeddings);
		if (where != null) {
			query.put("where", where);
		}

		String responseBody = HttpHelperUtility.postRequestStringBody(
				collection(this.url, this.tenant, this.dbName, this.collectionID, API_QUERY),
				buildHeaders(), gson.toJson(query), ContentType.APPLICATION_JSON, null, null, null);

		ChromaQueryResponse parsed = gson.fromJson(responseBody, ChromaQueryResponse.class);
		if (parsed == null) {
			throw new SemossPixelException("Failed to query Chroma collection.");
		}
		if (parsed.metadatas == null || parsed.metadatas.isEmpty() || parsed.metadatas.get(0) == null) {
			return new ArrayList<>();
		}

		List<Map<String, Object>> rows = parsed.metadatas.get(0);
		List<Double> distances = (parsed.distances != null && !parsed.distances.isEmpty()) ? parsed.distances.get(0)
				: null;
		for (int i = 0; i < rows.size(); i++) {
			Double distance = (distances != null && i < distances.size()) ? distances.get(i) : null;
			rows.get(i).put(DISTANCE_KEY, distance);
		}
		return rows;
	}

	/** Vector distance tagged on a candidate row, or {@link Double#MAX_VALUE} if missing (sorts last). */
	private double distanceOf(Map<String, Object> row) {
		Object distance = row.get(DISTANCE_KEY);
		return (distance instanceof Number) ? ((Number) distance).doubleValue() : Double.MAX_VALUE;
	}

	/**
	 * BM25 keyword score per candidate (index-aligned), using corpus statistics from the candidate
	 * pool itself. A candidate matching no query term scores 0.
	 */
	private double[] bm25Scores(String query, List<Map<String, Object>> candidates) {
		int n = candidates.size();
		double[] scores = new double[n];

		List<String> queryTerms = tokenize(query);
		if (queryTerms.isEmpty() || n == 0) {
			return scores;
		}

		// term frequencies per document + document lengths
		List<Map<String, Integer>> docTermFreqs = new ArrayList<>(n);
		int[] docLengths = new int[n];
		double totalLength = 0.0;
		for (int i = 0; i < n; i++) {
			Object content = candidates.get(i).get(VectorDatabaseCSVTable.CONTENT);
			List<String> tokens = tokenize(content == null ? "" : content.toString());
			Map<String, Integer> termFreq = new HashMap<>();
			for (String token : tokens) {
				termFreq.put(token, termFreq.getOrDefault(token, 0) + 1);
			}
			docTermFreqs.add(termFreq);
			docLengths[i] = tokens.size();
			totalLength += tokens.size();
		}
		double avgDocLength = totalLength / n;
		if (avgDocLength <= 0.0) {
			return scores;
		}

		// document frequency per query term
		Map<String, Integer> docFreq = new HashMap<>();
		for (String term : queryTerms) {
			if (docFreq.containsKey(term)) {
				continue;
			}
			int df = 0;
			for (Map<String, Integer> termFreq : docTermFreqs) {
				if (termFreq.containsKey(term)) {
					df++;
				}
			}
			docFreq.put(term, df);
		}

		// BM25 score per document
		for (int i = 0; i < n; i++) {
			Map<String, Integer> termFreq = docTermFreqs.get(i);
			double score = 0.0;
			for (Map.Entry<String, Integer> entry : docFreq.entrySet()) {
				int f = termFreq.getOrDefault(entry.getKey(), 0);
				if (f == 0) {
					continue;
				}
				int df = entry.getValue();
				double idf = Math.log(1.0 + (n - df + 0.5) / (df + 0.5));
				double denom = f + BM25_K1 * (1.0 - BM25_B + BM25_B * docLengths[i] / avgDocLength);
				score += idf * (f * (BM25_K1 + 1.0)) / denom;
			}
			scores[i] = score;
		}
		return scores;
	}

	/** Lower-case and split into alphanumeric tokens (no external tokenizer/stemmer). */
	private List<String> tokenize(String text) {
		List<String> tokens = new ArrayList<>();
		if (text == null || text.isEmpty()) {
			return tokens;
		}
		for (String token : text.toLowerCase().split("[^a-z0-9]+")) {
			if (!token.isEmpty()) {
				tokens.add(token);
			}
		}
		return tokens;
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

	/** Typed Chroma query response — lets Gson resolve the nested generics without unchecked casts. */
	private static class ChromaQueryResponse {
		private List<List<Map<String, Object>>> metadatas;
		private List<List<Double>> distances;
	}

	/** Typed Chroma delete response: {@code {"deleted": N}}. */
	private static class ChromaDeleteResponse {
		private Integer deleted;
	}

}