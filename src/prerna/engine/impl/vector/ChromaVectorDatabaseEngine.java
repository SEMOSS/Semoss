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
	private final String API_GET = "/get";

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
	// over-fetch the hybrid candidate pool: pull more than the caller's limit from each signal
	// (vector + keyword) so RRF has a meaningful pool to fuse before truncating back to limit
	private static final int HYBRID_CANDIDATE_MULTIPLIER = 10;
	private static final int HYBRID_MIN_CANDIDATES = 100;
	// the in-memory BM25 index loads the whole collection on open; warn past this size (see PR notes on scale)
	private static final int BM25_LARGE_CORPUS_WARN = 100_000;
	// transient key holding each candidate's vector distance during ranking; stripped before return
	private static final String DISTANCE_KEY = "_distance";

	private boolean useHybridSearch = false;
	private double hybridVectorWeight = DEFAULT_HYBRID_VECTOR_WEIGHT;
	private double hybridKeywordGateThreshold = DEFAULT_HYBRID_KEYWORD_GATE_THRESHOLD;
	// in-memory keyword index (rebuilt from Chroma on open); used only when hybrid search is enabled
	private volatile ChromaBm25Index bm25Index;

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

		// weight/gate tuning only matters when hybrid search is enabled
		if (this.useHybridSearch) {
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
		}

		// create or fetch collection Id from the Chroma DB
		this.collectionID = createCollection(this.className);

		if (this.useHybridSearch) {
			initBm25Index();
		}
	}

	/**
	 * Build the in-memory BM25 index from the chunks already stored in the Chroma collection. The
	 * index holds no separate persisted state — Chroma is the source of truth — so it is rebuilt on
	 * every engine open. Failures are non-fatal: hybrid search simply contributes no keyword signal.
	 */
	private void initBm25Index() {
		this.bm25Index = new ChromaBm25Index();
		try {
			Gson gson = new Gson();
			Map<String, Object> getBody = new HashMap<>();
			List<String> include = new ArrayList<>();
			include.add("metadatas");
			getBody.put("include", include);

			// TODO: paginate (Chroma /get supports offset/limit) for very large collections
			String response = HttpHelperUtility.postRequestStringBody(
					collection(this.url, this.tenant, this.dbName, this.collectionID, API_GET),
					buildHeaders(), gson.toJson(getBody), ContentType.APPLICATION_JSON, null, null, null);

			ChromaGetResponse parsed = gson.fromJson(response, ChromaGetResponse.class);
			if (parsed == null || parsed.ids == null || parsed.metadatas == null) {
				return;
			}
			int count = Math.min(parsed.ids.size(), parsed.metadatas.size());
			for (int i = 0; i < count; i++) {
				Map<String, Object> metadata = parsed.metadatas.get(i);
				Object content = metadata.get(VectorDatabaseCSVTable.CONTENT);
				Object source = metadata.get(VectorDatabaseCSVTable.SOURCE);
				this.bm25Index.addRecord(parsed.ids.get(i), source == null ? null : source.toString(),
						content == null ? "" : content.toString(), metadata);
			}
			this.bm25Index.refreshStats();
			classLogger.info("Built BM25 index for engine '{}' from {} chunk(s)", this.className, count);
			if (count >= BM25_LARGE_CORPUS_WARN) {
				classLogger.warn(
						"BM25 index for engine '{}' loaded {} chunks into memory on open; at this scale consider "
								+ "paginated loading or native sparse search (Chroma Cloud /search)",
						this.className, count);
			}
		} catch (Exception e) {
			classLogger.error("Failed to build BM25 index for engine '{}'; keyword scoring disabled", this.className, e);
		}
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

		// keep the in-memory BM25 index in sync with the successful add
		if (this.useHybridSearch && this.bm25Index != null && response != null && !response.trim().isEmpty()) {
			for (int i = 0; i < ids.size(); i++) {
				Map<String, Object> metadata = metadatas.get(i);
				Object source = metadata.get(VectorDatabaseCSVTable.SOURCE);
				Object content = metadata.get(VectorDatabaseCSVTable.CONTENT);
				this.bm25Index.addRecord(ids.get(i), source == null ? null : source.toString(),
						content == null ? "" : content.toString(), metadata);
			}
			this.bm25Index.refreshStats();
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

			// prune the same source from the in-memory BM25 index
			if (this.useHybridSearch && this.bm25Index != null) {
				this.bm25Index.removeBySource(source);
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

		if (this.useHybridSearch && this.bm25Index != null) {
			this.bm25Index.refreshStats();
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
		Map<String, Object> where = buildWhereClause(parameters);

		if (this.useHybridSearch) {
			return executeHybridRrfSearch(searchStatement, vector, limit.intValue(), where);
		}

		// vector-only: return each candidate with its vector Score + raw Distance, metadata after
		List<Map<String, Object>> candidates = queryVectors(vector, limit.intValue(), where);
		List<Map<String, Object>> results = new ArrayList<>(candidates.size());
		for (Map<String, Object> candidate : candidates) {
			Object distance = candidate.remove(DISTANCE_KEY);
			candidate.remove(ChromaBm25Index.ID_KEY);
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
	private Map<String, Object> buildWhereClause(Map<String, Object> parameters) {
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
	 * Hybrid search via full-corpus union: take the vector candidate pool and the BM25 keyword hits
	 * (from the persistent index), union them by chunk id, rank each dimension independently, and fuse
	 * with weighted RRF ({@code score = vw/(k+vRank) + kw/(k+kRank)}, k=60). A document contributes a
	 * term only for the dimension(s) it appears in, so a keyword-only match outside the vector pool can
	 * still surface. Keyword ranking is skipped when nothing matched the query text.
	 */
	private List<Map<String, Object>> executeHybridRrfSearch(String searchStatement, List<Double> vector, int limit,
			Map<String, Object> where) {
		int candidateLimit = Math.max(limit * HYBRID_CANDIDATE_MULTIPLIER, HYBRID_MIN_CANDIDATES);

		List<Map<String, Object>> vectorHits = queryVectors(vector, candidateLimit, where);
		List<Map<String, Object>> keywordHits = (this.bm25Index != null)
				? this.bm25Index.search(searchStatement, candidateLimit)
				: new ArrayList<>();

		// union vector + keyword hits by chunk id
		Map<String, Map<String, Object>> rowById = new LinkedHashMap<>();
		Map<String, Double> distanceById = new LinkedHashMap<>();
		Map<String, Double> keywordById = new LinkedHashMap<>();
		for (Map<String, Object> hit : vectorHits) {
			String id = idOf(hit);
			if (id == null) {
				continue;
			}
			rowById.put(id, hit);
			distanceById.put(id, distanceOf(hit));
		}
		for (Map<String, Object> hit : keywordHits) {
			// the vector side is filtered by Chroma; apply the same filter to the keyword side.
			// NOTE: matches() compares as strings, so it can diverge from Chroma's type coercion for
			// non-string metadata (e.g. numeric); our metadata is string-typed so this is consistent today.
			if (where != null && !ChromaVectorQueryFilterTranslationHelper.matches(hit, where)) {
				continue;
			}
			String id = idOf(hit);
			if (id == null) {
				continue;
			}
			keywordById.put(id, ((Number) hit.get("Score")).doubleValue());
			rowById.putIfAbsent(id, hit);
		}
		if (rowById.isEmpty()) {
			return new ArrayList<>();
		}

		// independent ranks (rank 0 = best): vector by ascending distance, keyword by descending BM25
		Map<String, Integer> vectorRank = ranksOf(distanceById, true);
		boolean useKeyword = !keywordById.isEmpty() && maxValue(keywordById) > this.hybridKeywordGateThreshold;
		Map<String, Integer> keywordRank = ranksOf(keywordById, false);

		double vectorWeight = this.hybridVectorWeight;
		double keywordWeight = 1.0 - vectorWeight;
		classLogger.debug("Chroma hybrid RRF for query '{}': vectorHits={}, keywordHits={}, union={}, vectorWeight={}, keywordWeight={}, useKeyword={}",
				searchStatement, vectorHits.size(), keywordHits.size(), rowById.size(), vectorWeight, keywordWeight, useKeyword);

		// RRF over the union: each doc contributes per dimension only where it ranks
		Map<String, Double> rrfScores = new LinkedHashMap<>();
		for (String id : rowById.keySet()) {
			double score = 0.0;
			Integer vRank = vectorRank.get(id);
			if (vRank != null) {
				score += vectorWeight / (RRF_K + vRank + 1.0);
			}
			Integer kRank = keywordRank.get(id);
			if (useKeyword && kRank != null) {
				score += keywordWeight / (RRF_K + kRank + 1.0);
			}
			rrfScores.put(id, score);
		}

		// top-N by fused score
		List<String> rankedIds = new ArrayList<>(rrfScores.keySet());
		rankedIds.sort((a, b) -> Double.compare(rrfScores.get(b), rrfScores.get(a)));
		int resultCount = Math.min(limit, rankedIds.size());
		List<Map<String, Object>> results = new ArrayList<>(resultCount);
		for (int i = 0; i < resultCount; i++) {
			String id = rankedIds.get(i);
			Map<String, Object> row = rowById.get(id);
			row.put("Score", rrfScores.get(id));
			row.remove(DISTANCE_KEY);
			row.remove(ChromaBm25Index.ID_KEY);
			results.add(row);
		}
		return results;
	}

	/** The chunk id tagged on a row (vector hits via {@link #queryVectors}, keyword hits via the index). */
	private static String idOf(Map<String, Object> row) {
		Object id = row.get(ChromaBm25Index.ID_KEY);
		return (id == null) ? null : id.toString();
	}

	/**
	 * Rank ids by their score (rank 0 = best). {@code ascending} ranks smaller values first (vector
	 * distance); {@code !ascending} ranks larger values first (keyword score).
	 */
	private static Map<String, Integer> ranksOf(Map<String, Double> scoreById, boolean ascending) {
		List<String> order = new ArrayList<>(scoreById.keySet());
		order.sort((a, b) -> ascending ? Double.compare(scoreById.get(a), scoreById.get(b))
				: Double.compare(scoreById.get(b), scoreById.get(a)));
		Map<String, Integer> ranks = new LinkedHashMap<>();
		for (int i = 0; i < order.size(); i++) {
			ranks.put(order.get(i), i);
		}
		return ranks;
	}

	private static double maxValue(Map<String, Double> values) {
		double max = 0.0;
		for (double value : values.values()) {
			if (value > max) {
				max = value;
			}
		}
		return max;
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
		List<String> ids = (parsed.ids != null && !parsed.ids.isEmpty()) ? parsed.ids.get(0) : null;
		for (int i = 0; i < rows.size(); i++) {
			Double distance = (distances != null && i < distances.size()) ? distances.get(i) : null;
			rows.get(i).put(DISTANCE_KEY, distance);
			if (ids != null && i < ids.size()) {
				rows.get(i).put(ChromaBm25Index.ID_KEY, ids.get(i));
			}
		}
		return rows;
	}

	/** Vector distance tagged on a candidate row, or {@link Double#MAX_VALUE} if missing (sorts last). */
	private double distanceOf(Map<String, Object> row) {
		Object distance = row.get(DISTANCE_KEY);
		return (distance instanceof Number) ? ((Number) distance).doubleValue() : Double.MAX_VALUE;
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
		private List<List<String>> ids;
		private List<List<Map<String, Object>>> metadatas;
		private List<List<Double>> distances;
	}

	/** Typed Chroma delete response: {@code {"deleted": N}}. */
	private static class ChromaDeleteResponse {
		private Integer deleted;
	}

	/** Typed Chroma {@code /get} response (flat ids + metadata, used to backfill the BM25 index). */
	private static class ChromaGetResponse {
		private List<String> ids;
		private List<Map<String, Object>> metadatas;
	}

}
