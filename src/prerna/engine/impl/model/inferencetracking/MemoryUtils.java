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
package prerna.engine.impl.model.inferencetracking;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * JDBC-backed CRUD for the MEMORY / MEMORY_AUDIT tables in the
 * ModelInferenceLogsDatabase. This is the platform-native replacement for the
 * memory_mcp app's H2-over-Pixel storage layer: scoping mirrors ROOM (a memory
 * always has a USER_ID owner, and may optionally reference a ROOM_ID and/or
 * WORKSPACE_ID for shared visibility - see MemoryUtils#listMemories).
 *
 * Classification (auto event_type), embedding-based dedup, action items, and
 * compaction are intentionally out of scope for this first CRUD pass and are
 * layered on in later phases.
 */
public class MemoryUtils {

	private static final Logger classLogger = LogManager.getLogger(MemoryUtils.class);

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private static final String MEMORY_TABLE = "MEMORY";
	private static final String MEMORY_AUDIT_TABLE = "MEMORY_AUDIT";
	private static final String MEMORY_ACTION_ITEM_TABLE = "MEMORY_ACTION_ITEM";
	private static final Set<String> VALID_ACTION_ITEM_STATUSES = Set.of("open", "in_progress", "blocked", "completed",
			"cancelled");

	/**
	 * Default vector engine used for memory duplicate detection when the caller
	 * does not specify one and the user has no personal preference set (see
	 * {@link #getUserVectorEngineId}/{@link #resolveVectorEngineId}). A FAISS
	 * engine (local, no external DB dependency) created via
	 * CreateVectorDatabaseEngine, embedding through
	 * e4449559-bcff-4941-ae72-0e3f18e06660. Replaces the earlier JSON-blob +
	 * brute-force-cosine approach with real ANN search.
	 *
	 * <p>
	 * This is a recommended starting default, not a hard platform requirement:
	 * every caller of it goes through {@link #resolveVectorEngineId}, which treats
	 * a missing/deleted/misconfigured engine exactly like "no vector engine
	 * configured" (dedup/indexing silently skipped, never an error) - see
	 * {@link #findDuplicateMemoryViaVector}/{@link #indexMemoryEmbedding}.
	 */
	public static final String DEFAULT_VECTOR_ENGINE_ID = "c44a44a2-30a0-4d5d-898a-4e0b9dcdf0b3";

	private static final String MEMORY_USER_SETTINGS_TABLE = "MEMORY_USER_SETTINGS";

	private MemoryUtils() {
		// static utility class
	}

	/**
	 * Resolves which vector engine to use for a memory operation: an explicit
	 * per-call override, else the user's own configured preference (see
	 * {@link #getUserVectorEngineId}/{@link #setUserVectorEngineId}), else the
	 * platform default. Never validates that the resolved id actually exists -
	 * every caller of the resolved id already treats a bad/missing engine as "skip
	 * silently" (see {@link #findDuplicateMemoryViaVector}), so a stale or deleted
	 * preference can never turn into a hard error for the user.
	 *
	 * @param explicitEngineId engine id passed directly to this call, or blank to
	 *                         fall through to the next source
	 * @param userId           user whose preference to check, or blank to skip
	 *                         straight to the platform default
	 * @return the resolved engine id, or {@code null} if nothing is configured
	 *         anywhere (dedup/indexing is then skipped entirely, not an error)
	 */
	public static String resolveVectorEngineId(String explicitEngineId, String userId) {
		if (explicitEngineId != null && !explicitEngineId.isBlank()) {
			return explicitEngineId;
		}
		if (userId != null && !userId.isBlank()) {
			String userPreference = getUserVectorEngineId(userId);
			if (userPreference != null && !userPreference.isBlank()) {
				return userPreference;
			}
		}
		return DEFAULT_VECTOR_ENGINE_ID;
	}

	/**
	 * Fetches a user's configured default vector engine for memory operations.
	 *
	 * @param userId user identifier
	 * @return the configured vector engine id, or {@code null} if the user has not
	 *         set one
	 */
	public static String getUserVectorEngineId(String userId) {
		if (userId == null || userId.isBlank()) {
			return null;
		}
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MEMORY_USER_SETTINGS_TABLE + "__VECTOR_ENGINE_ID", "vector_engine_id"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(MEMORY_USER_SETTINGS_TABLE + "__USER_ID", "==", userId));
		qs.setLimit(1L);

		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
		if (rows.isEmpty()) {
			return null;
		}
		Object vectorEngineId = rows.get(0).get("vector_engine_id");
		return vectorEngineId == null ? null : vectorEngineId.toString();
	}

	/**
	 * Sets (or clears) a user's configured default vector engine for memory
	 * operations. Does not validate that the engine exists or is a vector engine -
	 * an invalid value simply behaves like "no engine configured" the next time
	 * it's resolved (see {@link #resolveVectorEngineId}), never a hard error.
	 *
	 * @param userId         user identifier (required)
	 * @param vectorEngineId engine id to use going forward, or {@code null}/blank
	 *                       to clear the preference and fall back to the platform
	 *                       default
	 */
	public static void setUserVectorEngineId(String userId, String vectorEngineId) {
		if (userId == null || userId.isBlank()) {
			throw new IllegalArgumentException("userId is required to set a memory vector engine preference.");
		}
		String normalizedEngineId = (vectorEngineId == null || vectorEngineId.isBlank()) ? null : vectorEngineId;

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		boolean hasExistingRow = getUserSettingsRowExists(modelInferenceLogsDb, userId);
		Timestamp now = Utility.getCurrentSqlTimestampUTC();
		String query = hasExistingRow
				? "UPDATE " + MEMORY_USER_SETTINGS_TABLE
						+ " SET VECTOR_ENGINE_ID = ?, DATE_UPDATED = ? WHERE USER_ID = ?"
				: "INSERT INTO " + MEMORY_USER_SETTINGS_TABLE
						+ " (VECTOR_ENGINE_ID, DATE_UPDATED, USER_ID) VALUES (?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			setNullableString(ps, 1, normalizedEngineId);
			ps.setTimestamp(2, now);
			ps.setString(3, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to set memory vector engine preference for userId '{}'.", userId, e);
			throw new IllegalArgumentException("Error setting memory vector engine preference: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	private static boolean getUserSettingsRowExists(IRDBMSEngine modelInferenceLogsDb, String userId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MEMORY_USER_SETTINGS_TABLE + "__USER_ID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(MEMORY_USER_SETTINGS_TABLE + "__USER_ID", "==", userId));
		qs.setLimit(1L);
		return !QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs).isEmpty();
	}

	/**
	 * Inserts a new memory row. Callers wanting duplicate detection should call
	 * {@link #findDuplicateMemoryViaVector} first, and index the new memory's
	 * embedding afterward via {@link #indexMemoryEmbedding} once the id is known.
	 *
	 * @param userId         owner/creator of the memory (required)
	 * @param content        memory text (required)
	 * @param eventType      classification, e.g. "memory"/"decision"/"lesson"
	 * @param roomId         optional conversation the memory was captured in
	 * @param workspaceId    optional workspace scope for shared visibility
	 * @param projectId      optional owning project of the room, informational
	 * @param agentId        optional model/agent id active when this memory was
	 *                       captured (mirrors ROOM.AGENT_ID)
	 * @param metadata       optional JSON-serializable metadata map
	 * @param parentMemoryId optional parent memory this is derived from
	 * @param metaFilters    optional flat key -&gt; values metadata to register as
	 *                       filterable dimensions in MEMORY_META (distinct from the
	 *                       free-form {@code metadata} JSON blob - see
	 *                       {@link #ensureMemoryMetaKeyExists})
	 * @return the generated memory id
	 */
	public static String addMemory(String userId, String content, String eventType, String roomId, String workspaceId,
			String projectId, String agentId, Map<String, Object> metadata, String parentMemoryId,
			Map<String, Object> metaFilters) {
		if (userId == null || userId.isBlank()) {
			throw new IllegalArgumentException("userId is required to add a memory.");
		}
		if (content == null || content.isBlank()) {
			throw new IllegalArgumentException("content is required to add a memory.");
		}

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String memoryId = GUID.v7().toUUID().toString();
		String resolvedEventType = (eventType == null || eventType.isBlank()) ? "memory" : eventType;
		Timestamp now = Utility.getCurrentSqlTimestampUTC();

		String query = "INSERT INTO " + MEMORY_TABLE + " (MEMORY_ID, USER_ID, ROOM_ID, WORKSPACE_ID, PROJECT_ID, "
				+ "AGENT_ID, EVENT_TYPE, CONTENT, METADATA, PARENT_MEMORY_ID, DELETED, DATE_CREATED, DATE_UPDATED) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, memoryId);
			ps.setString(index++, userId);
			setNullableString(ps, index++, roomId);
			setNullableString(ps, index++, workspaceId);
			setNullableString(ps, index++, projectId);
			setNullableString(ps, index++, agentId);
			ps.setString(index++, resolvedEventType);
			modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, content, index++, GSON);
			if (metadata != null && !metadata.isEmpty()) {
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, GSON.toJson(metadata), index++, GSON);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			setNullableString(ps, index++, parentMemoryId);
			ps.setBoolean(index++, false);
			ps.setTimestamp(index++, now);
			ps.setTimestamp(index++, now);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			if (metaFilters != null && !metaFilters.isEmpty()) {
				insertMemoryMeta(memoryId, metaFilters);
			}
			return memoryId;
		} catch (Exception e) {
			classLogger.error("Failed to add memory for userId '{}'.", userId, e);
			throw new IllegalArgumentException("Error adding memory: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Per-event-type similarity thresholds above which a new memory is considered a
	 * duplicate of an existing one, mirroring the memory_mcp app's dedup behavior.
	 * "decision" memories use a tighter threshold since two similar-but-distinct
	 * decisions are more consequential to conflate.
	 */
	private static final Map<String, Double> DUPLICATE_THRESHOLDS = Map.of("decision", 0.95);
	private static final double DEFAULT_DUPLICATE_THRESHOLD = 0.85;

	/**
	 * Over-fetch size for the nearest-neighbor search: vector engines have no
	 * native "filter by user/workspace before searching" concept, so this pulls
	 * more candidates than needed and filters by scope in Java (see class javadoc
	 * on the memory-vector-search design: a single shared FAISS index for all
	 * memories keeps provisioning simple, at the cost of some wasted ANN budget on
	 * out-of-scope candidates - cheap at this scale).
	 */
	private static final int DEDUP_CANDIDATE_LIMIT = 20;

	/**
	 * Default page size for {@link #listMemories} when the caller doesn't pass one.
	 * Listing is never truly unbounded - even a plain recency-ordered listing with
	 * no search term is capped here, so a caller (agent or UI) that forgets to pass
	 * a limit can't accidentally pull back a user's entire memory history in one
	 * call.
	 */
	private static final int DEFAULT_LIST_LIMIT = 20;

	/**
	 * Over-fetch size for semantic search candidates (see
	 * {@link #rankMemoriesBySimilarity}) before scope filtering - mirrors
	 * {@link #DEDUP_CANDIDATE_LIMIT}'s reasoning.
	 */
	private static final int SEMANTIC_CANDIDATE_LIMIT = 20;

	/**
	 * Maximum FAISS L2 distance accepted for semantic search candidates. This
	 * keeps weak vector matches out of hybrid retrieval while preserving BM25
	 * keyword matches. Relevant superseded sources are resolved to their compacted
	 * summary after ranking, so this can remain strict without losing summary
	 * recall.
	 */
	private static final double SEMANTIC_RETURN_THRESHOLD = 0.8;

	/**
	 * Finds the closest existing memory (same visibility scope) whose vector
	 * similarity exceeds the event type's duplicate-detection threshold. Runs a
	 * nearest-neighbor search against the shared vector engine, then filters the
	 * over-fetched candidates down to the caller's scope (owner or workspace) by
	 * looking each candidate up in MEMORY.
	 *
	 * @param vectorEngineId engine id of the vector database to search, or blank to
	 *                       skip dedup entirely
	 * @param insight        insight context to run the search under
	 * @param userId         owner scope to search within (used when workspaceId is
	 *                       blank)
	 * @param workspaceId    optional workspace scope to search within instead
	 * @param agentId        optional agent/workspace-persona scope
	 *                       (ROOM.WORKSPACE_ID) to additionally require a match on
	 *                       - without this, a memory captured under one agent could
	 *                       get silently merged into a similar memory captured
	 *                       under a different agent for the same user, leaving the
	 *                       new fact permanently invisible to the agent it was
	 *                       actually told to. Ignored when {@code workspaceId} is
	 *                       set (workspace-shared memories are cross-agent by
	 *                       design).
	 * @param eventType      event type, used to pick the similarity threshold
	 * @param content        content of the candidate new memory
	 * @return the id and similarity of the closest duplicate, or {@code null} if
	 *         none exceed the threshold (including when the vector engine is
	 *         unavailable or the search fails - dedup is best-effort, never
	 *         blocking)
	 */
	public static Map.Entry<String, Double> findDuplicateMemoryViaVector(String vectorEngineId,
			prerna.om.Insight insight, String userId, String workspaceId, String agentId, String eventType,
			String content) {
		if (vectorEngineId == null || vectorEngineId.isBlank()) {
			return null;
		}
		double threshold = DUPLICATE_THRESHOLDS.getOrDefault(eventType, DEFAULT_DUPLICATE_THRESHOLD);

		List<Map<String, Object>> neighbors;
		prerna.engine.api.IVectorDatabaseEngine vectorEngine;
		try {
			vectorEngine = Utility.getVectorDatabase(vectorEngineId);
			if (vectorEngine == null) {
				return null;
			}
			// Hybrid (vector + BM25 keyword) search combines scores via reciprocal-rank
			// fusion, which is great for retrieval relevance but not a usable duplicate
			// signal - two totally unrelated memories that merely share a few keywords
			// can outscore an actual near-duplicate. Force pure vector similarity for
			// dedup so the threshold check below means what it says.
			Map<String, Object> params = new HashMap<>();
			params.put(prerna.reactor.vector.VectorDatabaseParamOptionsEnum.USE_HYBRID_SEARCH.getKey(), false);
			neighbors = vectorEngine.nearestNeighbor(insight, content, DEDUP_CANDIDATE_LIMIT, params);
		} catch (Exception e) {
			classLogger.warn("Memory vector search failed; duplicate detection skipped for this add.", e);
			return null;
		}
		if (neighbors == null || neighbors.isEmpty()) {
			return null;
		}
		// FaissDatabaseEngine's "Score" (even with hybrid search off) is empirically
		// a raw L2 distance over the embedding vectors, not a 0-1 similarity, despite
		// the engine being configured with distance_method "Cosine Similarity" -
		// verified by observing scores >1 for unrelated content and ~0.02 for an
		// exact duplicate. For L2-normalized embeddings, cosine similarity relates
		// to L2 distance by cosine = 1 - (distance^2 / 2), so convert back to a
		// normal 0-1 similarity to compare against the same thresholds other
		// (already-normalized) vector engines use.
		//
		// Note: vectorEngine here is typically a dynamic proxy
		// (PipelineInvocationHandler)
		// wrapping the real engine over a Python socket bridge, so an
		// `instanceof FaissDatabaseEngine` check on the concrete class always fails -
		// checking the interface-exposed VECTOR_TYPE property instead works
		// through the proxy.
		boolean isL2Distance = "FAISS".equalsIgnoreCase(
				vectorEngine.getSmssProp().getProperty(prerna.engine.api.IVectorDatabaseEngine.VECTOR_TYPE));

		String bestId = null;
		double bestScore = -1;
		for (Map<String, Object> neighbor : neighbors) {
			Object scoreValue = neighbor.get("Score");
			Object sourceValue = neighbor.get(prerna.engine.impl.vector.VectorDatabaseCSVTable.SOURCE);
			if (!(scoreValue instanceof Number) || sourceValue == null) {
				continue;
			}
			double rawScore = ((Number) scoreValue).doubleValue();
			double score = isL2Distance ? (1 - (rawScore * rawScore) / 2) : rawScore;
			if (score <= bestScore || score < threshold) {
				continue;
			}
			String candidateMemoryId = sourceValue.toString();
			Map<String, Object> candidate = getMemoryById(candidateMemoryId);
			if (candidate == null) {
				// deleted or otherwise no longer visible - stale vector entry
				continue;
			}
			boolean inScope = workspaceId != null && !workspaceId.isBlank()
					? workspaceId.equals(candidate.get("workspace_id"))
					: userId.equals(candidate.get("user_id"))
							&& (agentId == null || agentId.isBlank() || agentId.equals(candidate.get("agent_id")));
			if (!inScope) {
				continue;
			}
			bestScore = score;
			bestId = candidateMemoryId;
		}
		return bestId != null ? Map.entry(bestId, bestScore) : null;
	}

	/**
	 * Indexes a memory's content into the vector engine for future duplicate
	 * detection, keyed by the memory's own id (so a nearest-neighbor hit can be
	 * looked back up directly). Best-effort: a failure here does not fail the add,
	 * it just means this memory will not be found as a future duplicate.
	 *
	 * @param vectorEngineId engine id of the vector database to index into, or
	 *                       blank to skip
	 * @param insight        insight context to run the embedding call under
	 * @param memoryId       id of the memory just inserted (used as the vector
	 *                       row's SOURCE)
	 * @param content        memory content to embed
	 */
	public static void indexMemoryEmbedding(String vectorEngineId, prerna.om.Insight insight, String memoryId,
			String content) {
		if (vectorEngineId == null || vectorEngineId.isBlank()) {
			return;
		}
		java.io.File tempCsv = null;
		try {
			prerna.engine.api.IVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase(vectorEngineId);
			if (vectorEngine == null) {
				return;
			}
			// FaissDatabaseEngine.addEmbeddings(VectorDatabaseCSVTable, ...) requires the
			// table to be backed by an actual file on disk (it calls table.getFile()) -
			// a purely in-memory table (new VectorDatabaseCSVTable() + addRow) is not
			// enough, unlike some other vector engine implementations. Write a
			// short-lived CSV and load it back via initCSVTable so the table has a
			// backing file regardless of which engine type is configured.
			tempCsv = java.io.File.createTempFile("memory-embedding-", ".csv");
			writeMemoryEmbeddingCsv(tempCsv, memoryId, content);
			prerna.engine.impl.vector.VectorDatabaseCSVTable table = prerna.engine.impl.vector.VectorDatabaseCSVTable
					.initCSVTable(tempCsv);
			vectorEngine.addEmbeddings(table, insight, new HashMap<>());
		} catch (Exception e) {
			classLogger.warn("Failed to index memory '{}' into the vector engine; it will not be found as a "
					+ "future duplicate.", memoryId, e);
		} finally {
			if (tempCsv != null) {
				tempCsv.delete();
			}
		}
	}

	/**
	 * Writes a single-row CSV matching
	 * {@link prerna.engine.impl.vector.VectorDatabaseCSVTable}'s expected columns
	 * (Source, Modality, Divider, Part, Tokens, Content), quoting the content so
	 * embedded commas/quotes/newlines round-trip correctly.
	 */
	private static void writeMemoryEmbeddingCsv(java.io.File file, String memoryId, String content)
			throws java.io.IOException {
		String escapedContent = content.replace("\"", "\"\"");
		String csvLine = "Source,Modality,Divider,Part,Tokens,Content\n" + memoryId + ",TEXT,,1,0,\"" + escapedContent
				+ "\"\n";
		java.nio.file.Files.writeString(file.toPath(), csvLine);
	}

	/**
	 * Removes a memory's vector entry, e.g. after a delete. Best-effort: logged and
	 * swallowed on failure so a vector-engine hiccup never blocks a delete.
	 *
	 * @param vectorEngineId engine id of the vector database, or blank to skip
	 * @param memoryId       id of the memory being removed
	 */
	public static void removeMemoryEmbedding(String vectorEngineId, String memoryId) {
		if (vectorEngineId == null || vectorEngineId.isBlank()) {
			return;
		}
		try {
			prerna.engine.api.IVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase(vectorEngineId);
			if (vectorEngine == null) {
				return;
			}
			vectorEngine.removeDocument(List.of(memoryId), new HashMap<>());
		} catch (Exception e) {
			classLogger.warn("Failed to remove memory '{}' from the vector engine.", memoryId, e);
		}
	}

	/**
	 * Calls a reasoning model engine to classify memory content into one of the
	 * supported event types. Falls back to "memory" if {@code reasoningEngineId} is
	 * blank or the model call fails/returns an unrecognized value.
	 *
	 * @param reasoningEngineId engine id of the reasoning model, or blank to skip
	 *                          classification
	 * @param insight           insight context to run the model call under
	 * @param content           memory content to classify
	 * @return a resolved event type, always non-blank
	 */
	public static String classifyEventType(String reasoningEngineId, prerna.om.Insight insight, String content) {
		if (reasoningEngineId == null || reasoningEngineId.isBlank()) {
			return "memory";
		}
		try {
			prerna.engine.api.IModelEngine reasoningEngine = Utility.getModel(reasoningEngineId);
			if (reasoningEngine == null) {
				return "memory";
			}
			String prompt = "Classify the following content into exactly one of these categories: "
					+ "memory, decision, lesson, error, task, session_summary, user_preference, observation, "
					+ "status_update. Respond with only the category name, nothing else.\n\nContent: " + content;
			prerna.engine.impl.model.responses.AskModelEngineResponse<?> response = reasoningEngine.ask(prompt, null,
					insight, Map.of());
			Object rawResponse = response == null ? null : response.getResponse();
			String classification = rawResponse == null ? null : rawResponse.toString();
			if (classification == null) {
				return "memory";
			}
			String normalized = classification.trim().toLowerCase().replaceAll("[^a-z_]", "");
			return EVENT_TYPES.contains(normalized) ? normalized : "memory";
		} catch (Exception e) {
			classLogger.warn("Memory classification failed; defaulting to 'memory'.", e);
			return "memory";
		}
	}

	private static final Set<String> EVENT_TYPES = Set.of("memory", "decision", "lesson", "error", "task",
			"session_summary", "user_preference", "observation", "status_update");

	/**
	 * Combines two or more memories into a single summary memory. Source memories
	 * are marked with {@code SUPERSEDES_MEMORY_ID} pointing at the new summary
	 * (they are not deleted - they remain available for audit/history). Only the
	 * creator of every source memory may compact them.
	 *
	 * @param userId            user requesting the compaction (must own every
	 *                          source memory)
	 * @param memoryIds         two or more source memory ids
	 * @param reasoningEngineId engine id used to generate the summary text, or
	 *                          blank to fall back to a simple concatenation
	 * @param insight           insight context to run the model call under
	 * @return the new summary memory id and the number of memories compacted
	 */
	public static Map<String, Object> compactMemories(String userId, List<String> memoryIds, String reasoningEngineId,
			prerna.om.Insight insight) {
		if (memoryIds == null || memoryIds.size() < 2) {
			throw new IllegalArgumentException("At least two memory ids are required to compact memories.");
		}

		List<Map<String, Object>> sources = new ArrayList<>();
		for (String memoryId : memoryIds) {
			Map<String, Object> source = getMemoryById(memoryId);
			if (source == null) {
				throw new IllegalArgumentException("Memory not found: " + memoryId);
			}
			if (!userId.equals(source.get("user_id"))) {
				throw new IllegalArgumentException("Only the creator of every source memory may compact them.");
			}
			sources.add(source);
		}

		// All sources must share the same agent/workspace scope - otherwise the
		// summary's scope would be ambiguous (arbitrarily "whichever memory
		// happened to be first"), and the compacted knowledge would silently
		// escape the scope it was captured in: e.g. compacting a Travel Agent
		// memory together with a Tax Agent memory would previously drop AGENT_ID
		// entirely, making the summary invisible to the very agent it came from.
		Map<String, Object> first = sources.get(0);
		String agentId = (String) first.get("agent_id");
		String workspaceId = (String) first.get("workspace_id");
		for (Map<String, Object> source : sources) {
			if (!Objects.equals(agentId, source.get("agent_id"))
					|| !Objects.equals(workspaceId, source.get("workspace_id"))) {
				throw new IllegalArgumentException(
						"All memories being compacted together must belong to the same agent/workspace scope.");
			}
		}

		String summaryContent = generateSummary(reasoningEngineId, insight, sources);
		String roomId = (String) first.get("room_id");
		Map<String, Object> metadata = Map.of("source_memory_ids", memoryIds, "compacted_count", sources.size());

		String summaryId = addMemory(userId, summaryContent, "session_summary", roomId, workspaceId, null, agentId,
				metadata, null, null);
		indexMemoryEmbedding(resolveVectorEngineId(null, userId), insight, summaryId, summaryContent);

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String updateQuery = "UPDATE " + MEMORY_TABLE + " SET SUPERSEDES_MEMORY_ID = ?, DATE_UPDATED = ? "
				+ "WHERE MEMORY_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(updateQuery);
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			for (String memoryId : memoryIds) {
				ps.setString(1, summaryId);
				ps.setTimestamp(2, now);
				ps.setString(3, memoryId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to mark source memories as superseded by '{}'.", summaryId, e);
			throw new IllegalArgumentException("Error compacting memories: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}

		return Map.of("summary_memory_id", summaryId, "compacted_count", sources.size());
	}

	/**
	 * Generates summary text for a set of source memories via a reasoning model,
	 * falling back to a simple concatenation when {@code reasoningEngineId} is
	 * blank or the model call fails.
	 *
	 * @param reasoningEngineId engine id of the reasoning model, or blank to skip
	 *                          straight to the fallback
	 * @param insight           insight context to run the model call under
	 * @param sources           source memory rows (must contain "content")
	 * @return summary text, always non-blank
	 */
	private static String generateSummary(String reasoningEngineId, prerna.om.Insight insight,
			List<Map<String, Object>> sources) {
		StringBuilder combined = new StringBuilder();
		for (Map<String, Object> source : sources) {
			combined.append("- ").append(source.get("content")).append('\n');
		}
		String fallback = "Summary of " + sources.size() + " memories:\n" + combined;

		if (reasoningEngineId == null || reasoningEngineId.isBlank()) {
			return fallback;
		}
		try {
			prerna.engine.api.IModelEngine reasoningEngine = Utility.getModel(reasoningEngineId);
			if (reasoningEngine == null) {
				return fallback;
			}
			String prompt = "Summarize the following related memories into a single, concise paragraph that "
					+ "preserves the important facts from each:\n\n" + combined;
			prerna.engine.impl.model.responses.AskModelEngineResponse<?> response = reasoningEngine.ask(prompt, null,
					insight, Map.of());
			Object rawResponse = response == null ? null : response.getResponse();
			String summary = rawResponse == null ? null : rawResponse.toString();
			return (summary == null || summary.isBlank()) ? fallback : summary.trim();
		} catch (Exception e) {
			classLogger.warn("Memory summary generation failed; falling back to concatenation.", e);
			return fallback;
		}
	}

	/**
	 * Fetches one non-deleted memory by id, or {@code null} if missing/deleted.
	 *
	 * @param memoryId memory identifier
	 * @return the memory row as a map, or {@code null}
	 */
	public static Map<String, Object> getMemoryById(String memoryId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		addMemorySelectors(qs);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__MEMORY_ID", "==", memoryId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__DELETED", "==", false, PixelDataType.BOOLEAN));
		qs.setLimit(1L);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
		return results.isEmpty() ? null : results.get(0);
	}

	/**
	 * Lists non-deleted memories visible to a user: memories they own, plus (when
	 * {@code workspaceId} is supplied) memories shared to that workspace. Callers
	 * are responsible for verifying the user can view the workspace before passing
	 * it in (see SecurityProjectUtils#userCanViewProject).
	 *
	 * @param userId            requesting user (required)
	 * @param workspaceId       optional workspace scope the user has view access to
	 * @param roomId            optional room to restrict results to
	 * @param agentId           optional agent/workspace id (see
	 *                          {@code ROOM.WORKSPACE_ID} /
	 *                          {@code SetRoomWorkspaceReactor}) to restrict results
	 *                          to - always ANDed with the ownership/workspace-share
	 *                          check above, never a substitute for it, so this can
	 *                          be used to recall "everything I told this agent"
	 *                          without exposing other users' memories
	 * @param eventTypes        optional event-type filter, OR'd together
	 *                          (multi-value)
	 * @param search            optional free-text query. When a vector engine
	 *                          resolves (see {@code vectorEngineId}), this is
	 *                          matched by semantic similarity within the same
	 *                          visibility scope, ranked most-relevant first, and
	 *                          capped by {@code limit} - not a literal substring
	 *                          scan of every visible memory. Falls back to a plain
	 *                          case-insensitive substring match when no vector
	 *                          engine is available or the search returns no usable
	 *                          candidates.
	 * @param metaFilters       optional metakey -&gt; values filters, AND'd across
	 *                          keys and OR'd within a key's values (mirrors
	 *                          PROMPTMETA / ENGINEMETA subquery filtering)
	 * @param projectId         optional project/app scope: narrows to memories
	 *                          captured while that specific project/app's room was
	 *                          active (informational - separate from which agent
	 *                          was attached to the room, see {@code agentId})
	 * @param includeSuperseded when {@code false} (the default), memories that have
	 *                          already been folded into a {@code CompactMemories}
	 *                          summary ({@code SUPERSEDES_MEMORY_ID} set) are
	 *                          excluded - the summary already covers them, so
	 *                          showing both is redundant noise for a caller (agent
	 *                          or human) just trying to recall what's relevant.
	 *                          Pass {@code true} to see the full, uncollapsed
	 *                          history (e.g. for an audit view).
	 * @param vectorEngineId    resolved vector engine id to use for semantic
	 *                          ranking of {@code search} (see
	 *                          {@link #resolveVectorEngineId}), or blank to force
	 *                          the plain substring path
	 * @param insight           insight context to run the embedding/search call
	 *                          under; required only when semantic search is
	 *                          actually attempted
	 * @param limit             max rows to return; defaults to
	 *                          {@link #DEFAULT_LIST_LIMIT} when {@code null}/&lt;=0
	 *                          - listing is never unbounded, even for a caller that
	 *                          forgets to pass a limit
	 * @param offset            rows to skip, or {@code null}/&lt;0 for no offset
	 *                          (ignored once semantic ranking is used - relevance
	 *                          order doesn't paginate meaningfully)
	 * @return a map with {@code memories} (the page of rows, most relevant/ recent
	 *         first), {@code total_count} (rows matching the filters across all
	 *         pages), and {@code has_more}
	 */
	public static Map<String, Object> listMemories(String userId, String workspaceId, String roomId, String agentId,
			List<String> eventTypes, String search, Map<String, Object> metaFilters, String projectId,
			Boolean includeSuperseded, String vectorEngineId, prerna.om.Insight insight, Integer limit,
			Integer offset) {
		if (userId == null || userId.isBlank()) {
			throw new IllegalArgumentException("userId is required to list memories.");
		}

		long effectiveLimit = (limit != null && limit > 0) ? limit : DEFAULT_LIST_LIMIT;
		long effectiveOffset = (offset != null && offset > 0) ? offset : 0;

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();

		// Semantic ranking: over-fetch candidate ids from the vector engine, with
		// hybrid search on (see rankMemoriesBySimilarity - the engine itself already
		// fuses real vector + BM25 ranking when it supports hybrid search, so there's
		// no need to reimplement that fusion here), then let the SQL query below
		// re-apply every other filter (scope/eventType/metaFilters/superseded)
		// against just those candidates - so a match still has to pass every
		// ownership and visibility check a plain listing would. Superseded source
		// ids resolve to their latest compacted summary before filtering. Falls
		// through to the original substring match if nothing usable comes back.
		List<String> rankedIds = null;
		if (search != null && !search.isBlank() && vectorEngineId != null && !vectorEngineId.isBlank()
				&& insight != null) {
			rankedIds = rankMemoriesBySimilarity(vectorEngineId, insight, search,
					Math.max((int) effectiveLimit * 3, SEMANTIC_CANDIDATE_LIMIT));
			if (!Boolean.TRUE.equals(includeSuperseded) && !rankedIds.isEmpty()) {
				rankedIds = resolveCompactedMemoryIds(modelInferenceLogsDb, rankedIds);
			}
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		addMemorySelectors(qs);
		qs.addSelector(
				new prerna.query.querystruct.selectors.QueryOpaqueSelector("COUNT(*) OVER()", "total_row_count"));
		addMemoryScopeFilters(qs, userId, workspaceId, roomId, agentId, projectId, eventTypes, includeSuperseded);
		if (rankedIds != null && !rankedIds.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__MEMORY_ID", "==", rankedIds));
		} else if (search != null && !search.isBlank()) {
			// No vector engine available, or it returned nothing usable - fall back to
			// a plain substring match rather than silently returning zero results.
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__CONTENT", "?like",
					"%" + search + "%", PixelDataType.CONST_STRING));
		}
		addMemoryMetaFilters(qs, metaFilters);

		if (rankedIds == null || rankedIds.isEmpty()) {
			qs.addOrderBy(new QueryColumnOrderBySelector(MEMORY_TABLE + "__DATE_CREATED", "DESC"));
			if (effectiveLimit > 0) {
				qs.setLimit(effectiveLimit);
			}
			if (effectiveOffset > 0) {
				qs.setOffSet(effectiveOffset);
			}
		}
		// When ranking semantically, deliberately no SQL LIMIT/OFFSET/ORDER BY - the
		// candidate list from the vector engine is already capped and ordered by
		// relevance; rows are re-sorted to match that order and capped below, after
		// the visibility/scope filters above have had a chance to drop any
		// candidate that isn't actually visible to this caller.

		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
		if (rankedIds != null && !rankedIds.isEmpty()) {
			Map<String, Integer> rankById = new HashMap<>();
			for (int i = 0; i < rankedIds.size(); i++) {
				rankById.put(rankedIds.get(i), i);
			}
			rows.sort(Comparator.comparingInt(row -> rankById.getOrDefault(row.get("memory_id"), Integer.MAX_VALUE)));
			if (rows.size() > effectiveLimit) {
				rows = rows.subList(0, (int) effectiveLimit);
			}
		}
		return toPagedResult(rows, "memories", effectiveLimit, effectiveOffset);
	}

	/**
	 * Replaces ranked source-memory ids with the latest summary that supersedes
	 * them. Chains are followed so a summary compacted again into a newer summary
	 * still resolves to the visible row. The original rank order is preserved and
	 * duplicate summaries are removed.
	 */
	private static List<String> resolveCompactedMemoryIds(IRDBMSEngine modelInferenceLogsDb, List<String> rankedIds) {
		Map<String, String> replacements = new HashMap<>();
		Set<String> loadedIds = new HashSet<>();
		Set<String> pendingIds = new LinkedHashSet<>(rankedIds);

		while (!pendingIds.isEmpty()) {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__MEMORY_ID", "memory_id"));
			qs.addSelector(
					new QueryColumnSelector(MEMORY_TABLE + "__SUPERSEDES_MEMORY_ID", "supersedes_memory_id"));
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__MEMORY_ID", "==", pendingIds));

			List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
			loadedIds.addAll(pendingIds);
			pendingIds.clear();
			for (Map<String, Object> row : rows) {
				Object memoryId = row.get("memory_id");
				Object supersedesMemoryId = row.get("supersedes_memory_id");
				if (memoryId == null || supersedesMemoryId == null
						|| supersedesMemoryId.toString().isBlank()) {
					continue;
				}
				String replacementId = supersedesMemoryId.toString();
				replacements.put(memoryId.toString(), replacementId);
				if (!loadedIds.contains(replacementId)) {
					pendingIds.add(replacementId);
				}
			}
		}

		return replaceSupersededMemoryIds(rankedIds, replacements);
	}

	static List<String> replaceSupersededMemoryIds(List<String> rankedIds, Map<String, String> replacements) {
		Set<String> resolvedIds = new LinkedHashSet<>();
		for (String rankedId : rankedIds) {
			String resolvedId = rankedId;
			Set<String> visitedIds = new HashSet<>();
			while (visitedIds.add(resolvedId)) {
				String replacementId = replacements.get(resolvedId);
				if (replacementId == null || replacementId.isBlank()) {
					break;
				}
				resolvedId = replacementId;
			}
			resolvedIds.add(resolvedId);
		}
		return new ArrayList<>(resolvedIds);
	}

	/**
	 * Over-fetches the vector engine's nearest neighbors for a free-text query and
	 * returns just the memory ids, ranked most-relevant first. Does <b>not</b>
	 * filter by visibility scope itself - that's left to the SQL query in
	 * {@link #listMemories}, which re-applies every ownership/scope filter against
	 * this candidate list, so a match can never bypass the same checks a plain
	 * listing would.
	 *
	 * <p>
	 * Hybrid search is deliberately left <b>on</b> here (unlike
	 * {@link #findDuplicateMemoryViaVector}, which forces it off) - this method
	 * only needs the engine's relative rank order, never a comparable absolute
	 * score, so it can safely delegate to whatever combined vector+keyword ranking
	 * the underlying engine already implements instead of reimplementing a weaker
	 * approximation in Java: {@code FaissDatabaseEngine} (the platform default
	 * memory vector engine) builds and queries a real BM25 index alongside the
	 * vector index when hybrid search is enabled, and
	 * {@code OpenSearchRestVectorDatabaseEngine} does the same via OpenSearch's
	 * native hybrid search pipeline. Engines that don't implement hybrid search at
	 * all simply ignore the flag and fall back to plain vector ranking, so this is
	 * safe platform-wide regardless of which vector engine a given installation
	 * uses.
	 *
	 * @param vectorEngineId engine id to search (required, non-blank)
	 * @param insight        insight context to run the embedding call under
	 * @param query          free-text query to rank memories against
	 * @param candidateLimit how many nearest neighbors to fetch
	 * @return memory ids ordered by descending relevance, or an empty list if the
	 *         search fails/returns nothing usable (best-effort - the caller falls
	 *         back to a plain substring match in that case)
	 */
	private static List<String> rankMemoriesBySimilarity(String vectorEngineId, prerna.om.Insight insight, String query,
			int candidateLimit) {
		try {
			prerna.engine.api.IVectorDatabaseEngine vectorEngine = Utility.getVectorDatabase(vectorEngineId);
			if (vectorEngine == null) {
				return List.of();
			}
			Map<String, Object> params = new HashMap<>();
			params.put(prerna.reactor.vector.VectorDatabaseParamOptionsEnum.USE_HYBRID_SEARCH.getKey(), true);
			params.put(prerna.reactor.vector.VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey(),
					SEMANTIC_RETURN_THRESHOLD);
			List<Map<String, Object>> neighbors = vectorEngine.nearestNeighbor(insight, query, candidateLimit, params);
			if (neighbors == null || neighbors.isEmpty()) {
				return List.of();
			}
			// Already returned in the engine's own best-first order (see
			// findDuplicateMemoryViaVector's notes on FAISS's raw-distance quirk -
			// harmless here since we only need relative order, not the absolute
			// score, to rank candidates).
			List<String> ids = new ArrayList<>();
			for (Map<String, Object> neighbor : neighbors) {
				Object sourceValue = neighbor.get(prerna.engine.impl.vector.VectorDatabaseCSVTable.SOURCE);
				if (sourceValue != null) {
					ids.add(sourceValue.toString());
				}
			}
			return ids;
		} catch (Exception e) {
			classLogger.warn("Memory search failed; falling back to substring match.", e);
			return List.of();
		}
	}

	/**
	 * Adds the visibility/room/agent/project/eventType/superseded scope filters
	 * shared by every {@link #listMemories} SQL path.
	 *
	 * @param qs                query to add filters to
	 * @param userId            requesting user
	 * @param workspaceId       optional shared-workspace scope (bypasses per-user
	 *                          ownership - see {@link #listMemories})
	 * @param roomId            optional room filter
	 * @param agentId           optional agent filter (always ANDed with ownership,
	 *                          never a substitute for it)
	 * @param projectId         optional project filter
	 * @param eventTypes        optional event-type filter (OR'd)
	 * @param includeSuperseded whether to include already-compacted memories
	 */
	private static void addMemoryScopeFilters(SelectQueryStruct qs, String userId, String workspaceId, String roomId,
			String agentId, String projectId, List<String> eventTypes, Boolean includeSuperseded) {
		AndQueryFilter visibilityFilter = new AndQueryFilter();
		visibilityFilter.addFilter(
				SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__DELETED", "==", false, PixelDataType.BOOLEAN));
		if (workspaceId != null && !workspaceId.isBlank()) {
			// Shared scope: anyone with view access to the workspace (caller already
			// verified before this call - see SecurityProjectUtils#userCanViewProject).
			// Deliberately not ANDed with USER_ID - that is the whole point of
			// promoting a memory to a workspace (see PromoteMemoryToWorkspaceReactor).
			visibilityFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__WORKSPACE_ID", "==", workspaceId));
		} else {
			// Personal scope: always the caller's own memories only. agentId (below)
			// can narrow this further to "memories I told this specific agent", but
			// it is ANDed in, never substituted for this ownership check - unlike
			// workspaceId, it can never expose another user's memories.
			visibilityFilter.addFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__USER_ID", "==", userId));
		}
		qs.addExplicitFilter(visibilityFilter);

		if (roomId != null && !roomId.isBlank()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__ROOM_ID", "==", roomId));
		}
		if (agentId != null && !agentId.isBlank()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__AGENT_ID", "==", agentId));
		}
		if (projectId != null && !projectId.isBlank()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__PROJECT_ID", "==", projectId));
		}
		if (eventTypes != null && !eventTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__EVENT_TYPE", "==", eventTypes));
		}
		if (!Boolean.TRUE.equals(includeSuperseded)) {
			// Hide memories already folded into a CompactMemories summary by default -
			// the summary already covers them, so surfacing both is redundant noise
			// for whoever (agent or human) is just trying to recall what's relevant.
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__SUPERSEDES_MEMORY_ID", "==", (Object) null));
		}
	}

	/**
	 * Adds one AND'd subquery filter per metakey to {@code qs}, each restricting
	 * MEMORY_ID to those present in MEMORY_META with that key and any of the given
	 * values - the same subquery-per-metakey pattern PROMPTMETA/ENGINEMETA use for
	 * their metadata filters.
	 *
	 * @param qs          query struct to add filters to
	 * @param metaFilters optional metakey -&gt; values filters
	 */
	private static void addMemoryMetaFilters(SelectQueryStruct qs, Map<String, Object> metaFilters) {
		if (metaFilters == null || metaFilters.isEmpty()) {
			return;
		}
		for (Map.Entry<String, Object> entry : metaFilters.entrySet()) {
			if (entry.getValue() == null) {
				continue;
			}
			SelectQueryStruct metaSubQs = new SelectQueryStruct();
			metaSubQs.addSelector(new QueryColumnSelector(MEMORY_META_TABLE + "__MEMORY_ID"));
			AndQueryFilter metaFilter = new AndQueryFilter();
			metaFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_META_TABLE + "__METAKEY", "==", entry.getKey()));
			metaFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_META_TABLE + "__METAVALUE", "==", entry.getValue()));
			metaSubQs.addExplicitFilter(metaFilter);
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(MEMORY_TABLE + "__MEMORY_ID", "==", metaSubQs));
		}
	}

	/**
	 * Extracts the {@code total_row_count} window-count pseudo-column that
	 * {@code COUNT(*) OVER()} adds to every row, removes it from each row map, and
	 * packages the page into {@code {<itemsKey>, total_count, has_more}}.
	 *
	 * @param rows     rows returned from a query that included a
	 *                 {@code COUNT(*) OVER()} opaque selector aliased
	 *                 {@code total_row_count}
	 * @param itemsKey key to store the (now-cleaned) rows under
	 * @param limit    the limit that was applied (0 means "no limit")
	 * @param offset   the offset that was applied
	 * @return the paged result map
	 */
	private static Map<String, Object> toPagedResult(List<Map<String, Object>> rows, String itemsKey, long limit,
			long offset) {
		long totalCount = rows.size();
		for (Map<String, Object> row : rows) {
			Object totalRowCount = row.remove("total_row_count");
			if (totalRowCount instanceof Number number) {
				totalCount = number.longValue();
			}
		}
		boolean hasMore = limit > 0 && (offset + rows.size()) < totalCount;

		Map<String, Object> result = new HashMap<>();
		result.put(itemsKey, rows);
		result.put("total_count", totalCount);
		result.put("has_more", hasMore);
		return result;
	}

	/**
	 * Updates only a memory's EVENT_TYPE column, with no audit entry - used by
	 * {@code MemoryClassificationWorker} to backfill an async classification
	 * result. Not user-facing (no ownership check); the caller must already know
	 * the memory id is the one it just inserted.
	 *
	 * @param memoryId  memory identifier
	 * @param eventType resolved event type
	 * @return {@code true} if a row was updated
	 */
	public static boolean updateEventType(String memoryId, String eventType) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE " + MEMORY_TABLE + " SET EVENT_TYPE = ?, DATE_UPDATED = ? WHERE MEMORY_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			ps.setString(1, eventType);
			ps.setTimestamp(2, Utility.getCurrentSqlTimestampUTC());
			ps.setString(3, memoryId);
			int updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			return updated > 0;
		} catch (SQLException e) {
			classLogger.error("Failed to backfill classification for memory '{}'.", memoryId, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Updates a memory's content/metadata and records an audit row with the prior
	 * values. Only the memory's creator may edit it.
	 *
	 * @param memoryId   memory identifier
	 * @param userId     user attempting the edit
	 * @param newContent replacement content (required)
	 * @param metadata   replacement metadata, or {@code null} to leave unchanged
	 * @return {@code true} if a row was updated
	 */
	public static boolean editMemory(String memoryId, String userId, String newContent, Map<String, Object> metadata) {
		if (newContent == null || newContent.isBlank()) {
			throw new IllegalArgumentException("newContent is required to edit a memory.");
		}
		Map<String, Object> existing = getMemoryById(memoryId);
		if (existing == null) {
			throw new IllegalArgumentException("Memory not found: " + memoryId);
		}
		if (!userId.equals(existing.get("user_id"))) {
			throw new IllegalArgumentException("Only the memory's creator may edit it.");
		}

		recordAudit(memoryId, "edit", (String) existing.get("content"), (String) existing.get("metadata"), userId);

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE " + MEMORY_TABLE
				+ " SET CONTENT = ?, METADATA = ?, DATE_UPDATED = ? WHERE MEMORY_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, newContent, index++, GSON);
			if (metadata != null) {
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, GSON.toJson(metadata), index++, GSON);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(index++, memoryId);
			int updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			return updated > 0;
		} catch (Exception e) {
			classLogger.error("Failed to edit memory '{}'.", memoryId, e);
			throw new IllegalArgumentException("Error editing memory: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Soft-deletes a memory (only the creator may delete it) and records an audit
	 * row with the content/metadata as of deletion.
	 *
	 * @param memoryId memory identifier
	 * @param userId   user attempting the delete
	 * @return {@code true} if a row was updated
	 */
	public static boolean deleteMemory(String memoryId, String userId) {
		Map<String, Object> existing = getMemoryById(memoryId);
		if (existing == null) {
			throw new IllegalArgumentException("Memory not found: " + memoryId);
		}
		if (!userId.equals(existing.get("user_id"))) {
			throw new IllegalArgumentException("Only the memory's creator may delete it.");
		}

		recordAudit(memoryId, "delete", (String) existing.get("content"), (String) existing.get("metadata"), userId);

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE " + MEMORY_TABLE + " SET DELETED = ?, DELETED_AT = ? WHERE MEMORY_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			ps.setBoolean(1, true);
			ps.setTimestamp(2, Utility.getCurrentSqlTimestampUTC());
			ps.setString(3, memoryId);
			int updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			removeMemoryEmbedding(resolveVectorEngineId(null, userId), memoryId);
			return updated > 0;
		} catch (SQLException e) {
			classLogger.error("Failed to delete memory '{}'.", memoryId, e);
			throw new IllegalArgumentException("Error deleting memory: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Admin-only listing across every user/workspace, with optional filters.
	 * Callers must have already verified the requesting user is an admin (see
	 * {@code SecurityAdminUtils.getInstance(user)}).
	 *
	 * @param userIdFilter      optional exact USER_ID filter
	 * @param workspaceIdFilter optional exact WORKSPACE_ID filter
	 * @param eventTypeFilter   optional exact EVENT_TYPE filter
	 * @param fromDate          optional inclusive lower bound on DATE_CREATED
	 * @param toDate            optional inclusive upper bound on DATE_CREATED
	 * @param limit             max rows to return, or {@code null}/&lt;=0 for no
	 *                          limit
	 * @param offset            rows to skip, or {@code null}/&lt;0 for no offset
	 * @return matching memory rows across all users, most recent first
	 */
	public static List<Map<String, Object>> adminListMemories(String userIdFilter, String workspaceIdFilter,
			String eventTypeFilter, String fromDate, String toDate, Integer limit, Integer offset) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		addMemorySelectors(qs);
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__DELETED", "deleted"));

		if (userIdFilter != null && !userIdFilter.isBlank()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__USER_ID", "==", userIdFilter));
		}
		if (workspaceIdFilter != null && !workspaceIdFilter.isBlank()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__WORKSPACE_ID", "==", workspaceIdFilter));
		}
		if (eventTypeFilter != null && !eventTypeFilter.isBlank()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__EVENT_TYPE", "==", eventTypeFilter));
		}
		if (fromDate != null && !fromDate.isBlank()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__DATE_CREATED", ">=", fromDate));
		}
		if (toDate != null && !toDate.isBlank()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__DATE_CREATED", "<=", toDate));
		}

		qs.addOrderBy(new QueryColumnOrderBySelector(MEMORY_TABLE + "__DATE_CREATED", "DESC"));
		if (limit != null && limit > 0) {
			qs.setLimit(limit.longValue());
		}
		if (offset != null && offset > 0) {
			qs.setOffSet(offset.longValue());
		}

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Admin-only soft delete, bypassing the creator-only ownership check that
	 * {@link #deleteMemory} enforces. The audit row records the acting admin's user
	 * id, not the memory's original creator, so moderation actions are
	 * distinguishable from self-service deletes.
	 *
	 * @param memoryId    memory identifier
	 * @param adminUserId user id of the admin performing the deletion
	 * @return {@code true} if a row was updated
	 */
	public static boolean adminDeleteMemory(String memoryId, String adminUserId) {
		Map<String, Object> existing = getMemoryById(memoryId);
		if (existing == null) {
			throw new IllegalArgumentException("Memory not found: " + memoryId);
		}

		recordAudit(memoryId, "admin_delete", (String) existing.get("content"), (String) existing.get("metadata"),
				adminUserId);

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE " + MEMORY_TABLE + " SET DELETED = ?, DELETED_AT = ? WHERE MEMORY_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			ps.setBoolean(1, true);
			ps.setTimestamp(2, Utility.getCurrentSqlTimestampUTC());
			ps.setString(3, memoryId);
			int updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			removeMemoryEmbedding(resolveVectorEngineId(null, (String) existing.get("user_id")), memoryId);
			return updated > 0;
		} catch (SQLException e) {
			classLogger.error("Admin '{}' failed to delete memory '{}'.", adminUserId, memoryId, e);
			throw new IllegalArgumentException("Error deleting memory: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Admin-only usage rollup: memory counts grouped by user, optionally scoped to
	 * a workspace.
	 *
	 * @param workspaceIdFilter optional exact WORKSPACE_ID filter
	 * @return rows of {@code {user_id, memory_count}}
	 */
	public static List<Map<String, Object>> adminGetMemoryUsagePerUser(String workspaceIdFilter) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__USER_ID", "user_id"));
		qs.addSelector(prerna.query.querystruct.selectors.QueryFunctionSelector.makeFunctionSelector(
				prerna.query.querystruct.selectors.QueryFunctionHelper.COUNT, MEMORY_TABLE + "__MEMORY_ID",
				"memory_count"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__DELETED", "==", false, PixelDataType.BOOLEAN));
		if (workspaceIdFilter != null && !workspaceIdFilter.isBlank()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__WORKSPACE_ID", "==", workspaceIdFilter));
		}
		qs.addGroupBy(new QueryColumnSelector(MEMORY_TABLE + "__USER_ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector(MEMORY_TABLE + "__USER_ID", "ASC"));

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Inserts an audit row capturing a memory's prior state before an edit or
	 * delete.
	 *
	 * @param memoryId         memory identifier
	 * @param action           "edit" or "delete"
	 * @param previousContent  content prior to the change
	 * @param previousMetadata metadata prior to the change (already-serialized JSON
	 *                         string)
	 * @param userId           user performing the change
	 */
	private static void recordAudit(String memoryId, String action, String previousContent, String previousMetadata,
			String userId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "INSERT INTO " + MEMORY_AUDIT_TABLE
				+ " (AUDIT_ID, MEMORY_ID, ACTION, PREVIOUS_CONTENT, PREVIOUS_METADATA, USER_ID, DATE_CREATED) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, GUID.v7().toUUID().toString());
			ps.setString(index++, memoryId);
			ps.setString(index++, action);
			modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps,
					previousContent == null ? "" : previousContent, index++, GSON);
			if (previousMetadata != null) {
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, previousMetadata, index++, GSON);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setString(index++, userId);
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to record audit entry for memory '{}'.", memoryId, e);
			throw new IllegalArgumentException("Error recording memory audit: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Lists audit rows for a given memory, most recent first.
	 *
	 * @param memoryId memory identifier
	 * @return audit rows
	 */
	public static List<Map<String, Object>> listAuditTrail(String memoryId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MEMORY_AUDIT_TABLE + "__AUDIT_ID", "audit_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_AUDIT_TABLE + "__MEMORY_ID", "memory_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_AUDIT_TABLE + "__ACTION", "action"));
		qs.addSelector(new QueryColumnSelector(MEMORY_AUDIT_TABLE + "__PREVIOUS_CONTENT", "previous_content"));
		qs.addSelector(new QueryColumnSelector(MEMORY_AUDIT_TABLE + "__PREVIOUS_METADATA", "previous_metadata"));
		qs.addSelector(new QueryColumnSelector(MEMORY_AUDIT_TABLE + "__USER_ID", "user_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_AUDIT_TABLE + "__DATE_CREATED", "date_created"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_AUDIT_TABLE + "__MEMORY_ID", "==", memoryId));
		qs.addOrderBy(new QueryColumnOrderBySelector(MEMORY_AUDIT_TABLE + "__DATE_CREATED", "DESC"));

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Sets or clears a memory's workspace scope (sharing it to / unsharing it from
	 * a workspace). Only the memory's creator may promote it; callers must
	 * separately verify the user has edit access to the target workspace before
	 * calling this (see SecurityProjectUtils#userCanEditProject).
	 *
	 * @param memoryId    memory identifier
	 * @param userId      user attempting the promotion (must be the creator)
	 * @param workspaceId workspace to share to, or {@code null} to make personal
	 * @return {@code true} if a row was updated
	 */
	public static boolean setMemoryWorkspace(String memoryId, String userId, String workspaceId) {
		Map<String, Object> existing = getMemoryById(memoryId);
		if (existing == null) {
			throw new IllegalArgumentException("Memory not found: " + memoryId);
		}
		if (!userId.equals(existing.get("user_id"))) {
			throw new IllegalArgumentException("Only the memory's creator may promote it to a workspace.");
		}

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE " + MEMORY_TABLE + " SET WORKSPACE_ID = ?, DATE_UPDATED = ? WHERE MEMORY_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			setNullableString(ps, index++, workspaceId);
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(index++, memoryId);
			int updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			return updated > 0;
		} catch (Exception e) {
			classLogger.error("Failed to set workspace for memory '{}'.", memoryId, e);
			throw new IllegalArgumentException("Error promoting memory to workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Creates a first-class action-item, optionally linked to a parent memory.
	 *
	 * @param userId         creator of the action item (required)
	 * @param content        action item text (required)
	 * @param parentMemoryId optional parent memory this action item was derived
	 *                       from
	 * @param owner          optional assignee/owner
	 * @param dueDate        optional due date
	 * @param status         status, defaults to "open" when blank/invalid
	 * @param roomId         optional room the action item was captured in
	 * @param workspaceId    optional workspace scope
	 * @param metadata       optional JSON-serializable metadata map
	 * @return the generated action item id
	 */
	public static String createActionItem(String userId, String content, String parentMemoryId, String owner,
			Timestamp dueDate, String status, String roomId, String workspaceId, Map<String, Object> metadata) {
		if (userId == null || userId.isBlank()) {
			throw new IllegalArgumentException("userId is required to create an action item.");
		}
		if (content == null || content.isBlank()) {
			throw new IllegalArgumentException("content is required to create an action item.");
		}

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String actionItemId = GUID.v7().toUUID().toString();
		String resolvedStatus = VALID_ACTION_ITEM_STATUSES.contains(status) ? status : "open";
		Timestamp now = Utility.getCurrentSqlTimestampUTC();

		String query = "INSERT INTO " + MEMORY_ACTION_ITEM_TABLE
				+ " (ACTION_ITEM_ID, MEMORY_ID, CONTENT, OWNER, STATUS, DUE_DATE, USER_ID, ROOM_ID, WORKSPACE_ID, "
				+ "METADATA, DATE_CREATED, DATE_UPDATED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, actionItemId);
			setNullableString(ps, index++, parentMemoryId);
			modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, content, index++, GSON);
			setNullableString(ps, index++, owner);
			ps.setString(index++, resolvedStatus);
			if (dueDate != null) {
				ps.setTimestamp(index++, dueDate);
			} else {
				ps.setNull(index++, java.sql.Types.TIMESTAMP);
			}
			ps.setString(index++, userId);
			setNullableString(ps, index++, roomId);
			setNullableString(ps, index++, workspaceId);
			if (metadata != null && !metadata.isEmpty()) {
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, GSON.toJson(metadata), index++, GSON);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setTimestamp(index++, now);
			ps.setTimestamp(index++, now);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			return actionItemId;
		} catch (Exception e) {
			classLogger.error("Failed to create action item for userId '{}'.", userId, e);
			throw new IllegalArgumentException("Error creating action item: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Updates an action item's status. Only the item's creator may update it.
	 *
	 * @param actionItemId action item identifier
	 * @param userId       user attempting the update
	 * @param status       new status - must be one of open/in_progress/blocked/
	 *                     completed/cancelled
	 * @return {@code true} if a row was updated
	 */
	public static boolean updateActionItemStatus(String actionItemId, String userId, String status) {
		if (!VALID_ACTION_ITEM_STATUSES.contains(status)) {
			throw new IllegalArgumentException(
					"status must be one of " + VALID_ACTION_ITEM_STATUSES + ", got: " + status);
		}

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE " + MEMORY_ACTION_ITEM_TABLE
				+ " SET STATUS = ?, DATE_UPDATED = ? WHERE ACTION_ITEM_ID = ? AND USER_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, status);
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(index++, actionItemId);
			ps.setString(index++, userId);
			int updated = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			if (updated == 0 && getActionItemById(actionItemId) != null) {
				throw new IllegalArgumentException("Only the action item's creator may update its status.");
			}
			return updated > 0;
		} catch (SQLException e) {
			classLogger.error("Failed to update action item '{}'.", actionItemId, e);
			throw new IllegalArgumentException("Error updating action item: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Fetches one action item by id, or {@code null} if missing.
	 *
	 * @param actionItemId action item identifier
	 * @return the action item row as a map, or {@code null}
	 */
	public static Map<String, Object> getActionItemById(String actionItemId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		addActionItemSelectors(qs);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_ACTION_ITEM_TABLE + "__ACTION_ITEM_ID", "==",
				actionItemId));
		qs.setLimit(1L);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
		return results.isEmpty() ? null : results.get(0);
	}

	/**
	 * Lists action items visible to a user (owned by them or shared to a workspace
	 * they can view), with optional filters.
	 *
	 * @param userId      requesting user (required)
	 * @param workspaceId optional workspace scope the user has view access to
	 * @param assignee    optional owner/assignee filter
	 * @param status      optional status filter
	 * @param roomId      optional room filter
	 * @param limit       max rows to return, or {@code null}/&lt;=0 for no limit
	 * @param offset      rows to skip, or {@code null}/&lt;0 for no offset
	 * @return matching action item rows, most recently created first
	 */
	/**
	 * Lists action items visible to a user (owned by them or shared to a workspace
	 * they can view), with optional filters.
	 *
	 * @param userId      requesting user (required)
	 * @param workspaceId optional workspace scope the user has view access to
	 * @param assignees   optional owner/assignee filter, OR'd together
	 *                    (multi-value)
	 * @param statuses    optional status filter, OR'd together (multi-value)
	 * @param roomId      optional room filter
	 * @param search      optional case-insensitive substring match against CONTENT
	 * @param limit       max rows to return, or {@code null}/&lt;=0 for no limit
	 * @param offset      rows to skip, or {@code null}/&lt;0 for no offset
	 * @return a map with {@code action_items} (the page of rows, most recently
	 *         created first), {@code total_count}, and {@code has_more}
	 */
	public static Map<String, Object> listActionItems(String userId, String workspaceId, List<String> assignees,
			List<String> statuses, String roomId, String search, Integer limit, Integer offset) {
		if (userId == null || userId.isBlank()) {
			throw new IllegalArgumentException("userId is required to list action items.");
		}

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		addActionItemSelectors(qs);
		qs.addSelector(
				new prerna.query.querystruct.selectors.QueryOpaqueSelector("COUNT(*) OVER()", "total_row_count"));

		if (workspaceId != null && !workspaceId.isBlank()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_ACTION_ITEM_TABLE + "__WORKSPACE_ID", "==",
					workspaceId));
		} else {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_ACTION_ITEM_TABLE + "__USER_ID", "==", userId));
		}
		if (assignees != null && !assignees.isEmpty()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_ACTION_ITEM_TABLE + "__OWNER", "==", assignees));
		}
		if (statuses != null && !statuses.isEmpty()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_ACTION_ITEM_TABLE + "__STATUS", "==", statuses));
		}
		if (roomId != null && !roomId.isBlank()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_ACTION_ITEM_TABLE + "__ROOM_ID", "==", roomId));
		}
		if (search != null && !search.isBlank()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_ACTION_ITEM_TABLE + "__CONTENT", "?like",
					"%" + search + "%", PixelDataType.CONST_STRING));
		}

		qs.addOrderBy(new QueryColumnOrderBySelector(MEMORY_ACTION_ITEM_TABLE + "__DATE_CREATED", "DESC"));
		long effectiveLimit = (limit != null && limit > 0) ? limit : 0;
		long effectiveOffset = (offset != null && offset > 0) ? offset : 0;
		if (effectiveLimit > 0) {
			qs.setLimit(effectiveLimit);
		}
		if (effectiveOffset > 0) {
			qs.setOffSet(effectiveOffset);
		}

		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
		return toPagedResult(rows, "action_items", effectiveLimit, effectiveOffset);
	}

	private static void addActionItemSelectors(SelectQueryStruct qs) {
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__ACTION_ITEM_ID", "action_item_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__MEMORY_ID", "memory_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__CONTENT", "content"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__OWNER", "owner"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__STATUS", "status"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__DUE_DATE", "due_date"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__USER_ID", "user_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__ROOM_ID", "room_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__WORKSPACE_ID", "workspace_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__DATE_CREATED", "date_created"));
		qs.addSelector(new QueryColumnSelector(MEMORY_ACTION_ITEM_TABLE + "__DATE_UPDATED", "date_updated"));
	}

	private static void addMemorySelectors(SelectQueryStruct qs) {
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__MEMORY_ID", "memory_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__USER_ID", "user_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__ROOM_ID", "room_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__WORKSPACE_ID", "workspace_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__PROJECT_ID", "project_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__AGENT_ID", "agent_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__EVENT_TYPE", "event_type"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__CONTENT", "content"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__METADATA", "metadata"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__PARENT_MEMORY_ID", "parent_memory_id"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__DATE_CREATED", "date_created"));
		qs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__DATE_UPDATED", "date_updated"));
	}

	private static void setNullableString(PreparedStatement ps, int index, String value) throws SQLException {
		if (value != null && !value.isBlank()) {
			ps.setString(index, value);
		} else {
			ps.setNull(index, java.sql.Types.VARCHAR);
		}
	}

	private static final String MEMORY_META_TABLE = "MEMORY_META";
	private static final String MEMORY_METAKEYS_TABLE = "MEMORY_METAKEYS";

	/**
	 * Fetches the distinct metakey/metavalue combinations in use across the
	 * caller's visible memories, with a count of how many memories carry each one -
	 * for populating a filter UI (e.g. the same "Filters" pill/count pattern the
	 * Engine/Project catalog pages use), mirroring
	 * {@code SecurityEngineUtils#getAvailableMetaValues}/{@code ENGINEMETA}'s
	 * shape.
	 *
	 * @param userId      requesting user (required)
	 * @param workspaceId optional workspace scope the user has view access to (same
	 *                    visibility rule as {@link #listMemories}); when omitted,
	 *                    scoped to the caller's own memories
	 * @return rows shaped {@code {metakey, metavalue, count}}
	 */
	public static List<Map<String, Object>> getAvailableMemoryMetaValues(String userId, String workspaceId) {
		if (userId == null || userId.isBlank()) {
			throw new IllegalArgumentException("userId is required to fetch available memory metadata values.");
		}

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();

		SelectQueryStruct visibleMemoryIdsQs = new SelectQueryStruct();
		visibleMemoryIdsQs.addSelector(new QueryColumnSelector(MEMORY_TABLE + "__MEMORY_ID"));
		AndQueryFilter visibilityFilter = new AndQueryFilter();
		visibilityFilter.addFilter(
				SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__DELETED", "==", false, PixelDataType.BOOLEAN));
		if (workspaceId != null && !workspaceId.isBlank()) {
			visibilityFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__WORKSPACE_ID", "==", workspaceId));
		} else {
			visibilityFilter.addFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_TABLE + "__USER_ID", "==", userId));
		}
		visibleMemoryIdsQs.addExplicitFilter(visibilityFilter);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MEMORY_META_TABLE + "__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector(MEMORY_META_TABLE + "__METAVALUE", "metavalue"));
		prerna.query.querystruct.selectors.QueryFunctionSelector countSelector = new prerna.query.querystruct.selectors.QueryFunctionSelector();
		countSelector.setAlias("count");
		countSelector.setFunction(prerna.query.querystruct.selectors.QueryFunctionHelper.COUNT);
		countSelector.addInnerSelector(new QueryColumnSelector(MEMORY_META_TABLE + "__MEMORY_ID"));
		qs.addSelector(countSelector);
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToSubQuery(MEMORY_META_TABLE + "__MEMORY_ID", "==", visibleMemoryIdsQs));
		qs.addGroupBy(new QueryColumnSelector(MEMORY_META_TABLE + "__METAKEY"));
		qs.addGroupBy(new QueryColumnSelector(MEMORY_META_TABLE + "__METAVALUE"));

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Fetches a memory's custom metadata (MEMORY_META rows) as metakey -&gt;
	 * ordered list of values, for display alongside a memory (e.g. in the Settings
	 * UI's memory detail view). Returns an empty map if the memory has no custom
	 * metadata.
	 *
	 * @param memoryId memory identifier
	 * @return map of metakey to its values, in METAORDER
	 */
	public static Map<String, List<String>> getMemoryMeta(String memoryId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MEMORY_META_TABLE + "__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector(MEMORY_META_TABLE + "__METAVALUE", "metavalue"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MEMORY_META_TABLE + "__MEMORY_ID", "==", memoryId));
		qs.addOrderBy(new QueryColumnOrderBySelector(MEMORY_META_TABLE + "__METAORDER", "ASC"));

		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
		Map<String, List<String>> meta = new LinkedHashMap<>();
		for (Map<String, Object> row : rows) {
			String key = String.valueOf(row.get("metakey"));
			meta.computeIfAbsent(key, k -> new ArrayList<>()).add(String.valueOf(row.get("metavalue")));
		}
		return meta;
	}

	/**
	 * Normalizes a metaFilters value (single scalar or a Collection) into a flat
	 * list of string values to insert, one MEMORY_META row per value.
	 */
	private static List<String> toMetaValues(Object value) {
		if (value instanceof Collection<?> collection) {
			List<String> values = new ArrayList<>();
			for (Object v : collection) {
				if (v != null) {
					values.add(String.valueOf(v));
				}
			}
			return values;
		}
		return value == null ? List.of() : List.of(String.valueOf(value));
	}

	/**
	 * Inserts flat, filterable metadata entries into MEMORY_META - mirrors
	 * PROMPTMETA/ENGINEMETA/PROJECTMETA's METAKEY/METAVALUE/METAORDER shape (the
	 * platform-wide convention for app-definable filter dimensions, not just a
	 * Prompt-specific one). Registers each metakey in MEMORY_METAKEYS first
	 * (best-effort, for display metadata) via {@link #ensureMemoryMetaKeyExists}.
	 *
	 * @param memoryId    memory identifier the metadata belongs to
	 * @param metaFilters map of metakey -&gt; one or more metavalues
	 */
	private static void insertMemoryMeta(String memoryId, Map<String, Object> metaFilters) {
		for (String metaKey : metaFilters.keySet()) {
			ensureMemoryMetaKeyExists(metaKey);
		}

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "INSERT INTO " + MEMORY_META_TABLE + " (MEMORY_ID, METAKEY, METAVALUE, METAORDER) "
				+ "VALUES (?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			for (Map.Entry<String, Object> entry : metaFilters.entrySet()) {
				int order = 0;
				for (String metaValue : toMetaValues(entry.getValue())) {
					int index = 1;
					ps.setString(index++, memoryId);
					ps.setString(index++, entry.getKey());
					ps.setString(index++, metaValue);
					ps.setInt(index++, order++);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert memory metadata for memory '{}'.", memoryId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Ensures a metakey exists in MEMORY_METAKEYS, copying it from the security
	 * database's USERMETAKEYS registry (the same platform-wide metakey registry
	 * ENGINEMETAKEYS/PROJECTMETAKEYS/PROMPTMETAKEYS sync from) if not already
	 * present. Best-effort: a metakey not yet registered in USERMETAKEYS is
	 * silently skipped here - the MEMORY_META row is still inserted by the caller,
	 * it just will not have display metadata (order/options) until an admin
	 * registers the key.
	 *
	 * @param metaKey the metakey to ensure exists
	 */
	private static void ensureMemoryMetaKeyExists(String metaKey) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct existsQs = new SelectQueryStruct();
		existsQs.addSelector(new QueryColumnSelector(MEMORY_METAKEYS_TABLE + "__METAKEY"));
		existsQs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(MEMORY_METAKEYS_TABLE + "__METAKEY", "==", metaKey));
		List<Map<String, Object>> existing = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, existsQs);
		if (!existing.isEmpty()) {
			return;
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct userMetaQs = new SelectQueryStruct();
		userMetaQs.addSelector(new QueryColumnSelector("USERMETAKEYS__METAKEY"));
		userMetaQs.addSelector(new QueryColumnSelector("USERMETAKEYS__SINGLEMULTI"));
		userMetaQs.addSelector(new QueryColumnSelector("USERMETAKEYS__DISPLAYOPTIONS"));
		userMetaQs.addSelector(new QueryColumnSelector("USERMETAKEYS__DEFAULTVALUES"));
		userMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETAKEYS__METAKEY", "==", metaKey));
		List<Map<String, Object>> userMetaKeys = QueryExecutionUtility.flushRsToMap(securityDb, userMetaQs);
		if (userMetaKeys.isEmpty()) {
			return;
		}
		Map<String, Object> userMetaKey = userMetaKeys.get(0);

		String insertQuery = modelInferenceLogsDb.getQueryUtil().createInsertPreparedStatementString(
				MEMORY_METAKEYS_TABLE, new String[] { "METAKEY", "SINGLEMULTI", "DISPLAYOPTIONS", "DEFAULTVALUES" });
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(insertQuery);
			ps.setString(1, (String) userMetaKey.get("metakey"));
			ps.setString(2, (String) userMetaKey.get("singlemulti"));
			ps.setString(3, (String) userMetaKey.get("displayoptions"));
			ps.setString(4, (String) userMetaKey.get("defaultvalues"));
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to copy metakey '{}' into MEMORY_METAKEYS.", metaKey, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

}
