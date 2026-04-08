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
package prerna.engine.impl.model.inferencetracking.reactors.memory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import prerna.util.Utility;

/**
 * Reactor that performs semantic recall over a user's stored memories.
 * <p>
 * Uses cosine similarity over in-memory embeddings to find the top-K most
 * relevant memories for a given query. Embeddings are generated via the user's
 * configured embedding model ({@link IModelEngine#embeddings}).
 * <p>
 * If no embeddings exist yet for a memory, they are computed lazily during recall
 * and cached in the MEMORY_EMBEDDING table for future queries.
 * <p>
 * Pixel usage:
 * <pre>
 *   RecallMemory(content=["What database does the user prefer?"], limit=[5]);
 *   RecallMemory(content=["project deadlines"], memoryType=["FACT"], engine=["embedding-model-id"]);
 * </pre>
 *
 * @see ModelInferenceLogsUtils#getMemoriesForUser(String, String, long, long)
 * @see ModelInferenceLogsUtils#getMemoryEmbeddings(String, String)
 * @see ModelInferenceLogsUtils#insertMemoryEmbedding(String, byte[], String)
 */
public class RecallMemoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RecallMemoryReactor.class);

	private static final String MEMORY_TYPE_KEY = "memoryType";

	public RecallMemoryReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.CONTENT.getKey(),
				MEMORY_TYPE_KEY,
				ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.ENGINE.getKey()
		};
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	/**
	 * Performs semantic recall by embedding the query, then computing cosine
	 * similarity against all of the user's memory embeddings.
	 * <p>
	 * Required parameters:
	 * <ul>
	 *   <li>{@code content} — the query text to match against memories</li>
	 * </ul>
	 * Optional parameters:
	 * <ul>
	 *   <li>{@code memoryType} — filter by memory category before similarity search</li>
	 *   <li>{@code limit} — max results to return (default: 5)</li>
	 *   <li>{@code engine} — embedding model engine ID (uses default if not provided)</li>
	 * </ul>
	 *
	 * @return {@link NounMetadata} map containing {@code memories} list (sorted by relevance)
	 * @throws IllegalArgumentException if user is not logged in or query is empty
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in to recall memories");
		}
		if (user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User authentication token is missing");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String query = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.CONTENT.getKey()));
		if (query == null || query.trim().isEmpty()) {
			throw new IllegalArgumentException("Query content is required for memory recall");
		}

		String memoryType = this.keyValue.get(MEMORY_TYPE_KEY);

		int limit = 5;
		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		if (limitStr != null && !limitStr.trim().isEmpty()) {
			limit = Integer.parseInt(limitStr);
		}

		String embeddingEngineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		try {
			List<Map<String, Object>> results = semanticRecall(userId, query, memoryType, limit, embeddingEngineId);

			Map<String, Object> output = new HashMap<>();
			output.put("memories", results);
			output.put("count", results.size());
			return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to recall memories for user '{}'.", userId, e);
			throw new IllegalArgumentException("Failed to recall memories: " + e.getMessage());
		}
	}

	/**
	 * Core semantic recall logic: embed the query, load user embeddings, compute
	 * cosine similarity, and return the top-K results.
	 *
	 * @param userId           user identifier
	 * @param query            the text query to embed and match
	 * @param memoryType       optional type filter (null for all)
	 * @param limit            max number of results
	 * @param embeddingEngineId optional embedding model ID (null uses default)
	 * @return list of memory maps sorted by relevance (highest score first)
	 */
	private List<Map<String, Object>> semanticRecall(String userId, String query,
			String memoryType, int limit, String embeddingEngineId) {
		// 1. Get all active memories for user (with optional type filter)
		List<Map<String, Object>> allMemories = ModelInferenceLogsUtils.getMemoriesForUser(
				userId, memoryType, 10000, 0);

		if (allMemories.isEmpty()) {
			return new ArrayList<>();
		}

		// 2. If no embedding engine specified, fall back to text-only search
		if (embeddingEngineId == null || embeddingEngineId.trim().isEmpty()) {
			// Text-based fallback: return memories containing the query terms
			return textFallbackSearch(allMemories, query, limit);
		}

		// 3. Embed the query
		IModelEngine embeddingEngine = (IModelEngine) Utility.getEngine(embeddingEngineId);
		EmbeddingsModelEngineResponse embResponse = embeddingEngine.embeddings(
				List.of(query), this.insight, new HashMap<>());
		List<List<Double>> queryEmbeddings = embResponse.getResponse();
		if (queryEmbeddings == null || queryEmbeddings.isEmpty()) {
			classLogger.warn("Embedding engine returned empty response for query");
			return textFallbackSearch(allMemories, query, limit);
		}
		double[] queryVector = queryEmbeddings.get(0).stream().mapToDouble(Double::doubleValue).toArray();

		// 4. Load existing embeddings from MEMORY_EMBEDDING table
		List<Map<String, Object>> existingEmbeddings = ModelInferenceLogsUtils.getMemoryEmbeddings(
				userId, embeddingEngineId);

		// Build lookup: memoryId -> embedding vector
		Map<String, double[]> embeddingMap = new HashMap<>();
		for (Map<String, Object> embRow : existingEmbeddings) {
			String memId = (String) embRow.get("memory_id");
			byte[] embBytes = (byte[]) embRow.get("embedding");
			if (embBytes != null) {
				embeddingMap.put(memId, deserializeEmbedding(embBytes));
			}
		}

		// 5. Generate missing embeddings and cache them
		List<String> missingIds = new ArrayList<>();
		List<String> missingContents = new ArrayList<>();
		for (Map<String, Object> mem : allMemories) {
			String memId = (String) mem.get("memory_id");
			if (!embeddingMap.containsKey(memId)) {
				missingIds.add(memId);
				missingContents.add((String) mem.get("content"));
			}
		}
		if (!missingContents.isEmpty()) {
			try {
				EmbeddingsModelEngineResponse batchResponse = embeddingEngine.embeddings(
						missingContents, this.insight, new HashMap<>());
				List<List<Double>> batchVectors = batchResponse.getResponse();
				if (batchVectors != null) {
					for (int i = 0; i < Math.min(missingIds.size(), batchVectors.size()); i++) {
						double[] vec = batchVectors.get(i).stream().mapToDouble(Double::doubleValue).toArray();
						embeddingMap.put(missingIds.get(i), vec);
						// Cache the embedding for future recall
						ModelInferenceLogsUtils.insertMemoryEmbedding(
								missingIds.get(i), serializeEmbedding(vec), embeddingEngineId);
					}
				}
			} catch (Exception e) {
				classLogger.warn("Failed to generate batch embeddings, falling back to text search", e);
				return textFallbackSearch(allMemories, query, limit);
			}
		}

		// 6. Cosine similarity search using a min-heap for top-K
		PriorityQueue<ScoredMemory> topK = new PriorityQueue<>(limit + 1,
				Comparator.comparingDouble(s -> s.score));
		for (Map<String, Object> mem : allMemories) {
			String memId = (String) mem.get("memory_id");
			double[] memVector = embeddingMap.get(memId);
			if (memVector == null) {
				continue;
			}
			double score = cosineSimilarity(queryVector, memVector);
			topK.offer(new ScoredMemory(mem, score));
			if (topK.size() > limit) {
				topK.poll();
			}
		}

		// 7. Return results sorted by score descending
		List<Map<String, Object>> results = new ArrayList<>();
		while (!topK.isEmpty()) {
			ScoredMemory sm = topK.poll();
			Map<String, Object> enriched = new HashMap<>(sm.memory);
			enriched.put("relevance_score", sm.score);
			results.add(0, enriched);
		}
		return results;
	}

	/**
	 * Text-based fallback when no embedding engine is available.
	 * Returns memories whose content contains any of the query terms (case-insensitive).
	 */
	private List<Map<String, Object>> textFallbackSearch(List<Map<String, Object>> memories,
			String query, int limit) {
		String queryLower = query.toLowerCase();
		String[] terms = queryLower.split("\\s+");
		List<Map<String, Object>> matches = new ArrayList<>();
		for (Map<String, Object> mem : memories) {
			String content = ((String) mem.get("content")).toLowerCase();
			for (String term : terms) {
				if (content.contains(term)) {
					matches.add(mem);
					break;
				}
			}
			if (matches.size() >= limit) {
				break;
			}
		}
		return matches;
	}

	/**
	 * Computes cosine similarity between two vectors.
	 *
	 * @param a first vector
	 * @param b second vector
	 * @return cosine similarity score, or 0.0 if vectors have different dimensions
	 */
	static double cosineSimilarity(double[] a, double[] b) {
		if (a.length != b.length) {
			classLogger.warn("Embedding dimension mismatch: query={}, memory={}. Returning 0.", a.length, b.length);
			return 0.0;
		}
		double dot = 0.0;
		double normA = 0.0;
		double normB = 0.0;
		for (int i = 0; i < a.length; i++) {
			dot += a[i] * b[i];
			normA += a[i] * a[i];
			normB += b[i] * b[i];
		}
		double denominator = Math.sqrt(normA) * Math.sqrt(normB);
		return denominator == 0.0 ? 0.0 : dot / denominator;
	}

	/**
	 * Serializes a double[] embedding vector to a byte[] for BLOB storage.
	 *
	 * @param embedding the vector to serialize
	 * @return byte array representation
	 */
	static byte[] serializeEmbedding(double[] embedding) {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos)) {
			dos.writeInt(embedding.length);
			for (double val : embedding) {
				dos.writeDouble(val);
			}
			dos.flush();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new IllegalStateException("Failed to serialize embedding", e);
		}
	}

	/**
	 * Deserializes a byte[] from BLOB storage back to a double[] vector.
	 *
	 * @param bytes the byte array to deserialize
	 * @return the reconstructed vector
	 */
	static double[] deserializeEmbedding(byte[] bytes) {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				DataInputStream dis = new DataInputStream(bais)) {
			int length = dis.readInt();
			double[] embedding = new double[length];
			for (int i = 0; i < length; i++) {
				embedding[i] = dis.readDouble();
			}
			return embedding;
		} catch (IOException e) {
			throw new IllegalStateException("Failed to deserialize embedding", e);
		}
	}

	/** Internal holder pairing a memory row with its similarity score. */
	private static class ScoredMemory {
		final Map<String, Object> memory;
		final double score;

		ScoredMemory(Map<String, Object> memory, double score) {
			this.memory = memory;
			this.score = score;
		}
	}

}
