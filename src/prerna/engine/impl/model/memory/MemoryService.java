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
package prerna.engine.impl.model.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;

/**
 * Stateless service class for the persistent user memory system.
 * <p>
 * Provides recall (pre-turn), extraction (post-turn), deduplication,
 * and formatting — all without coupling to Room, Reactor, or MCP tools.
 * Any caller (AskPlaygroundReactor, COT agents, admin reactors) can use
 * this service directly.
 * <p>
 * Memory is stored in the ModelInferenceLogsDb MEMORY table and is
 * user-scoped: every query includes a {@code USER_ID} filter.
 *
 * <h3>Design Principles</h3>
 * <ul>
 *   <li>Recall is fast (SQL text search, ~5ms) — safe for the critical path</li>
 *   <li>Extraction is async — never blocks user-facing response</li>
 *   <li>Deduplication resolves contradictions (blue → red) via LLM decision</li>
 *   <li>Graceful degradation — failures are logged and swallowed, never crash the conversation</li>
 * </ul>
 */
public class MemoryService {

	private static final Logger classLogger = LogManager.getLogger(MemoryService.class);
	private static final Gson GSON = new GsonBuilder().create();

	// =========================================================================
	// 1. RECALL — Pre-turn, fast, SQL-based
	// =========================================================================

	/**
	 * Searches the user's active memories by keywords extracted from the query.
	 * Returns the most relevant memories within the token budget.
	 *
	 * @param userId      authenticated user ID
	 * @param query       the user's current message text
	 * @param limit       max memories to return
	 * @param tokenBudget max estimated tokens for the formatted memory block
	 * @return list of relevant memory maps, or empty list if none match
	 */
	public List<Map<String, Object>> recallRelevant(String userId, String query, int limit, int tokenBudget) {
		if (userId == null || query == null || query.trim().isEmpty()) {
			return new ArrayList<>();
		}

		try {
			List<String> keywords = extractKeywords(query);
			if (keywords.isEmpty()) {
				return new ArrayList<>();
			}

			List<Map<String, Object>> candidates = ModelInferenceLogsUtils
					.searchMemoriesByKeywords(userId, keywords, limit * 2);

			if (candidates.isEmpty()) {
				return new ArrayList<>();
			}

			// enforce token budget
			List<Map<String, Object>> result = new ArrayList<>();
			int tokensUsed = 0;
			for (Map<String, Object> memory : candidates) {
				String content = getContentString(memory);
				int memTokens = estimateTokens(content);
				if (tokensUsed + memTokens > tokenBudget) {
					break;
				}
				result.add(memory);
				tokensUsed += memTokens;
				if (result.size() >= limit) {
					break;
				}
			}

			// update access tracking for recalled memories
			if (!result.isEmpty()) {
				List<String> recalledIds = result.stream()
						.map(m -> (String) m.get("memory_id"))
						.collect(Collectors.toList());
				try {
					ModelInferenceLogsUtils.touchMemories(recalledIds);
				} catch (Exception e) {
					classLogger.debug("Failed to update access tracking for recalled memories", e);
				}
			}

			return result;
		} catch (Exception e) {
			classLogger.warn("Memory recall failed for user '{}', returning empty", userId, e);
			return new ArrayList<>();
		}
	}

	// =========================================================================
	// 2. FORMAT — Turn memory list into injectable text block
	// =========================================================================

	/**
	 * Formats a list of recalled memories into a compact text block for LLM injection.
	 *
	 * @param memories the recalled memory maps
	 * @return formatted string ready to pass as {@code memory_context}, or null if empty
	 */
	public String formatForInjection(List<Map<String, Object>> memories) {
		if (memories == null || memories.isEmpty()) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		sb.append(MemoryConstants.INJECTION_PREAMBLE).append("\n\n");

		for (Map<String, Object> memory : memories) {
			String content = getContentString(memory);
			sb.append("- ").append(content).append("\n");
		}

		return sb.toString().trim();
	}

	// =========================================================================
	// 3. EXTRACT — Post-turn, async, LLM-based fact extraction
	// =========================================================================

	/**
	 * Extracts memorable facts from a conversation turn and stores them.
	 * This method is designed to be called asynchronously after the LLM response
	 * has been returned to the user.
	 *
	 * @param userId            authenticated user ID
	 * @param roomId            source room ID (for provenance)
	 * @param userMessage       the user's input text
	 * @param assistantResponse the LLM's response text
	 * @param modelEngine       the model engine to use for extraction LLM call
	 * @param insight           the insight context for the LLM call
	 */
	public void extractAndStore(String userId, String roomId, String userMessage,
			String assistantResponse, IModelEngine modelEngine, Insight insight) {
		if (userId == null || userMessage == null || userMessage.trim().isEmpty()) {
			return;
		}

		try {
			// call LLM to extract facts
			List<ExtractedFact> facts = callExtractionLLM(userMessage, assistantResponse, modelEngine, insight);

			if (facts.isEmpty()) {
				return;
			}

			// deduplicate and store each fact
			deduplicateAndStore(userId, roomId, facts, modelEngine, insight);
		} catch (Exception e) {
			classLogger.warn("Memory extraction failed for user '{}' in room '{}'. Conversation unaffected.",
					userId, roomId, e);
		}
	}

	/**
	 * Calls the LLM to extract memorable facts from a conversation turn.
	 */
	private List<ExtractedFact> callExtractionLLM(String userMessage, String assistantResponse,
			IModelEngine modelEngine, Insight insight) {
		String prompt = MemoryConstants.EXTRACTION_PROMPT
				.replace("{USER_MESSAGE}", truncate(userMessage, 2000))
				.replace("{ASSISTANT_RESPONSE}", truncate(assistantResponse != null ? assistantResponse : "", 2000));

		Map<String, Object> params = new HashMap<>();
		params.put("temperature", 0.1);
		params.put("max_new_tokens", 1024);

		AskModelEngineResponse response = modelEngine.ask(prompt, null, insight, params);
		if (response == null || response.getStringResponse() == null) {
			return new ArrayList<>();
		}

		return parseExtractionResponse(response.getStringResponse());
	}

	/**
	 * Parses the LLM extraction response JSON into a list of facts.
	 */
	private List<ExtractedFact> parseExtractionResponse(String llmResponse) {
		List<ExtractedFact> facts = new ArrayList<>();
		try {
			// extract JSON array from response (may have surrounding text)
			String json = extractJsonArray(llmResponse);
			if (json == null) {
				return facts;
			}

			JsonArray arr = JsonParser.parseString(json).getAsJsonArray();
			for (JsonElement elem : arr) {
				JsonObject obj = elem.getAsJsonObject();
				String content = getJsonString(obj, "content");
				String type = getJsonString(obj, "type");
				if (content != null && !content.trim().isEmpty()) {
					if (type == null || type.trim().isEmpty()) {
						type = "FACT";
					}
					facts.add(new ExtractedFact(content.trim(), type.toUpperCase().trim()));
				}
			}
		} catch (Exception e) {
			classLogger.debug("Failed to parse extraction response: {}", llmResponse, e);
		}
		return facts;
	}

	// =========================================================================
	// 4. DEDUP — Write-time deduplication with LLM decision
	// =========================================================================

	/**
	 * For each extracted fact, searches for similar existing memories and decides
	 * whether to ADD, UPDATE (resolve contradiction), or NOOP (skip duplicate).
	 */
	private void deduplicateAndStore(String userId, String roomId, List<ExtractedFact> facts,
			IModelEngine modelEngine, Insight insight) {
		for (ExtractedFact fact : facts) {
			try {
				deduplicateAndStoreSingle(userId, roomId, fact, modelEngine, insight);
			} catch (Exception e) {
				classLogger.debug("Failed to dedup/store memory '{}' for user '{}'",
						fact.content, userId, e);
			}
		}
	}

	private void deduplicateAndStoreSingle(String userId, String roomId, ExtractedFact fact,
			IModelEngine modelEngine, Insight insight) {
		// search for similar existing memories by keywords
		List<String> factKeywords = extractKeywords(fact.content);
		if (factKeywords.isEmpty()) {
			// no meaningful keywords — just store directly
			insertNewMemory(userId, roomId, fact, null);
			return;
		}

		List<Map<String, Object>> similar = ModelInferenceLogsUtils
				.searchMemoriesByKeywords(userId, factKeywords, 5);

		if (similar.isEmpty()) {
			// fast path: no similar memories — just INSERT
			insertNewMemory(userId, roomId, fact, null);
			return;
		}

		// similar memories found — ask LLM to decide
		DedupDecision decision = callDedupLLM(fact, similar, modelEngine, insight);

		switch (decision.action) {
		case "UPDATE":
			if (decision.updateMemoryId != null && !decision.updateMemoryId.isEmpty()) {
				// soft-delete the old memory
				try {
					ModelInferenceLogsUtils.deleteMemory(decision.updateMemoryId, userId);
				} catch (Exception e) {
					classLogger.debug("Failed to soft-delete superseded memory '{}'", decision.updateMemoryId, e);
				}
				// insert new with supersedes metadata
				Map<String, Object> metadata = new HashMap<>();
				metadata.put("supersedes", decision.updateMemoryId);
				metadata.put("reason", decision.reason);
				insertNewMemory(userId, roomId, fact, metadata);
			} else {
				// no specific ID to update — treat as ADD
				insertNewMemory(userId, roomId, fact, null);
			}
			break;
		case "NOOP":
			classLogger.debug("Dedup NOOP for fact '{}' — duplicate of existing memory", fact.content);
			break;
		case "ADD":
		default:
			insertNewMemory(userId, roomId, fact, null);
			break;
		}
	}

	/**
	 * Calls the LLM to decide ADD/UPDATE/NOOP for a new fact against existing memories.
	 */
	private DedupDecision callDedupLLM(ExtractedFact fact, List<Map<String, Object>> similar,
			IModelEngine modelEngine, Insight insight) {
		// build the existing memories block
		StringBuilder existingBlock = new StringBuilder();
		for (Map<String, Object> mem : similar) {
			String memId = (String) mem.get("memory_id");
			String content = getContentString(mem);
			existingBlock.append("[id=").append(memId).append("] \"").append(content).append("\"\n");
		}

		String prompt = MemoryConstants.DEDUP_PROMPT
				.replace("{NEW_FACT}", fact.content)
				.replace("{EXISTING_MEMORIES}", existingBlock.toString().trim());

		Map<String, Object> params = new HashMap<>();
		params.put("temperature", 0.0);
		params.put("max_new_tokens", 256);

		try {
			AskModelEngineResponse response = modelEngine.ask(prompt, null, insight, params);
			if (response != null && response.getStringResponse() != null) {
				return parseDedupResponse(response.getStringResponse());
			}
		} catch (Exception e) {
			classLogger.debug("Dedup LLM call failed, defaulting to ADD", e);
		}

		// default: ADD on failure (conservative — don't lose data)
		return new DedupDecision("ADD", null, "dedup LLM call failed");
	}

	/**
	 * Parses the LLM dedup response JSON.
	 */
	private DedupDecision parseDedupResponse(String llmResponse) {
		try {
			String json = extractJsonObject(llmResponse);
			if (json == null) {
				return new DedupDecision("ADD", null, "no JSON found in response");
			}
			JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
			String action = getJsonString(obj, "action");
			String updateId = getJsonString(obj, "updateMemoryId");
			String reason = getJsonString(obj, "reason");

			if (action == null) {
				return new DedupDecision("ADD", null, "no action in response");
			}
			action = action.toUpperCase().trim();
			if (!"ADD".equals(action) && !"UPDATE".equals(action) && !"NOOP".equals(action)) {
				return new DedupDecision("ADD", null, "unrecognized action: " + action);
			}
			if ("null".equalsIgnoreCase(updateId)) {
				updateId = null;
			}
			return new DedupDecision(action, updateId, reason);
		} catch (Exception e) {
			classLogger.debug("Failed to parse dedup response: {}", llmResponse, e);
			return new DedupDecision("ADD", null, "parse failed");
		}
	}

	// =========================================================================
	// 5. STORAGE — Insert new memory records
	// =========================================================================

	private void insertNewMemory(String userId, String roomId, ExtractedFact fact, Map<String, Object> metadata) {
		String memoryId = UUID.randomUUID().toString();
		String metadataJson = metadata != null ? GSON.toJson(metadata) : null;
		ModelInferenceLogsUtils.insertMemory(memoryId, userId, roomId, fact.type, fact.content, metadataJson);
		classLogger.debug("Stored new memory '{}' ({}) for user '{}'", memoryId, fact.type, userId);
	}

	// =========================================================================
	// UTILITY METHODS
	// =========================================================================

	/**
	 * Extracts meaningful keywords from a text string by splitting on whitespace,
	 * lowering case, removing stop words, and filtering short tokens.
	 */
	List<String> extractKeywords(String text) {
		if (text == null || text.trim().isEmpty()) {
			return new ArrayList<>();
		}
		// remove punctuation and split on whitespace
		String cleaned = text.replaceAll("[^a-zA-Z0-9\\s]", " ");
		String[] tokens = cleaned.toLowerCase().split("\\s+");

		List<String> keywords = new ArrayList<>();
		for (String token : tokens) {
			if (token.length() >= 3 && !MemoryConstants.STOP_WORDS.contains(token)) {
				keywords.add(token);
			}
		}
		// cap at 10 keywords to avoid overly broad queries
		if (keywords.size() > 10) {
			keywords = keywords.subList(0, 10);
		}
		return keywords;
	}

	/**
	 * Estimates token count from text length.
	 * Uses a simple heuristic: ~4 characters per token.
	 */
	private int estimateTokens(String text) {
		if (text == null) {
			return 0;
		}
		return Math.max(1, text.length() / MemoryConstants.CHARS_PER_TOKEN_ESTIMATE);
	}

	/**
	 * Safely extracts the content string from a memory map,
	 * handling CLOBs, nulls, and type variations.
	 */
	private String getContentString(Map<String, Object> memory) {
		Object content = memory.get("content");
		if (content == null) {
			return "";
		}
		return content.toString();
	}

	/**
	 * Truncates text to a maximum length, appending "..." if truncated.
	 */
	private String truncate(String text, int maxLength) {
		if (text == null) {
			return "";
		}
		if (text.length() <= maxLength) {
			return text;
		}
		return text.substring(0, maxLength) + "...";
	}

	/**
	 * Extracts the first JSON array from a string that may contain surrounding text.
	 */
	private String extractJsonArray(String text) {
		if (text == null) {
			return null;
		}
		int start = text.indexOf('[');
		int end = text.lastIndexOf(']');
		if (start >= 0 && end > start) {
			return text.substring(start, end + 1);
		}
		return null;
	}

	/**
	 * Extracts the first JSON object from a string that may contain surrounding text.
	 */
	private String extractJsonObject(String text) {
		if (text == null) {
			return null;
		}
		int start = text.indexOf('{');
		int end = text.lastIndexOf('}');
		if (start >= 0 && end > start) {
			return text.substring(start, end + 1);
		}
		return null;
	}

	/**
	 * Safely reads a string field from a JsonObject.
	 */
	private String getJsonString(JsonObject obj, String key) {
		if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) {
			return null;
		}
		return obj.get(key).getAsString();
	}

	// =========================================================================
	// INNER CLASSES
	// =========================================================================

	/** An extracted fact from a conversation turn. */
	static class ExtractedFact {
		final String content;
		final String type;

		ExtractedFact(String content, String type) {
			this.content = content;
			this.type = type;
		}
	}

	/** Result of a deduplication LLM decision. */
	private static class DedupDecision {
		final String action;
		final String updateMemoryId;
		final String reason;

		DedupDecision(String action, String updateMemoryId, String reason) {
			this.action = action;
			this.updateMemoryId = updateMemoryId;
			this.reason = reason;
		}
	}
}
