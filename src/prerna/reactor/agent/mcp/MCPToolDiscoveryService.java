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
package prerna.reactor.agent.mcp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.util.Utility;

public class MCPToolDiscoveryService {

	private static final Logger classLogger = LogManager.getLogger(MCPToolDiscoveryService.class);

	private static final Pattern WHITESPACE = Pattern.compile("\\s+");
	private static final Pattern NON_ALPHANUMERIC = Pattern.compile("[^A-Za-z0-9]+");
	private static final Pattern CAMEL_BOUNDARY = Pattern.compile("(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])");
	private static final Pattern LETTER_DIGIT_BOUNDARY = Pattern
			.compile("(?<=[A-Za-z])(?=[0-9])|(?<=[0-9])(?=[A-Za-z])");

	/** Per-engine/project cache of tool index entries. Keyed by engine or project ID. */
	private static final ConcurrentMap<String, List<ToolIndexEntry>> toolCache = new ConcurrentHashMap<>();

	/**
	 * Invalidates cached tools for a specific engine or project.
	 * Call this when MCP tools are created, modified, or deleted for that engine/project.
	 */
	public static void invalidate(String id) {
		if (id != null) {
			toolCache.remove(id);
		}
	}

	/**
	 * Searches MCP tools across engines and projects/apps the user can access.
	 * Relevance is computed only from the MCP tool name and description, matching
	 * the ticket requirement.
	 *
	 * @param user        logged-in user used for engine/project security filtering.
	 * @param query       search text supplied by the caller.
	 * @param engineTypes optional engine catalog type filters.
	 * @param limit       maximum number of results to return.
	 * @param offset      number of matching results to skip.
	 * @return response map with query, limit, offset, total, and ranked results.
	 */
	public Map<String, Object> search(User user, String query, List<String> engineTypes, Integer limit,
			Integer offset) {
		NormalizedText normalizedQuery = normalize(query.trim());
		Set<String> typeFilters = normalizeEngineTypes(engineTypes);
		List<ToolIndexEntry> index = buildIndex(user, typeFilters);
		TokenStats tokenStats = buildTokenStats(index);

		List<ScoredTool> scoredTools = new ArrayList<>();
		for (ToolIndexEntry entry : index) {
			double score = getMatchScore(entry, normalizedQuery, tokenStats);
			if (score > 0) {
				scoredTools.add(new ScoredTool(entry, score));
			}
		}

		scoredTools.sort(Comparator.comparingDouble(ScoredTool::score).reversed()
				.thenComparing(tool -> tool.entry.toolName.toLowerCase())
				.thenComparing(tool -> tool.entry.engineName.toLowerCase()));

		int total = scoredTools.size();
		int fromIndex = Math.min(offset, total);
		int toIndex = Math.min(fromIndex + limit, total);

		List<Map<String, Object>> results = new ArrayList<>();
		for (ScoredTool scoredTool : scoredTools.subList(fromIndex, toIndex)) {
			results.add(scoredTool.entry.toResultMap());
		}
		return buildResponse(query, limit, offset, total, results);
	}

	private Map<String, Object> buildResponse(String query, int limit, int offset, int total,
			List<Map<String, Object>> results) {

		Map<String, Object> response = new LinkedHashMap<>();
		response.put("query", query);
		response.put("limit", limit);
		response.put("offset", offset);
		response.put("total", total);
		response.put("results", results);
		return response;
	}

	/**
	 * Returns the first matching MCP tools for a missing capability recommendation.
	 *
	 * @param user        logged-in user used for engine/project security filtering.
	 * @param query       missing capability or user request text.
	 * @param engineTypes optional engine catalog type filters.
	 * @param limit       maximum number of recommended tools to return.
	 * @return response map containing ranked recommendation results.
	 */
	public Map<String, Object> recommend(User user, String query, List<String> engineTypes, Integer limit) {
		return search(user, query, engineTypes, limit, 0);
	}

	/**
	 * Builds a request-time index of all non-disabled MCP tools from accessible
	 * engines and projects/apps.
	 *
	 * @param user        logged-in user used for security filtering.
	 * @param engineTypes normalized engine catalog type filters.
	 * @return list of tool index entries.
	 */
	private List<ToolIndexEntry> buildIndex(User user, Set<String> engineTypes) {
		List<ToolIndexEntry> index = new ArrayList<>();
		Set<String> seenIds = new HashSet<>();

		List<String> engineTypeFilters = new ArrayList<>();
		for (String type : engineTypes) {
			if (!IEngine.CATALOG_TYPE.PROJECT.name().equals(type)) {
				engineTypeFilters.add(type);
			}
		}

		if (engineTypes.isEmpty() || !engineTypeFilters.isEmpty()) {
			List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(user,
					engineTypeFilters.isEmpty() ? null : engineTypeFilters, null, false, null, null, null, null, null);
			for (Map<String, Object> engineInfo : engines) {
				String engineId = asString(engineInfo.get("engine_id"));
				if (engineId == null || !seenIds.add(engineId)) {
					continue;
				}
				List<ToolIndexEntry> cached = toolCache.get(engineId);
				if (cached != null) {
					index.addAll(cached);
				} else {
					try {
						List<ToolIndexEntry> engineTools = new ArrayList<>();
						addTools(Utility.getEngine(engineId), engineTools);
						toolCache.put(engineId, engineTools);
						index.addAll(engineTools);
					} catch (Exception e) {
						classLogger.debug("Unable to resolve MCP engine {}", engineId, e);
					}
				}
			}
		}

		if (engineTypes.isEmpty() || engineTypes.contains(IEngine.CATALOG_TYPE.PROJECT.name())) {
			List<Map<String, Object>> projects = SecurityProjectUtils.getUserProjectList(user, null, null, false, false,
					null, null, null, null, null);
			for (Map<String, Object> projectInfo : projects) {
				String projectId = asString(projectInfo.get("project_id"));
				if (projectId == null || !seenIds.add(projectId)) {
					continue;
				}
				List<ToolIndexEntry> cached = toolCache.get(projectId);
				if (cached != null) {
					index.addAll(cached);
				} else {
					try {
						List<ToolIndexEntry> projectTools = new ArrayList<>();
						addTools(Utility.getProject(projectId), projectTools);
						toolCache.put(projectId, projectTools);
						index.addAll(projectTools);
					} catch (Exception e) {
						classLogger.debug("Unable to resolve MCP project {}", projectId, e);
					}
				}
			}
		}

		return index;
	}

	/**
	 * Adds non-disabled MCP tools from a resolved engine/project into the index.
	 *
	 * @param engine resolved engine or project.
	 * @param index  mutable list that receives tool metadata.
	 */
	private void addTools(IEngine engine, List<ToolIndexEntry> index) {
		if (engine == null) {
			return;
		}

		JSONObject toolMap;
		try {
			toolMap = MCPUtility.getAggregatedTools(engine);
		} catch (Exception e) {
			classLogger.debug("Unable to read MCP tools for engine/project {}", engine.getEngineId(), e);
			return;
		}

		JSONArray tools = toolMap == null ? null : toolMap.optJSONArray("tools");
		if (tools == null) {
			return;
		}

		String engineId = engine.getEngineId();
		String engineName = safeString(engine.getEngineName());
		String engineType = engine.getCatalogType() == null ? "" : engine.getCatalogType().name();
		List<String> tags = getTags(engine);
		for (int i = 0; i < tools.length(); i++) {
			JSONObject tool = tools.optJSONObject(i);
			if (tool == null) {
				continue;
			}

			JSONObject meta = tool.optJSONObject("_meta");
			if (meta != null && MCPUtility.MCPExecution.DISABLED.getValue()
					.equalsIgnoreCase(meta.optString(MCPUtility.SMSS_MCP_EXECUTION))) {
				continue;
			}

			String toolName = safeString(tool.optString("name", "")).trim();
			if (toolName.isEmpty()) {
				continue;
			}

			Object inputSchema = tool.opt("inputSchema");
			if (inputSchema instanceof JSONObject schema) {
				inputSchema = schema.toMap();
			}

			index.add(new ToolIndexEntry(toolName, safeString(tool.optString("description", "")), inputSchema, engineId,
					engineName, engineType, tags));
		}
	}

	/**
	 * Reads tag metadata for the engine/project that owns an MCP tool.
	 *
	 * @param engine resolved engine or project.
	 * @return normalized list of tag values.
	 */
	private List<String> getTags(IEngine engine) {
		try {
			Map<String, Object> metadata = engine.getCatalogType() == IEngine.CATALOG_TYPE.PROJECT
					? SecurityProjectUtils.getAggregateProjectMetadata(engine.getEngineId(), List.of("tag"), false)
					: SecurityEngineUtils.getAggregateEngineMetadata(engine.getEngineId(), List.of("tag"), false);
			Object rawTags = metadata == null ? null : metadata.get("tag");
			if (rawTags == null) {
				return Collections.emptyList();
			}

			Set<String> tags = new LinkedHashSet<>();
			Collection<?> values = rawTags instanceof Collection<?> collection ? collection : List.of(rawTags);
			for (Object value : values) {
				if (value == null) {
					continue;
				}
				for (String tag : value.toString().split(",")) {
					String cleanTag = tag.trim();
					if (!cleanTag.isEmpty()) {
						tags.add(cleanTag);
					}
				}
			}
			return new ArrayList<>(tags);
		} catch (Exception e) {
			classLogger.debug("Unable to read MCP toolbox tags for {}", engine.getEngineId(), e);
			return Collections.emptyList();
		}
	}

	/**
	 * Computes a simple lexical rank using only tool name and description. This
	 * mirrors the platform's existing case-insensitive search behavior while adding
	 * the minimum ranking needed by SearchTools.
	 *
	 * @param entry index entry to evaluate.
	 * @param query normalized query text.
	 * @return positive rank for a match, otherwise zero.
	 */
	private double getMatchScore(ToolIndexEntry entry, NormalizedText query, TokenStats tokenStats) {
		NormalizedText toolName = normalize(entry.toolName);
		NormalizedText description = normalize(entry.description);

		if (toolName.text.equals(query.text) || toolName.compact.equals(query.compact)) {
			return 1000;
		}
		if (containsQuery(toolName, query)) {
			return 800;
		}
		if (containsQuery(description, query)) {
			return 600;
		}

		double score = 0;
		int matchedTokens = 0;
		int nameMatchedTokens = 0;
		Set<String> queryTokens = uniqueSearchTokens(query.tokens);
		for (String queryToken : queryTokens) {
			double nameScore = getTokenMatchScore(toolName.tokens, queryToken, tokenStats, 12, 8);
			double descriptionScore = getTokenMatchScore(description.tokens, queryToken, tokenStats, 5, 3);
			double tokenScore = Math.max(nameScore, descriptionScore);
			if (tokenScore > 0) {
				matchedTokens++;
				if (nameScore > 0) {
					nameMatchedTokens++;
				}
				score += tokenScore;
			}
		}
		if (matchedTokens == 0) {
			return 0;
		}
		if (queryTokens.size() > 1 && nameMatchedTokens == 0 && matchedTokens < 2) {
			return 0;
		}
		// Apply coverage ratio to reward tools that match more of the query
		if (queryTokens.size() > 1) {
			double coverage = (double) matchedTokens / queryTokens.size();
			score *= coverage;
		}
		return score;
	}

	/**
	 * Normalizes text for case-insensitive lexical matching.
	 *
	 * @param value raw text to normalize.
	 * @return normalized text, compact text, and token list.
	 */
	private NormalizedText normalize(String value) {
		String safeValue = value == null ? "" : value.trim();
		String normalized = CAMEL_BOUNDARY.matcher(safeValue).replaceAll(" ");
		normalized = LETTER_DIGIT_BOUNDARY.matcher(normalized).replaceAll(" ");
		normalized = NON_ALPHANUMERIC.matcher(normalized).replaceAll(" ");
		normalized = WHITESPACE.matcher(normalized).replaceAll(" ").trim().toLowerCase(Locale.ROOT);

		List<String> tokens = new ArrayList<>();
		if (!normalized.isEmpty()) {
			for (String token : normalized.split(" ")) {
				if (!token.isEmpty()) {
					tokens.add(token);
				}
			}
		}
		return new NormalizedText(normalized, String.join("", tokens), tokens);
	}

	/**
	 * Checks whether a normalized field contains the full normalized query. For
	 * compact matching, requires the match to start at a token boundary to prevent
	 * false positives across unrelated tokens.
	 *
	 * @param field normalized field text.
	 * @param query normalized query text.
	 * @return true when the query appears in the field text.
	 */
	private boolean containsQuery(NormalizedText field, NormalizedText query) {
		if (query.text.isEmpty()) {
			return false;
		}
		if (field.text.contains(query.text)) {
			return true;
		}
		if (query.compact.isEmpty()) {
			return false;
		}
		int matchIndex = field.compact.indexOf(query.compact);
		if (matchIndex < 0) {
			return false;
		}
		// Verify the match starts at a token boundary in the field
		int pos = 0;
		for (String token : field.tokens) {
			if (pos == matchIndex) {
				return true;
			}
			if (pos > matchIndex) {
				break;
			}
			pos += token.length();
		}
		return false;
	}

	/**
	 * Keeps useful query tokens unique while ignoring one-character noise.
	 *
	 * @param tokens normalized query tokens.
	 * @return unique tokens eligible for overlap scoring.
	 */
	private Set<String> uniqueSearchTokens(List<String> tokens) {
		Set<String> uniqueTokens = new LinkedHashSet<>();
		for (String token : tokens) {
			if (isSearchableToken(token)) {
				uniqueTokens.add(token);
			}
		}
		return uniqueTokens;
	}

	/**
	 * Computes token overlap score for one field. Unmatched query terms simply add
	 * no score, so extra natural-language words do not prevent relevant matches.
	 *
	 * @param fieldTokens  normalized field tokens.
	 * @param queryToken   normalized query token.
	 * @param tokenStats   document-frequency weights for the current tool index.
	 * @param exactWeight  field weight for exact token matches.
	 * @param prefixWeight field weight for prefix token matches.
	 * @return weighted token score for the best field token match.
	 */
	private double getTokenMatchScore(List<String> fieldTokens, String queryToken, TokenStats tokenStats,
			double exactWeight, double prefixWeight) {
		double bestScore = 0;
		double idfWeight = tokenStats.weight(queryToken);
		for (String fieldToken : fieldTokens) {
			if (fieldToken.equals(queryToken)) {
				bestScore = Math.max(bestScore, exactWeight * idfWeight);
			} else if (isPrefixMatch(fieldToken, queryToken)) {
				bestScore = Math.max(bestScore, prefixWeight * idfWeight);
			}
		}
		return bestScore;
	}

	private boolean isPrefixMatch(String fieldToken, String queryToken) {
		if (fieldToken.length() < 4 || queryToken.length() < 4) {
			return false;
		}
		int shorter = Math.min(fieldToken.length(), queryToken.length());
		int longer = Math.max(fieldToken.length(), queryToken.length());
		// Require the shared prefix to cover at least 60% of the longer token
		// to avoid spurious matches like "send" matching "sendEmailWithAttachment"
		if ((double) shorter / longer < 0.6) {
			return false;
		}
		return fieldToken.startsWith(queryToken) || queryToken.startsWith(fieldToken);
	}

	private boolean isSearchableToken(String token) {
		return token != null && token.length() > 1;
	}

	private TokenStats buildTokenStats(List<ToolIndexEntry> index) {
		Map<String, Integer> documentFrequency = new HashMap<>();
		for (ToolIndexEntry entry : index) {
			Set<String> documentTokens = new HashSet<>();
			documentTokens.addAll(normalize(entry.toolName).tokens);
			documentTokens.addAll(normalize(entry.description).tokens);
			for (String token : documentTokens) {
				if (isSearchableToken(token)) {
					documentFrequency.merge(token, 1, Integer::sum);
				}
			}
		}
		return new TokenStats(index.size(), documentFrequency);
	}

	/**
	 * Normalizes engine type filters to IEngine catalog type names.
	 *
	 * @param engineTypes raw engine type filter values.
	 * @return valid catalog type names.
	 */
	private Set<String> normalizeEngineTypes(List<String> engineTypes) {
		if (engineTypes == null || engineTypes.isEmpty()) {
			return Collections.emptySet();
		}

		Set<String> normalized = new LinkedHashSet<>();
		for (String rawType : engineTypes) {
			if (rawType == null || rawType.trim().isEmpty()) {
				continue;
			}
			String upperType = rawType.trim().replace('-', '_').toUpperCase(Locale.ROOT);
			try {
				normalized.add(IEngine.CATALOG_TYPE.valueOf(upperType).name());
			} catch (IllegalArgumentException e) {
				classLogger.debug("Ignoring unsupported MCP tool search engine type {}", rawType);
			}
		}
		return normalized;
	}

	private String asString(Object value) {
		if (value == null) {
			return null;
		}
		String stringValue = value.toString().trim();
		return stringValue.isEmpty() ? null : stringValue;
	}

	private String safeString(String value) {
		return value == null ? "" : value;
	}

	private record NormalizedText(String text, String compact, List<String> tokens) {

	}

	private record TokenStats(int documentCount, Map<String, Integer> documentFrequency) {
		double weight(String token) {
			if (documentCount <= 0) {
				return 1;
			}
			int frequency = documentFrequency.getOrDefault(token, 0);
			return 1 + Math.log((documentCount + 1.0) / (frequency + 1.0));
		}
	}

	private record ToolIndexEntry(String toolName, String description, Object inputSchema, String engineId,
			String engineName, String engineType, List<String> tags) {

		Map<String, Object> toResultMap() {
			Map<String, Object> result = new LinkedHashMap<>();
			result.put("toolName", toolName);
			result.put("description", description == null ? "" : description);
			result.put("inputSchema", inputSchema);
			result.put("engineType", engineType);
			result.put("engineName", engineName);
			result.put("tags", tags);
			result.put("engineId", engineId);

			Map<String, Object> roomOptionMcpEntry = new LinkedHashMap<>();
			roomOptionMcpEntry.put("id", engineId);
			roomOptionMcpEntry.put("toolName", toolName);
			result.put("roomOptionMcpEntry", roomOptionMcpEntry);
			return result;
		}
	}

	private record ScoredTool(ToolIndexEntry entry, double score) {

	}
}