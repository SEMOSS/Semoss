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
package prerna.reactor.automation.run;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.reactor.automation.AutomationConstants;

/**
 * Field-specific rendering for supported direct Pixel node adapters.
 */
final class AutomationPixelExpressionRenderer {

	private static final Pattern FILTER_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
	private static final Pattern SCOPE_REFERENCE = Pattern.compile("\\$\\{([^}]+)}");

	private AutomationPixelExpressionRenderer() {
	}

	static String databaseQuery(String databaseId, String query, int limit) {
		return "SqlQuery(database=" + AutomationRuntimeUtils.GSON.toJson(databaseId)
				+ ",query=" + AutomationRuntimeUtils.GSON.toJson(query)
				+ ",limit=" + limit + ");";
	}

	static String functionExecute(Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		Object engineValue = AutomationScopeResolver.resolveExactOrLiteral(config.get("engineId"), scope,
				"Function node '" + nodeId + "' engineId");
		if (!(engineValue instanceof String engineId) || engineId.isBlank()) {
			throw new IllegalArgumentException("Function node '" + nodeId + "' requires a nonblank engineId.");
		}
		Object arguments = AutomationScopeResolver.resolveConfig(config.get("arguments"), scope,
				"Function node '" + nodeId + "' arguments");
		if (!(arguments instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Function node '" + nodeId + "' arguments must resolve to a map.");
		}
		return "ExecuteFunctionEngine(engine=" + AutomationRuntimeUtils.GSON.toJson(engineId) + ", map=["
				+ AutomationRuntimeUtils.GSON.toJson(arguments) + "]);";
	}

	static String storageList(Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		String engineId = requiredString(config.get("engineId"), scope, "Storage node '" + nodeId + "' engineId");
		Object pathValue = AutomationScopeResolver.resolveExactOrLiteral(config.getOrDefault("path", ""), scope,
				"Storage node '" + nodeId + "' path");
		String path = pathValue == null ? "" : pathValue.toString();
		return "Storage(" + AutomationRuntimeUtils.GSON.toJson(engineId)
				+ ")|ListStoragePathDetails(storagePath=" + AutomationRuntimeUtils.GSON.toJson(path) + ");";
	}

	static String databaseWrite(Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		String engineId = requiredString(config.get("engineId"), scope,
				"Database write node '" + nodeId + "' engineId");
		String query = requiredString(config.get("query"), scope, "Database write node '" + nodeId + "' query");
		return "Database(" + AutomationRuntimeUtils.GSON.toJson(engineId) + ")|Query("
				+ AutomationRuntimeUtils.GSON.toJson(query) + ")|ExecQuery(commit=true);";
	}

	static String vectorWrite(String nodeType, Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		String engineId = requiredString(config.get("engineId"), scope, "Vector node '" + nodeId + "' engineId");
		Object configuredValues = AutomationScopeResolver.resolveExactOrLiteral(config.get("value"), scope,
				"Vector node '" + nodeId + "' value");
		List<String> values = stringList(configuredValues);
		if (values.isEmpty()) {
			throw new IllegalArgumentException("Vector node '" + nodeId + "' requires at least one file.");
		}
		Object parameters = AutomationScopeResolver.resolveConfig(config.getOrDefault("paramValues", Map.of()), scope,
				"Vector node '" + nodeId + "' paramValues");
		if (!(parameters instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Vector node '" + nodeId + "' paramValues must resolve to a map.");
		}
		String arguments = "engine=" + AutomationRuntimeUtils.GSON.toJson(engineId);
		if (AutomationConstants.NODE_VECTOR_ADD.equals(nodeType)) {
			arguments += ",filePaths=" + AutomationRuntimeUtils.GSON.toJson(values);
			Object spaceValue = AutomationScopeResolver.resolveExactOrLiteral(config.getOrDefault("space", ""), scope,
					"Vector node '" + nodeId + "' space");
			if (spaceValue != null && !spaceValue.toString().isBlank()) {
				arguments += ",space=[" + AutomationRuntimeUtils.GSON.toJson(spaceValue.toString()) + "]";
			}
			arguments += ",paramValues=[" + AutomationRuntimeUtils.GSON.toJson(parameters) + "]";
			return "CreateEmbeddingsFromDocuments(" + arguments + ");";
		}
		arguments += ",fileNames=" + AutomationRuntimeUtils.GSON.toJson(values);
		arguments += ",paramValues=[" + AutomationRuntimeUtils.GSON.toJson(parameters) + "]";
		return "RemoveDocumentFromVectorDatabase(" + arguments + ");";
	}

	static String vectorSearch(Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		String engineId = requiredString(config.get("engineId"), scope, "Vector node '" + nodeId + "' engineId");
		String query = requiredString(config.get("value"), scope, "Vector node '" + nodeId + "' value").trim();
		Object limitValue = AutomationScopeResolver.resolveExactOrLiteral(
				config.getOrDefault("limit", AutomationConstants.DEFAULT_VECTOR_SEARCH_LIMIT), scope,
				"Vector node '" + nodeId + "' limit");
		if (!(limitValue instanceof Number number) || number.intValue() <= 0) {
			throw new IllegalArgumentException("Vector node '" + nodeId + "' limit must be a positive number.");
		}
		Object filtersValue = AutomationScopeResolver.resolveConfig(config.getOrDefault("filters", Map.of()), scope,
				"Vector node '" + nodeId + "' filters");
		if (!(filtersValue instanceof Map<?, ?> filters)) {
			throw new IllegalArgumentException("Vector node '" + nodeId + "' filters must resolve to a map.");
		}
		Object parameters = AutomationScopeResolver.resolveConfig(config.getOrDefault("paramValues", Map.of()), scope,
				"Vector node '" + nodeId + "' paramValues");
		if (!(parameters instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Vector node '" + nodeId + "' paramValues must resolve to a map.");
		}
		StringBuilder expression = new StringBuilder("VectorDatabaseQuery(engine=").append(json(engineId))
				.append(",command=[").append(json(query)).append("],limit=[").append(number.intValue()).append("]");
		if (!filters.isEmpty()) {
			List<String> conditions = new ArrayList<>();
			for (Map.Entry<?, ?> entry : filters.entrySet()) {
				if (!(entry.getKey() instanceof String key) || !FILTER_FIELD.matcher(key).matches()) {
					throw new IllegalArgumentException("Vector node '" + nodeId + "' contains an invalid filter field.");
				}
				String field = Character.toUpperCase(key.charAt(0)) + key.substring(1);
				List<String> values = stringList(entry.getValue());
				if (values.isEmpty()) {
					throw new IllegalArgumentException(
							"Vector node '" + nodeId + "' filter '" + key + "' has no values.");
				}
				conditions.add(field + " == " + json(values));
			}
			expression.append(",filters=[Filter(").append(String.join(",", conditions)).append(")]");
		}
		return expression.append(",paramValues=[").append(json(parameters)).append("]);").toString();
	}

	static String model(String nodeType, Map<String, Object> config, Map<String, Object> scope,
			String nodeId, String roomId) {
		String engineId = requiredString(config.get("engineId"), scope, "Model node '" + nodeId + "' engineId");
		Object parameters = AutomationScopeResolver.resolveConfig(config.getOrDefault("paramValues", Map.of()), scope,
				"Model node '" + nodeId + "' paramValues");
		if (!(parameters instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Model node '" + nodeId + "' paramValues must resolve to a map.");
		}
		if (AutomationConstants.NODE_MODEL_EMBEDDINGS.equals(nodeType)) {
			Object values = AutomationScopeResolver.resolveExactOrLiteral(config.get("text"), scope,
					"Model node '" + nodeId + "' text");
			List<String> texts = stringList(values);
			if (texts.isEmpty()) {
				throw new IllegalArgumentException("Model node '" + nodeId + "' text must not be empty.");
			}
			return "Embeddings(engine=" + json(engineId) + ",values=" + json(texts) + ",paramValues=["
					+ json(parameters) + "],encoded=false);";
		}
		if (AutomationConstants.NODE_MODEL_NER.equals(nodeType)) {
			String text = requiredString(config.get("text"), scope, "Model node '" + nodeId + "' text");
			Object entities = AutomationScopeResolver.resolveConfig(config.get("entities"), scope,
					"Model node '" + nodeId + "' entities");
			Object masked = AutomationScopeResolver.resolveConfig(config.getOrDefault("maskEntities", List.of()), scope,
					"Model node '" + nodeId + "' maskEntities");
			return "NER(engine=" + json(engineId) + ",prompt=" + json(text) + ",entities=" + json(entities)
					+ ",maskEntities=" + json(masked) + ");";
		}
		String prompt = modelPrompt(config.get("prompt"), scope, nodeId);
		StringBuilder expression = new StringBuilder("LLM(engine=").append(json(engineId))
				.append(",command=").append(json(prompt));
		Object systemPrompt = AutomationScopeResolver.resolveExactOrLiteral(config.get("systemPrompt"), scope,
				"Model node '" + nodeId + "' systemPrompt");
		if (systemPrompt != null && !systemPrompt.toString().isBlank()) {
			expression.append(",context=[").append(json(systemPrompt.toString())).append("]");
		}
		expression.append(",useHistory=true,paramValues=[").append(json(parameters)).append("]");
		if (roomId != null && !roomId.isBlank()) {
			expression.append(",roomId=").append(json(roomId));
		}
		if (AutomationConstants.NODE_MODEL_VISION.equals(nodeType)) {
			Object media = AutomationScopeResolver.resolveConfig(config.getOrDefault("image", ""), scope,
					"Model node '" + nodeId + "' image");
			Object urls = AutomationScopeResolver.resolveConfig(config.getOrDefault("urls", List.of()), scope,
					"Model node '" + nodeId + "' urls");
			List<String> mediaValues = stringList(media);
			List<String> urlValues = stringList(urls);
			if (mediaValues.isEmpty() && urlValues.isEmpty()) {
				throw new IllegalArgumentException("Model node '" + nodeId + "' requires media or a URL.");
			}
			if (!mediaValues.isEmpty()) {
				expression.append(",media=").append(json(mediaValues));
			}
			if (!urlValues.isEmpty()) {
				expression.append(",url=").append(json(urlValues));
			}
		}
		return expression.append(");").toString();
	}

	private static String modelPrompt(Object configuredValue, Map<String, Object> scope, String nodeId) {
		return interpolatedText(configuredValue, scope, "Model node '" + nodeId + "' prompt");
	}

	private static String interpolatedText(Object configuredValue, Map<String, Object> scope, String valueName) {
		if (configuredValue instanceof String configuredText
				&& AutomationScopeResolver.exactReferencePath(configuredText) == null) {
			Matcher matcher = SCOPE_REFERENCE.matcher(configuredText);
			StringBuffer resolved = new StringBuffer();
			while (matcher.find()) {
				Object referenced = AutomationScopeResolver.resolveRequired(matcher.group(1), scope,
						valueName);
				matcher.appendReplacement(resolved, Matcher.quoteReplacement(promptText(referenced)));
			}
			matcher.appendTail(resolved);
			if (!resolved.isEmpty()) {
				return resolved.toString();
			}
		}
		Object value = AutomationScopeResolver.resolveExactOrLiteral(configuredValue, scope, valueName);
		String prompt = promptText(value);
		if (!prompt.isBlank()) {
			return prompt;
		}
		throw new IllegalArgumentException(valueName + " must resolve to nonblank text or data.");
	}

	private static String promptText(Object value) {
		if (value instanceof AutomationFrameOutput.RegisteredFrame frame) {
			return frame.promptJson();
		}
		if (value instanceof Map<?, ?> || value instanceof List<?>) {
			return AutomationRuntimeUtils.toRuntimeJson(value);
		}
		return value == null ? "" : value.toString();
	}

	static Object modelResult(String nodeType, Object result, String nodeId) {
		if (!(result instanceof Map<?, ?> response)) {
			throw new IllegalStateException("Model node '" + nodeId + "' did not return an object.");
		}
		if (!response.containsKey("response")) {
			throw new IllegalStateException("Model node '" + nodeId + "' response is missing content.");
		}
		Object value = response.get("response");
		if (AutomationConstants.NODE_MODEL_NER.equals(nodeType) && value instanceof Map<?, ?> map
				&& "error".equalsIgnoreCase(String.valueOf(map.get("status")))) {
			throw new IllegalStateException(String.valueOf(map.get("message")));
		}
		if (AutomationConstants.NODE_MODEL_CHAT.equals(nodeType)
				|| AutomationConstants.NODE_MODEL_VISION.equals(nodeType)) {
			Map<String, Object> metadata = new LinkedHashMap<>();
			metadata.put("roomId", response.get("roomId"));
			metadata.put("messageId", response.get("messageId"));
			Map<String, Object> adapted = new LinkedHashMap<>();
			adapted.put(AutomationConstants.INTERNAL_RESULT_VALUE, value);
			adapted.put(AutomationConstants.INTERNAL_RESULT_METADATA, metadata);
			return adapted;
		}
		return value;
	}

	static String agent(Map<String, Object> config, Map<String, Object> scope, String nodeId, String roomId) {
		if (roomId == null || roomId.isBlank()) {
			throw new IllegalArgumentException("Agent node '" + nodeId + "' requires a durable room.");
		}
		String command = interpolatedText(config.get(AutomationConstants.CONFIG_COMMAND), scope,
				"Agent node '" + nodeId + "' command");
		StringBuilder expression = new StringBuilder("RunAgent(roomId=").append(json(roomId))
				.append(",command=").append(json(command));
		appendResolved(expression, "engine", config.get(AutomationConstants.CONFIG_ENGINE_ID), scope,
				"Agent node '" + nodeId + "' engineId");
		appendResolved(expression, "harnessType", config.get(AutomationConstants.CONFIG_HARNESS_TYPE), scope,
				"Agent node '" + nodeId + "' harnessType");
		appendResolved(expression, "workspaceId", config.get(AutomationConstants.CONFIG_WORKSPACE_ID), scope,
				"Agent node '" + nodeId + "' workspaceId");
		appendResolved(expression, "maxTurns", config.get(AutomationConstants.CONFIG_MAX_TURNS), scope,
				"Agent node '" + nodeId + "' maxTurns");
		appendResolved(expression, "maxReflections", config.get(AutomationConstants.CONFIG_MAX_REFLECTIONS), scope,
				"Agent node '" + nodeId + "' maxReflections");
		expression.append(",wait=false");
		appendResolved(expression, "waitTimeoutMs", config.get(AutomationConstants.CONFIG_WAIT_TIMEOUT_MS), scope,
				"Agent node '" + nodeId + "' waitTimeoutMs");
		Object paramMap = config.containsKey("paramMap") ? config.get("paramMap")
				: config.get(AutomationConstants.CONFIG_PARAM_VALUES);
		appendResolvedConfig(expression, "paramValues", paramMap, scope,
				"Agent node '" + nodeId + "' paramValues");
		appendResolvedConfig(expression, "agentParams", config.get("agentParams"), scope,
				"Agent node '" + nodeId + "' agentParams");
		return expression.append(");").toString();
	}

	static Object agentResult(Object result, String nodeId) {
		if (result instanceof List<?> list && list.size() == 1) {
			result = list.get(0);
		}
		if (!(result instanceof Map<?, ?> response)) {
			throw new IllegalStateException("Agent node '" + nodeId + "' did not return a durable run object.");
		}
		Map<String, Object> metadata = new LinkedHashMap<>();
		for (String key : List.of("runId", "roomId", "workspaceId", "status", "waitTimedOut", "errorMessage")) {
			metadata.put(key, response.get(key));
		}
		Map<String, Object> adapted = new LinkedHashMap<>();
		adapted.put(AutomationConstants.INTERNAL_RESULT_VALUE, response.get("finalText"));
		adapted.put(AutomationConstants.INTERNAL_RESULT_METADATA, metadata);
		return adapted;
	}

	static List<String> storagePaths(Object result, Object configuredExtensions, Map<String, Object> scope,
			String nodeId) {
		if (!(result instanceof List<?> details)) {
			throw new IllegalStateException("Storage node '" + nodeId + "' did not return a detail list.");
		}
		Object resolvedExtensions = AutomationScopeResolver.resolveConfig(configuredExtensions, scope,
				"Storage node '" + nodeId + "' extensions");
		List<String> suffixes = new ArrayList<>();
		List<?> extensions = resolvedExtensions instanceof List<?> list ? list
				: resolvedExtensions == null ? List.of() : List.of(resolvedExtensions);
		for (Object extension : extensions) {
			String normalized = String.valueOf(extension).trim().toLowerCase(Locale.ROOT);
			if (!normalized.isEmpty()) {
				suffixes.add("." + (normalized.startsWith(".") ? normalized.substring(1) : normalized));
			}
		}
		List<String> paths = new ArrayList<>();
		for (Object detail : details) {
			if (!(detail instanceof Map<?, ?> map) || !(map.get("Path") instanceof String path)) {
				continue;
			}
			String normalizedPath = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
			if (suffixes.isEmpty() || suffixes.stream()
					.anyMatch(suffix -> normalizedPath.toLowerCase(Locale.ROOT).endsWith(suffix))) {
				paths.add(path);
			}
		}
		return paths;
	}

	static String storageOperation(String nodeType, Map<String, Object> config, Map<String, Object> scope,
			String nodeId) {
		String engineId = requiredString(config.get("engineId"), scope, "Storage node '" + nodeId + "' engineId");
		String path = requiredString(config.get("path"), scope, "Storage node '" + nodeId + "' path");
		if (AutomationConstants.NODE_STORAGE_READ.equals(nodeType)) {
			Object configured = AutomationScopeResolver.resolveExactOrLiteral(
					config.getOrDefault("convertToPdf", false), scope,
					"Storage node '" + nodeId + "' convertToPdf");
			boolean convert = configured instanceof Boolean bool ? bool : Boolean.parseBoolean(String.valueOf(configured));
			return "GetStorageFileAsBase64(storage=" + json(engineId) + ",storagePath=" + json(path)
					+ ",convertToPdf=" + convert + ");";
		}
		if (AutomationConstants.NODE_STORAGE_DELETE.equals(nodeType)) {
			Object configured = AutomationScopeResolver.resolveExactOrLiteral(
					config.getOrDefault("leaveFolderStructure", false), scope,
					"Storage node '" + nodeId + "' leaveFolderStructure");
			boolean leaveFolders = configured instanceof Boolean bool
					? bool : Boolean.parseBoolean(String.valueOf(configured));
			return "Storage(" + json(engineId) + ")|DeleteFromStorage(storagePath=" + json(path)
					+ ",leaveFolderStructure=" + leaveFolders + ");";
		}
		String filePath = requiredString(config.get("destination"), scope,
				"Storage node '" + nodeId + "' destination");
		Object metadata = AutomationScopeResolver.resolveConfig(config.getOrDefault("metadata", Map.of()), scope,
				"Storage node '" + nodeId + "' metadata");
		if (!(metadata instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Storage node '" + nodeId + "' metadata must resolve to a map.");
		}
		return "Storage(" + json(engineId) + ")|PushToStorage(storagePath=" + json(path) + ",filePath="
				+ json(filePath) + ",metadata=[" + json(metadata) + "]);";
	}

	static StorageDownload storageDownload(Map<String, Object> config, Map<String, Object> scope, String nodeId) {
		String engineId = requiredString(config.get("engineId"), scope, "Storage node '" + nodeId + "' engineId");
		String path = requiredString(config.get("path"), scope, "Storage node '" + nodeId + "' path");
		Object destinationValue = AutomationScopeResolver.resolveExactOrLiteral(config.get("destination"), scope,
				"Storage node '" + nodeId + "' destination");
		String destination = destinationValue == null || destinationValue.toString().isBlank()
				? "/" : destinationValue.toString().trim();
		Object versionValue = AutomationScopeResolver.resolveExactOrLiteral(config.getOrDefault("version", ""), scope,
				"Storage node '" + nodeId + "' version");
		String version = versionValue == null ? "" : versionValue.toString().trim();
		String transfer = version.isEmpty()
				? "Storage(" + json(engineId) + ")|PullFromStorage(storagePath=" + json(path)
						+ ",filePath=" + json(destination) + ");"
				: "PullFromStorage(storage=" + json(engineId) + ",storagePath=" + json(path)
						+ ",filePath=" + json(destination) + ",version=" + json(version) + ");";
		String search = "SearchInsightAssets(filePath=" + json(destination)
				+ ",search=\".*\",options=\"regex\");";
		return new StorageDownload(transfer, search, path, destination);
	}

	static Map<String, Object> storageDownloadResult(Object transferResult, Object assetResult,
			StorageDownload request, String nodeId) {
		if (!(assetResult instanceof List<?> assets)) {
			throw new IllegalStateException("Storage node '" + nodeId + "' asset search did not return a list.");
		}
		List<String> files = new ArrayList<>();
		for (Object asset : assets) {
			if (asset instanceof Map<?, ?> map && !"directory".equals(map.get("type"))
					&& map.get("path") instanceof String path) {
				files.add(path);
			}
		}
		if (files.isEmpty()) {
			throw new IllegalStateException("No file was downloaded from storage path: " + request.storagePath());
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("success", transferResult);
		result.put("storagePath", request.storagePath());
		result.put("destination", request.destination());
		result.put("space", "INSIGHT");
		result.put("files", files);
		result.put("filePath", files.size() == 1 ? files.get(0) : null);
		return result;
	}

	private static String requiredString(Object configuredValue, Map<String, Object> scope, String valueName) {
		Object value = AutomationScopeResolver.resolveExactOrLiteral(configuredValue, scope, valueName);
		if (!(value instanceof String string) || string.isBlank()) {
			throw new IllegalArgumentException(valueName + " must resolve to a nonblank string.");
		}
		return string;
	}

	private static List<String> stringList(Object value) {
		if (value instanceof AutomationFrameOutput.RegisteredFrame) {
			throw new IllegalArgumentException(
					"Table values require an explicit table-aware field or data extraction.");
		}
		List<?> rawValues = value instanceof List<?> list ? list
				: value == null ? List.of() : List.of(value.toString().split(","));
		List<String> values = new ArrayList<>();
		for (Object rawValue : rawValues) {
			if (rawValue != null && !rawValue.toString().trim().isEmpty()) {
				values.add(rawValue.toString().trim());
			}
		}
		return values;
	}

	private static String json(Object value) {
		rejectImplicitTable(value);
		return AutomationRuntimeUtils.GSON.toJson(value);
	}

	private static void rejectImplicitTable(Object value) {
		if (value instanceof AutomationFrameOutput.RegisteredFrame) {
			throw new IllegalArgumentException(
					"Table values require an explicit table-aware field or data extraction.");
		}
		if (value instanceof Map<?, ?> map) {
			map.values().forEach(AutomationPixelExpressionRenderer::rejectImplicitTable);
		} else if (value instanceof List<?> list) {
			list.forEach(AutomationPixelExpressionRenderer::rejectImplicitTable);
		}
	}

	private static void appendResolved(StringBuilder expression, String name, Object configuredValue,
			Map<String, Object> scope, String valueName) {
		if (configuredValue == null) {
			return;
		}
		Object value = AutomationScopeResolver.resolveExactOrLiteral(configuredValue, scope, valueName);
		if (value != null) {
			expression.append(",").append(name).append("=").append(json(value));
		}
	}

	private static void appendResolvedConfig(StringBuilder expression, String name, Object configuredValue,
			Map<String, Object> scope, String valueName) {
		if (configuredValue == null) {
			return;
		}
		Object value = AutomationScopeResolver.resolveConfig(configuredValue, scope, valueName);
		if (!(value instanceof Map<?, ?>)) {
			throw new IllegalArgumentException(valueName + " must resolve to a map.");
		}
		expression.append(",").append(name).append("=").append(json(value));
	}

	record StorageDownload(String transferExpression, String searchExpression, String storagePath,
			String destination) {
	}
}
