package prerna.playground.reactors;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.logging.AuditLogsDbUtils;
import prerna.logging.LogActivityRecord;
import prerna.reactor.AbstractReactor;
import prerna.reactor.model.EmbeddingsReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;


public class ExtractDailyToolFailureSlicesReactor extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(ExtractDailyToolFailureSlicesReactor.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	// Configuration for learned directive management
	private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
	private static final String EMBEDDING_ENGINE_ID = "e4449559-bcff-4941-ae72-0e3f18e06660";
	private static final String DIRECTIVE_MODEL_ID = "e4e09373-3f91-4db9-a838-f4d87ecd0c66";
	private static final String TOOL_NAME = "tool_name";
	private static final String USER_DIRECTIVE = "user_directive";

	// Embedding-based deduplication parameters
	private static final int MAX_DIRECTIVES_PER_TOOL = 10;
	private static final int MAX_CONSOLIDATED_DIRECTIVES = 5;
	private static final double COSINE_MATCH_THRESHOLD = 0.85;

	// Regex patterns for parsing /remember directives in user messages
	private static final Pattern REMEMBER_MARKER = Pattern.compile("(?i)/remember\\s+");
	private static final Pattern TOOL_DIRECTIVE_PAIR = Pattern.compile("\"([^\"]+)\"\\s*-\\s*\"([^\"]+)\"");

	public ExtractDailyToolFailureSlicesReactor() {

	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		ZonedDateTime endDateTime = ZonedDateTime.now(UTC_ZONE);
		ZonedDateTime startDateTime = endDateTime.minusDays(1);
		SemossDate startDate = new SemossDate(startDateTime);

		Map<String, List<Map<String, Object>>> slicesByTool = new LinkedHashMap<>();
		List<String> userIds = AuditLogsDbUtils.fetchUserIds(startDate);
		for (String userId : userIds) {
			List<String> roomIds = AuditLogsDbUtils.fetchRoomIds(startDate, userId);
			for (String roomId : roomIds) {
				Map<String, Object> latestLog = new HashMap<>();
				try {
					latestLog = fetchLatestModelRoomAuditEntry(userId, roomId);
				} catch (Exception e) {
					LOGGER.warn("Failed to fetch latest Model audit log", e.getMessage());
				}
				if (latestLog == null) {
					continue;
				}
				try {
					extractRememberDirectives(latestLog, slicesByTool);
				} catch (Exception e) {
					LOGGER.warn("Failed to extract /remember directives for room '{}', skipping", roomId, e);
				}
			}
		}
		int directiveCount = 0;
		int directiveWriteCount = 0;
		if (!slicesByTool.isEmpty()) {
			IModelEngine directiveModelEngine = loadDirectiveModelEngine();
			try {
				List<Map<String, Object>> directives = analyzeFailurePatterns(slicesByTool, directiveModelEngine);
				directiveCount = directives.size();
				directiveWriteCount = writeDirectives(directives, directiveModelEngine);
			} catch (Exception e) {
				LOGGER.error("Failed to generate or write tool failure directives", e);
			}
		}

		Map<String, Object> response = new LinkedHashMap<>();
		response.put("directive_count", directiveCount);
		response.put("directive_write_count", directiveWriteCount);
		return new NounMetadata(response, PixelDataType.MAP);
	}

	private Map<String, Object> fetchLatestModelRoomAuditEntry(String userId, String roomId) throws Exception {
		if (userId == null) {
			throw new IllegalArgumentException("User ID is required to fetch logs");
		}
		if (roomId == null) {
			throw new IllegalArgumentException("Room ID is required to fetch logs");
		}
		List<LogActivityRecord> activityList = AuditLogsDbUtils.getAuditLogsTimeLineData(userId, null, null, null, null, roomId, null, 100, 0);
		if (activityList != null) {
			for (LogActivityRecord activity : activityList) {
				if ("MODEL".equalsIgnoreCase(activity.engineType())) {
					Map<String, Object> rawLog = new LinkedHashMap<>();
					rawLog.put("REQUEST", activity.request());
					return rawLog;
				}
			}
		}
		throw new IllegalArgumentException("No MODEL audit logs found for roomId: " + roomId);
	}

	private IModelEngine loadDirectiveModelEngine() {
		try {
			return Utility.getModel(DIRECTIVE_MODEL_ID);
		} catch (RuntimeException e) {
			throw new IllegalStateException("Directive model could not be loaded: " + DIRECTIVE_MODEL_ID, e);
		}
	}

	/**
	 * Reads the REQUEST JSON from a single audit log row and adds any
	 * {@code /remember} directives found in INPUT_TEXT messages to slicesByTool.
	 */
	private void extractRememberDirectives(Map<String, Object> latestLog,
			Map<String, List<Map<String, Object>>> slicesByTool) {
		String requestJson = stringValue(latestLog.get("REQUEST"));
		if (requestJson == null) {
			return;
		}
		JsonObject request = parseJsonObject(requestJson);
		if (request == null) {
			return;
		}
		for (String inputText : readInputTexts(request)) {
			Map<String, String> pairs = parseRememberDirectives(inputText);
			for (Map.Entry<String, String> entry : pairs.entrySet()) {
				Map<String, Object> slice = new LinkedHashMap<>();
				slice.put(TOOL_NAME, entry.getKey());
				slice.put(USER_DIRECTIVE, entry.getValue());
				slicesByTool.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(slice);
			}
		}
	}

	/**
	 * Extracts the text of every INPUT_TEXT message from the REQUEST JSON,
	 * covering both the conversation history (arg3.message_json) and the
	 * current message (arg2).
	 */
	private List<String> readInputTexts(JsonObject request) {
		List<String> texts = new ArrayList<>();
		JsonElement arg3 = request.get("arg3");
		if (arg3 != null && arg3.isJsonObject()) {
			JsonElement historyElement = arg3.getAsJsonObject().get("message_json");
			if (historyElement != null) {
				JsonElement historyArray = toJsonArray(historyElement);
				if (historyArray != null) {
					for (JsonElement el : historyArray.getAsJsonArray()) {
						String text = extractInputTextFromMessage(el);
						if (text != null) {
							texts.add(text);
						}
					}
				}
			}
		}
		JsonElement arg2 = request.get("arg2");
		if (arg2 != null) {
			String text = extractInputTextFromMessage(toParsedJson(arg2));
			if (text != null) {
				texts.add(text);
			}
		}
		return texts;
	}

	/**
	 * Returns the user-visible text of an INPUT_TEXT message element,
	 * or {@code null} when the element is not INPUT_TEXT or has no text parts.
	 */
	private String extractInputTextFromMessage(JsonElement element) {
		if (element == null || !element.isJsonObject()) {
			return null;
		}
		JsonObject message = element.getAsJsonObject();
		if (!"INPUT_TEXT".equalsIgnoreCase(stringValue(message.get("type")))) {
			return null;
		}
		JsonElement partsElement = message.get("parts");
		if (partsElement == null || !partsElement.isJsonArray()) {
			return null;
		}
		for (JsonElement partElement : partsElement.getAsJsonArray()) {
			if (partElement == null || !partElement.isJsonObject()) {
				continue;
			}
			JsonObject part = partElement.getAsJsonObject();
			String text = stringValue(part.get("uiText"));
			if (text == null) {
				text = stringValue(part.get("text"));
			}
			if (text != null) {
				return text;
			}
		}
		return null;
	}

	/**
	 * Parses {@code /remember "toolName" - "directive"} pairs from a message text.
	 * Returns an empty map when no {@code /remember} token is present.
	 */
	private static Map<String, String> parseRememberDirectives(String text) {
		if (text == null || !text.toLowerCase().contains("/remember")) {
			return Collections.emptyMap();
		}
		Matcher markerMatcher = REMEMBER_MARKER.matcher(text);
		if (!markerMatcher.find()) {
			return Collections.emptyMap();
		}
		String remainder = text.substring(markerMatcher.end());
		Map<String, String> result = new LinkedHashMap<>();
		Matcher pairMatcher = TOOL_DIRECTIVE_PAIR.matcher(remainder);
		while (pairMatcher.find()) {
			String toolName = pairMatcher.group(1).trim();
			String directive = pairMatcher.group(2).trim();
			if (!toolName.isEmpty() && !directive.isEmpty()) {
				result.put(toolName, directive);
			}
		}
		return result;
	}

	/** Parses a JSON string to a JsonObject; returns null on any parse failure. */
	private JsonObject parseJsonObject(String json) {
		try {
			JsonElement el = JsonParser.parseString(json);
			return el.isJsonObject() ? el.getAsJsonObject() : null;
		} catch (Exception ignored) {
			return null;
		}
	}

	/**
	 * Returns the element as a JsonArray, parsing from its string representation
	 * when needed. Returns null when it cannot be resolved to an array.
	 */
	private JsonElement toJsonArray(JsonElement element) {
		if (element.isJsonArray()) {
			return element;
		}
		if (element.isJsonPrimitive()) {
			try {
				JsonElement parsed = JsonParser.parseString(element.getAsString());
				return parsed.isJsonArray() ? parsed : null;
			} catch (Exception ignored) {
			}
		}
		return null;
	}

	/**
	 * Returns the element parsed from its string value when it is a JSON-encoded
	 * string; otherwise returns the element as-is.
	 */
	private JsonElement toParsedJson(JsonElement element) {
		if (element.isJsonObject() || element.isJsonArray()) {
			return element;
		}
		if (element.isJsonPrimitive()) {
			try {
				return JsonParser.parseString(element.getAsString());
			} catch (Exception ignored) {
			}
		}
		return element;
	}

	private List<Map<String, Object>> analyzeFailurePatterns(Map<String, List<Map<String, Object>>> slicesByTool,
			IModelEngine modelEngine) {
		String systemPrompt = "You are an expert at synthesizing user feedback into precise, actionable tool-use directives.\n"
				+ "INPUT: JSON grouped by tool_name. Each entry contains a tool_name and a user_directive — "
				+ "a user-authored instruction specifying how a tool should be invoked.\n"
				+ "TASK: For each tool, synthesize the provided user directives into one or more canonical, "
				+ "imperative directives that capture every distinct user intent. "
				+ "Only produce a directive when the input shows clear and consistent intent. "
				+ "Produce multiple directives for a tool only when the intents are clearly distinct and non-overlapping.\n"
				+ "RULES:\n"
				+ "- Each directive must be a specific, actionable instruction a model can follow when calling that tool.\n"
				+ "- Do not add generic advice. Do not invent tools, fields, or information not present in the input.\n"
				+ "- Do not include explanations, metadata, or confidence scores in the output.\n"
				+ "- tool_name must exactly match a key from the input.\n"
				+ "RESPONSE FORMAT: Return ONLY a JSON array with no prose, markdown, or code fences. "
				+ "Each element must have exactly two fields: tool_name (string) and directive (string). "
				+ "Return [] if no clear directive can be inferred.\n"
				+ "INPUT DATA:\n" + GSON.toJson(slicesByTool);

		Room tempRoom = new Room();
		tempRoom.setId(UUID.randomUUID().toString());
		tempRoom.setInsight(this.insight);

		InputMessage msg = InputMessage.builder(tempRoom).withSystemPrompt(systemPrompt).build();
		ResponseMessage modelResponse;
		try {
			modelResponse = tempRoom.ask(msg, modelEngine, null, false);
		} catch (RuntimeException e) {
			throw new IllegalStateException("Failed to generate learned tool directives", e);
		}
		String responseText = modelResponse == null ? null : stringValue(modelResponse.getContent());
		return parseDirectiveResponse(responseText, slicesByTool.keySet());
	}

	private List<Map<String, Object>> parseDirectiveResponse(String responseText, Set<String> validToolNames) {
		List<Map<String, Object>> directives = new ArrayList<>();
		Set<String> seenDirectives = new HashSet<>();
		JsonElement parsed = parseJsonSlice(responseText, "Directive generator");
		if (!parsed.isJsonArray()) {
			throw new IllegalStateException("Directive generator did not return a JSON array");
		}
		for (JsonElement element : parsed.getAsJsonArray()) {
			if (element == null || !element.isJsonObject()) {
				continue;
			}
			JsonObject item = element.getAsJsonObject();
			String toolName = stringValue(item.get(TOOL_NAME));
			String directive = stringValue(item.get("directive"));
			if (toolName == null || directive == null || !validToolNames.contains(toolName)) {
				continue;
			}
			String dedupeKey = toolName + "|" + directive.toLowerCase();
			if (!seenDirectives.add(dedupeKey)) {
				continue;
			}
			Map<String, Object> out = new LinkedHashMap<>();
			out.put(TOOL_NAME, toolName);
			out.put("directive", directive);
			directives.add(out);
		}
		return directives;
	}

	private int writeDirectives(List<Map<String, Object>> directives, IModelEngine modelEngine) {
		final String DIRECTIVE = "directive";
		int writeCount = 0;

		Map<String, List<Map<String, Object>>> byTool = new LinkedHashMap<>();
		for (Map<String, Object> row : directives) {
			String toolName = stringValue(row.get(TOOL_NAME));
			if (toolName != null) {
				byTool.computeIfAbsent(toolName, k -> new ArrayList<>()).add(row);
			}
		}

		for (Map.Entry<String, List<Map<String, Object>>> entry : byTool.entrySet()) {
			String toolName = entry.getKey();
			List<Map<String, Object>> existing = ModelInferenceLogsUtils.getDirectivesForTool(toolName);
			for (Map<String, Object> directiveRow : entry.getValue()) {
				String directive = stringValue(directiveRow.get(DIRECTIVE));
				if (directive == null) {
					continue;
				}
				try {
					List<Double> newEmbedding = generateEmbedding(directive);
					String embeddingJson = embeddingToJson(newEmbedding);
					String matchedId = cosineMatchDirective(newEmbedding, existing);
					boolean written = matchedId != null
							? ModelInferenceLogsUtils.incrementToolFailureDirective(matchedId, directive, embeddingJson)
							: ModelInferenceLogsUtils.insertToolFailureDirective(toolName, directive, embeddingJson);
					if (written) {
						writeCount++;
					}
				} catch (Exception e) {
					LOGGER.warn("Failed to write directive for tool '{}', skipping", toolName, e);
				}
			}
			try {
				List<Map<String, Object>> current = ModelInferenceLogsUtils.getDirectivesForTool(toolName);
				if (current.size() > MAX_DIRECTIVES_PER_TOOL) {
					consolidateDirectivesForTool(toolName, current, modelEngine);
				}
			} catch (Exception e) {
				LOGGER.warn("Failed to consolidate directives for tool '{}', skipping", toolName, e);
			}
		}
		return writeCount;
	}

	/**
	 * Generates the embedding vector for the given text using the configured embedding engine.
	 * Returns the first embedding vector from the response.
	 */
	private List<Double> generateEmbedding(String text) {
		NounStore ns = new NounStore(ReactorKeysEnum.ALL.getKey());
		ns.makeGenRowStruct(ReactorKeysEnum.ENGINE.getKey()).addLiteral(EMBEDDING_ENGINE_ID);
		ns.makeGenRowStruct(ReactorKeysEnum.VALUES.getKey()).addLiteral(text);
		EmbeddingsReactor embedding = new EmbeddingsReactor();
		embedding.setInsight(this.insight);
		embedding.setNounStore(ns);
		embedding.In();
		NounMetadata llmResponse = embedding.execute();
		if (llmResponse == null || llmResponse.getValue() == null) {
			throw new SemossPixelException("Embedding engine returned null response for directive text");
		}
		AskModelEngineResponse<?> response = AskModelEngineResponse.fromMap(llmResponse.getValue());
		Object result = response.getResponse();
		return (List<Double>) result;
	}

	/** Serializes an embedding vector to its JSON array string representation. */
	private String embeddingToJson(List<Double> embedding) {
		return GSON.toJson(embedding);
	}

	/** Deserializes an embedding JSON string back to a {@code List<Double>}. Returns empty list on failure. */
	private List<Double> embeddingFromJson(String json) {
		if (json == null || json.isBlank() || json.equals("[]")) {
			return Collections.emptyList();
		}
		try {
			return GSON.fromJson(json, new TypeToken<List<Double>>() {}.getType());
		} catch (Exception e) {
			return Collections.emptyList();
		}
	}

	/**
	 * Finds the {@code DIRECTIVE_ID} of the existing directive whose embedding is closest
	 * to {@code newEmbedding} and meets the {@link #COSINE_MATCH_THRESHOLD}.
	 * Returns {@code null} when no match is found (i.e. the directive is new).
	 */
	private String cosineMatchDirective(List<Double> newEmbedding, List<Map<String, Object>> existing) {
		String bestId = null;
		double bestScore = COSINE_MATCH_THRESHOLD;
		for (Map<String, Object> d : existing) {
			String embeddingJson = stringValue(d.get("directive_embedding"));
			if (embeddingJson == null) {
				continue;
			}
			List<Double> existingEmbedding = embeddingFromJson(embeddingJson);
			if (existingEmbedding.isEmpty()) {
				continue;
			}
			double score = cosineSimilarity(newEmbedding, existingEmbedding);
			if (score > bestScore) {
				bestScore = score;
				bestId = stringValue(d.get("directive_id"));
			}
		}
		return bestId;
	}

	/**
	 * Computes the cosine similarity between two equally-sized embedding vectors.
	 * Returns 0.0 for null, empty, or mismatched vectors.
	 */
	private static double cosineSimilarity(List<Double> a, List<Double> b) {
		if (a == null || b == null || a.isEmpty() || a.size() != b.size()) {
			return 0.0;
		}
		double dotProduct = 0.0;
		double normA = 0.0;
		double normB = 0.0;
		for (int i = 0; i < a.size(); i++) {
			double ai = a.get(i);
			double bi = b.get(i);
			dotProduct += ai * bi;
			normA += ai * ai;
			normB += bi * bi;
		}
		double denominator = Math.sqrt(normA) * Math.sqrt(normB);
		return denominator == 0.0 ? 0.0 : dotProduct / denominator;
	}

	private void consolidateDirectivesForTool(String toolName, List<Map<String, Object>> existing,
			IModelEngine modelEngine) {
		List<Map<String, Object>> existingForLLM = new ArrayList<>();
		for (Map<String, Object> row : existing) {
			Map<String, Object> slim = new LinkedHashMap<>();
			slim.put("directive", row.get("directive"));
			slim.put("occurrence_count", row.get("occurrence_count"));
			existingForLLM.add(slim);
		}

		String systemPrompt = "You are an expert at consolidating and de-duplicating tool-use directives.\n"
				+ "CONTEXT: The tool '" + toolName + "' has accumulated " + existing.size() + " learned directives. "
				+ "You must consolidate them into a smaller, high-quality canonical set.\n"
				+ "TASK:\n"
				+ "1. Group semantically equivalent or overlapping directives and merge them into a single, "
				+ "   maximally specific and actionable directive.\n"
				+ "2. For merged groups, the occurrence_count of the merged row is the SUM of the individual "
				+ "   occurrence_counts — this reflects total user signal strength.\n"
				+ "3. Preserve every DISTINCT behavioral constraint. Do not drop any directive that represents "
				+ "   a unique behavioral rule with no semantic equivalent among the others.\n"
				+ "4. Output at most " + MAX_CONSOLIDATED_DIRECTIVES + " directives.\n"
				+ "5. Each directive must be a single, complete, imperative sentence. "
				+ "   Do not add explanations, hedges, or qualifiers.\n"
				+ "RULES:\n"
				+ "- Do not invent new directives not supported by the input.\n"
				+ "- Do not split one directive into multiple unless the input already contains distinct sub-rules.\n"
				+ "- Prefer the most specific and actionable wording when merging.\n"
				+ "RESPONSE FORMAT: Return ONLY a JSON array with no prose, markdown, or code fences. "
				+ "Each element must have exactly two fields: directive (string) and occurrence_count (integer).\n"
				+ "INPUT DIRECTIVES:\n" + GSON.toJson(existingForLLM);

		Room tempRoom = new Room();
		tempRoom.setId(UUID.randomUUID().toString());
		tempRoom.setInsight(this.insight);

		InputMessage msg = InputMessage.builder(tempRoom).withSystemPrompt(systemPrompt).build();
		ResponseMessage modelResponse;
		try {
			modelResponse = tempRoom.ask(msg, modelEngine, null, false);
		} catch (RuntimeException e) {
			throw new IllegalStateException("Consolidation LLM call failed for tool: " + toolName, e);
		}
		String responseText = modelResponse == null ? null : stringValue(modelResponse.getContent());
		List<Map<String, Object>> consolidated = parseConsolidatedDirectives(responseText);
		if (consolidated.isEmpty()) {
			LOGGER.warn("Consolidation returned no directives for tool '{}', skipping replace", toolName);
			return;
		}
		for (Map<String, Object> row : consolidated) {
			String directive = (String) row.get("directive");
			if (directive != null) {
				try {
					List<Double> embedding = generateEmbedding(directive);
					row.put("directive_embedding", embeddingToJson(embedding));
				} catch (Exception e) {
					LOGGER.warn("Failed to generate embedding for consolidated directive of tool '{}'", toolName, e);
					row.put("directive_embedding", null);
				}
			}
		}
		ModelInferenceLogsUtils.replaceDirectivesForTool(toolName, consolidated);
		LOGGER.info("Consolidated {} directives for tool '{}' into {}", existing.size(), toolName,
				consolidated.size());
	}

	private List<Map<String, Object>> parseConsolidatedDirectives(String responseText) {
		List<Map<String, Object>> result = new ArrayList<>();
		JsonElement parsed = parseJsonSlice(responseText, "Directive consolidator");
		if (!parsed.isJsonArray()) {
			throw new IllegalStateException("Directive consolidator did not return a JSON array");
		}
		for (JsonElement element : parsed.getAsJsonArray()) {
			if (element == null || !element.isJsonObject()) {
				continue;
			}
			JsonObject item = element.getAsJsonObject();
			String directive = stringValue(item.get("directive"));
			if (directive == null) {
				continue;
			}
			int occurrenceCount = 1;
			try {
				JsonElement countElement = item.get("occurrence_count");
				if (countElement != null && !countElement.isJsonNull()) {
					occurrenceCount = countElement.getAsInt();
				}
			} catch (Exception ignored) {
			}
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("directive", directive);
			row.put("occurrence_count", occurrenceCount);
			result.add(row);
		}
		return result;
	}

	private JsonElement parseJsonSlice(String responseText, String sourceName) {
		if (responseText == null || responseText.isBlank()) {
			throw new IllegalStateException(sourceName + " returned an empty response");
		}
		int arrayStart = responseText.indexOf('[');
		int arrayEnd = responseText.lastIndexOf(']');
		String jsonText = null;
		if (arrayStart >= 0 && arrayEnd > arrayStart) {
			jsonText = responseText.substring(arrayStart, arrayEnd + 1);
		}
		if (jsonText == null) {
			int objectStart = responseText.indexOf('{');
			int objectEnd = responseText.lastIndexOf('}');
			if (objectStart >= 0 && objectEnd > objectStart) {
				jsonText = responseText.substring(objectStart, objectEnd + 1);
			}
		}
		if (jsonText == null) {
			throw new IllegalStateException(sourceName + " response did not contain JSON");
		}
		try {
			return JsonParser.parseString(jsonText);
		} catch (RuntimeException e) {
			throw new IllegalStateException(sourceName + " returned malformed JSON", e);
		}
	}

	private String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof JsonElement element) {
			if (element.isJsonNull()) {
				return null;
			}
			value = element.isJsonPrimitive() ? element.getAsString() : element.toString();
		}
		String text = value.toString().trim();
		return text.isEmpty() ? null : text;
	}

	@Override
	public String getReactorDescription() {
		return "Processes /remember directives from the last 24 hours and generates global learned tool directives.";
	}
}
