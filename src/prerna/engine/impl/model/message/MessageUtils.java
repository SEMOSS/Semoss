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
package prerna.engine.impl.model.message;

import java.lang.reflect.Type;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.gson.SemossDateAdapter;

/**
 * Helper methods for message serialization/deserialization, legacy
 * compatibility shims, prompt conversion, and room-file media handling.
 */
public class MessageUtils {

	private static Logger classLogger = LogManager.getLogger(MessageUtils.class);

	private static final ExclusionStrategy NO_ROOM_INSIGHT_SOCKET_EXCLUSION = new ExclusionStrategy() {
		@Override
		public boolean shouldSkipField(FieldAttributes f) {
			String fieldName = f.getName();
			if ("room".equals(fieldName) || "insight".equals(fieldName)) {
				return true;
			}
			Type declaredType = f.getDeclaredType();
			if (declaredType instanceof Class<?>) {
				Class<?> declaredClass = (Class<?>) declaredType;
				if (Room.class.isAssignableFrom(declaredClass) || Insight.class.isAssignableFrom(declaredClass)
						|| Socket.class.isAssignableFrom(declaredClass)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public boolean shouldSkipClass(Class<?> clazz) {
			return Room.class.isAssignableFrom(clazz) || Insight.class.isAssignableFrom(clazz)
					|| Socket.class.isAssignableFrom(clazz);
		}
	};

	// For DB: skips "room", "insight", "socket", and "base64Data"
	private static final Gson GSON_FOR_DB = new GsonBuilder().disableHtmlEscaping()
			.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
			.registerTypeAdapter(MessagePart.class, new MessagePartAdapter())
			.addSerializationExclusionStrategy(NO_ROOM_INSIGHT_SOCKET_EXCLUSION)
			.addSerializationExclusionStrategy(new ExclusionStrategy() {
				@Override
				public boolean shouldSkipField(FieldAttributes f) {
					return "base64Data".equals(f.getName());
				}

				@Override
				public boolean shouldSkipClass(Class<?> clazz) {
					return false;
				}
			}).create();

	// For Python: skips "room", "insight", "socket", "paramMap", includes
	// base64Data
	private static final Gson GSON_FOR_PY = new GsonBuilder().disableHtmlEscaping()
			.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
			.registerTypeAdapter(MessagePart.class, new MessagePartAdapter())
			.addSerializationExclusionStrategy(NO_ROOM_INSIGHT_SOCKET_EXCLUSION)
			.addSerializationExclusionStrategy(new ExclusionStrategy() {
				@Override
				public boolean shouldSkipField(FieldAttributes f) {
					return "paramMap".equals(f.getName());
				}

				@Override
				public boolean shouldSkipClass(Class<?> clazz) {
					return false;
				}
			}).create();

	private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
	}.getType();

	// ---- Serialization/Deserialization ----

	/**
	 * Normalize a tool-call `arguments` value into something the Python side can
	 * deserialize without double-escaping. The provider wire format delivers
	 * arguments as a JSON-encoded string; if we leave that string in place, Gson
	 * escapes the inner quotes a second time and json.loads on the Python side
	 * chokes on payloads with shell escapes, code edits, newlines, etc. Parsing to
	 * a Map here means args ride as a dict alongside every other field. On parse
	 * failure we return the raw string so the downstream builder can still forward
	 * it verbatim.
	 */
	private static Object toolArgumentsForPy(Object argsRaw) {
		if (argsRaw == null) {
			return new HashMap<>();
		}
		if (!(argsRaw instanceof String)) {
			// some providers deliver a structured object already; re-serialize so
			// the shape matches the string-input path
			return GSON_FOR_PY.toJson(argsRaw);
		}
		try {
			Map<String, Object> map = GSON_FOR_PY.fromJson((String) argsRaw, MAP_TYPE);
			return map != null ? map : argsRaw;
		} catch (Exception e) {
			return argsRaw;
		}
	}

	/**
	 * API compatibility: add legacy flat fields into a map built from a message
	 * JSON. This keeps FE consumers working while storage stays on parts-based
	 * schema.
	 *
	 * @param msg    input message source
	 * @param target output map to enrich (created when null)
	 * @return enriched map containing legacy input fields
	 */
	@Deprecated
	public static Map<String, Object> applyLegacyInputFields(InputMessage msg, Map<String, Object> target) {
		if (target == null) {
			target = new LinkedHashMap<>();
		}
		if (msg == null) {
			return target;
		}
		if (msg.getMessageType() != null) {
			target.put("type", msg.getMessageType().name());
		}
		String inputPrompt = msg.getInputPrompt();
		if (inputPrompt != null) {
			target.put("inputPrompt", inputPrompt);
		}
		String inputUIPrompt = msg.getInputUIPrompt();
		if (inputUIPrompt != null) {
			target.put("inputUIPrompt", inputUIPrompt);
		}
		String systemPrompt = msg.getSystemPrompt();
		if (systemPrompt != null && !systemPrompt.isEmpty()) {
			target.put("systemPrompt", systemPrompt);
		}
		String toolCallId = msg.getToolCallId();
		if (toolCallId != null) {
			target.put("tool_call_id", toolCallId);
		}
		String toolName = msg.getToolName();
		if (toolName != null) {
			target.put("tool_name", toolName);
		}
		String toolStatus = msg.getToolStatus();
		if (toolStatus != null) {
			target.put("tool_status", toolStatus);
		}
		Map<String, Object> toolParams = msg.getToolParameterValues();
		if (toolParams != null) {
			target.put("tool_parameter_values", toolParams);
		}
		List<MessageInputMedia> mediaInputs = msg.getMediaInfos();
		if (mediaInputs == null) {
			mediaInputs = new ArrayList<>();
		}
		target.put("mediaInputs", mediaInputs);
		return target;
	}

	/**
	 * Converts a JSON object string to a Map<String, Object>
	 *
	 * @param json The JSON string (must be a JSON object: { ... })
	 * @return The parsed map representation
	 * @throws IllegalArgumentException if the input is null/blank or not a JSON
	 *                                  object
	 */
	public static Map<String, Object> jsonToMapForPixelReturn(String json) {
		if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
			throw new IllegalArgumentException("Input must be a valid JSON object string.");
		}
		return GSON_FOR_DB.fromJson(json, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * Deserializes a single message JSON payload into either {@link InputMessage}
	 * or {@link ResponseMessage}, using schema discriminators and legacy fallbacks.
	 *
	 * @param json message JSON string
	 * @param room room context used during post-load normalization
	 * @return deserialized message, or {@code null} when deserialization yields no
	 *         message
	 */
	public static AbstractMessage fromJson(String json, Room room) {
		JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();

		// Prefer explicit discriminator first (new format), then legacy type prefix.
		MessageIO io = null;
		if (jsonObj.has("io") && !jsonObj.get("io").isJsonNull()) {
			try {
				io = MessageIO.valueOf(jsonObj.get("io").getAsString());
			} catch (IllegalArgumentException ignore) {
				io = null;
			}
		}

		String rawType = null;
		if (io == null && jsonObj.has("type") && !jsonObj.get("type").isJsonNull()) {
			rawType = jsonObj.get("type").getAsString();
			if (rawType != null) {
				rawType = rawType.trim();
			}
		}

		boolean isResponse = false;
		if (io != null) {
			isResponse = (io == MessageIO.OUTPUT);
		} else if (rawType != null) {
			isResponse = rawType.startsWith("RESPONSE_");
		} else if (jsonObj.has("parts") && jsonObj.get("parts").isJsonArray()) {
			// Minimal parts-only inference: tool calls / thinking belong to assistant
			// outputs,
			// tool results / system prompt belong to user inputs.
			for (JsonElement p : jsonObj.getAsJsonArray("parts")) {
				if (p == null || !p.isJsonObject()) {
					continue;
				}
				JsonObject partObj = p.getAsJsonObject();
				if (!partObj.has("type") || partObj.get("type").isJsonNull()) {
					continue;
				}
				String partType = partObj.get("type").getAsString();
				if ("TOOL_CALL".equals(partType) || "THINKING".equals(partType)) {
					isResponse = true;
					break;
				}
				if ("TOOL_RESULT".equals(partType) || "SYSTEM".equals(partType)) {
					isResponse = false;
					break;
				}
			}
		} else {
			// Legacy fallback
			isResponse = jsonObj.has("content") || jsonObj.has("thinking") || jsonObj.has("tool_responses");
		}

		AbstractMessage message = isResponse ? GSON_FOR_DB.fromJson(json, ResponseMessage.class)
				: GSON_FOR_DB.fromJson(json, InputMessage.class);

		if (message != null) {
			message.normalizeAfterLoad(room);
		}
		return message;
	}

	/**
	 * Serializes a message for DB persistence using the DB-safe Gson profile.
	 *
	 * @param msg message to serialize
	 * @return serialized JSON
	 */
	public static String toJson(AbstractMessage msg) {
		if (msg != null) {
			msg.normalizeForWrite();
		}
		return GSON_FOR_DB.toJson(msg);
	}

	/**
	 * Serializes a message for Python/model execution payloads, including image
	 * base64 when available.
	 *
	 * @param msg message to serialize
	 * @return serialized JSON
	 */
	public static String toJsonWithImage(AbstractMessage msg) {
		if (msg != null) {
			msg.normalizeForWrite();
		}
		return GSON_FOR_PY.toJson(msg);
	}

	/**
	 * Deserializes a JSON array of messages.
	 *
	 * @param jsonArrayString message-array JSON
	 * @param room            room context used during post-load normalization
	 * @return ordered list of deserialized messages
	 */
	public static List<AbstractMessage> fromJsonArray(String jsonArrayString, Room room) {
		if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
			return new ArrayList<>();
		}
		JsonArray array = JsonParser.parseString(jsonArrayString).getAsJsonArray();
		List<AbstractMessage> result = new ArrayList<>();
		for (JsonElement elem : array) {
			AbstractMessage message = fromJson(elem.toString(), room);
			if (message != null) {
				result.add(message);
			}
		}
		return result;
	}

	// --- Core two serialization methods ---

	/**
	 * Serializes a list of messages for DB persistence (without inline base64 media
	 * payloads).
	 *
	 * @param msgs messages to serialize
	 * @return JSON array string; {@code "[]"} when empty
	 */
	public static String toJsonArray(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return "[]";
		}
		for (AbstractMessage msg : msgs) {
			if (msg != null) {
				msg.normalizeForWrite();
			}
		}
		return GSON_FOR_DB.toJson(msgs);
	}

	/**
	 * Builds the current branch history (root to latest message) and serializes it
	 * for model execution payloads.
	 *
	 * @param messages full room message list
	 * @return branch JSON including image data
	 */
	public static String getCurrentMessageHistory(List<AbstractMessage> messages) {
		return toJsonArrayWithImageData(getMessageBranchWithNewMessage(messages, null));
	}

	/**
	 * Builds the branch history ending in {@code newMessage} and serializes it for
	 * model execution payloads.
	 *
	 * @param messages   full room message list
	 * @param newMessage new leaf message to append as branch tail
	 * @return branch JSON including image data
	 */
	public static String getMessageHistoryWithNewMessage(List<AbstractMessage> messages, AbstractMessage newMessage) {
		return toJsonArrayWithImageData(getMessageBranchWithNewMessage(messages, newMessage));
	}

	/**
	 * Serializes messages for Python/model execution and ensures image parts
	 * contain base64 payloads.
	 *
	 * @param msgs messages to serialize
	 * @return JSON array string; {@code "[]"} when empty
	 */
	public static String toJsonArrayWithImageData(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return "[]";
		}
		for (AbstractMessage msg : msgs) {
			if (msg != null) {
				msg.normalizeForWrite();
			}
		}
		// Ensure base64Data is loaded for all images
		for (AbstractMessage msg : msgs) {
			if (msg instanceof InputMessage) {
				InputMessage input = (InputMessage) msg;
				if (input.hasMediaInputs()) {
					for (MessageInputMedia img : input.getMediaInfos()) {
						// Populate the field (it will actually load the file if needed)
						img.setBase64Data(img.getBase64Data());
					}
				}
			}
		}
		return GSON_FOR_PY.toJson(msgs);
	}

	/**
	 * Returns a root-to-leaf message branch ending at {@code newMessage} (or the
	 * current tail message when {@code newMessage} is null).
	 *
	 * @param messages   complete message list
	 * @param newMessage optional branch leaf override
	 * @return ordered branch messages from root to leaf
	 */
	public static List<AbstractMessage> getMessageBranchWithNewMessage(List<AbstractMessage> messages,
			AbstractMessage newMessage) {
		// 1. Build lookup map (messageId to message)
		Map<String, AbstractMessage> idMap = new HashMap<>();
		for (AbstractMessage m : messages) {
			if (m.getMessageId() != null) {
				idMap.put(m.getMessageId(), m);
			}
		}
		// 2. Climb up parent chain
		List<AbstractMessage> history = new ArrayList<>();
		if (newMessage != null) {
			history.add(newMessage);
		} else {
			history.add(messages.getLast());
		}
		String currentId = history.getLast().getParentMessageId();
		while (currentId != null) {
			AbstractMessage m = idMap.get(currentId);
			if (m == null) {
				break;
			}
			history.add(m);
			// parentMessageId may be null/empty String
			currentId = m.getParentMessageId();
			if (currentId == null || currentId.isEmpty()) {
				break;
			}
		}
		// 3. Messages are from newest-to-oldest; reverse to get root-to-leaf
		Collections.reverse(history);
		return history;
	}

	/**
	 * Returns a root-to-node branch by walking parent links from
	 * {@code parentMessageId}.
	 *
	 * @param messages        complete message list
	 * @param parentMessageId leaf message id from which to walk parent links
	 * @return ordered branch messages from root to requested parent
	 */
	public static List<AbstractMessage> getMessageBranchFromParent(List<AbstractMessage> messages,
			String parentMessageId) {
		// 1. Build lookup map (messageId to message)
		Map<String, AbstractMessage> idMap = new HashMap<>();
		for (AbstractMessage m : messages) {
			if (m.getMessageId() != null) {
				idMap.put(m.getMessageId(), m);
			}
		}
		// 2. Climb up parent chain
		List<AbstractMessage> history = new ArrayList<>();
		String currentId = parentMessageId;
		while (currentId != null) {
			AbstractMessage m = idMap.get(currentId);
			if (m == null) {
				break;
			}
			history.add(m);
			// parentMessageId may be null/empty String
			currentId = m.getParentMessageId();
			if (currentId == null || currentId.isEmpty()) {
				break;
			}
		}
		// 3. Messages are from newest-to-oldest; reverse to get root-to-leaf
		Collections.reverse(history);
		return history;
	}

	/**
	 * Converts a mixed-format full prompt payload (Chat Completions/Responses API
	 * style) into normalized room messages.
	 *
	 * @param fullPrompt  full prompt payload as JSON string or list
	 * @param room        room context used for message construction
	 * @param modelEngine model engine used for message model typing
	 * @return normalized message list
	 * @throws IllegalArgumentException when {@code fullPrompt} is neither a JSON
	 *                                  string nor a list payload
	 */
	public static List<AbstractMessage> convertFullPrompt(Object fullPrompt, Room room, IModelEngine modelEngine) {
		List<AbstractMessage> result = new ArrayList<>();
		List<?> promptList;

		String systemPrompt = null;

		if (fullPrompt instanceof String) {
			promptList = GSON_FOR_PY.fromJson((String) fullPrompt, List.class);
		} else if (fullPrompt instanceof List<?>) {
			promptList = (List<?>) fullPrompt;
		} else {
			throw new IllegalArgumentException("fullPrompt must be a JSON string or List<Map>.");
		}

		// accumulates consecutive function_call entries (parallel tool calls) into one
		// RESPONSE_TOOL message
		List<Map<String, Object>> pendingFunctionCalls = new ArrayList<>();
		for (Object o : promptList) {
			if (!(o instanceof Map)) {
				continue;
			}
			Map<?, ?> map = (Map<?, ?>) o;

			// first we will check the type because responses api returns function_call /
			// function_call_output with no role
			String entryType = asStringOrNull(map.get("type"));

			// flush accumulated function_calls once we hit a non-function_call entry
			if (!"function_call".equals(entryType) && !pendingFunctionCalls.isEmpty()) {
				ResponseMessage.Builder fcBuilder = ResponseMessage.builder();
				fcBuilder.withType(MessageType.RESPONSE_TOOL);
				fcBuilder.withToolResponses(new ArrayList<>(pendingFunctionCalls));
				result.add(fcBuilder.build());
				pendingFunctionCalls.clear();
			}

			if ("function_call".equals(entryType)) {
				String callId = asStringOrNull(map.get("call_id"));
				String funcName = asStringOrNull(map.get("name"));
				String arguments = asStringOrNull(map.get("arguments"));
				Map<String, Object> flatTool = new HashMap<>();
				flatTool.put("id", callId);
				flatTool.put("type", "function");
				flatTool.put("name", funcName);
				flatTool.put("arguments", arguments);
				pendingFunctionCalls.add(flatTool);
				continue;
			}
			if ("function_call_output".equals(entryType)) {
				String callId = asStringOrNull(map.get("call_id"));
				String output = asStringOrNull(map.get("output"));
				result.add(InputMessage.toolExecution(room, callId, null, output, null, null, false));
				continue;
			}

			// this will continue the normal flow
			String role = asStringOrNull(map.get("role"));
			Object contentObj = map.get("content");
			String content = parseContentMap(contentObj);

			// -------- SYSTEM --------
			if ("system".equals(role)) {
				// Just cache the system prompt; don't set it in Room or append as a message
				systemPrompt = content;
				continue;
			}

			// -------- USER (TEXT and/or IMAGE) --------
			if ("user".equals(role)) {
				List<String> mediaInputList = new ArrayList<>();
				String textPart = "";
				// OpenAI-style: content is a list of dicts with type text/image_url
				if (contentObj instanceof List<?>) {
					for (Object part : (List<?>) contentObj) {
						if (!(part instanceof Map)) {
							continue;
						}
						Map<?, ?> partMap = (Map<?, ?>) part;
						String type = asStringOrNull(partMap.get("type"));
						if ("text".equals(type) || "input_text".equals(type)) {
							textPart += asStringOrNull(partMap.get("text"));
						} else if ("image_url".equals(type) || "input_image".equals(type)) {
							// Chat Completions: { "type": "image_url", "image_url": { "url": ... } }
							// Responses API: { "type": "input_image", "image_url": "..." }
							Object imgURLObj = partMap.get("image_url");
							String url = null;
							if (imgURLObj instanceof String) {
								url = (String) imgURLObj;
							} else if (imgURLObj instanceof Map) {
								url = asStringOrNull(((Map<?, ?>) imgURLObj).get("url"));
							}
							if (url != null) {
								mediaInputList.add(url);
							}
						}
					}
				} else if (contentObj instanceof String) {
					textPart = (String) contentObj;
				}

				InputMessage.Builder builder = InputMessage.builder(room).withText(textPart)
						.withModelType(modelEngine.getModelType());

				if (!mediaInputList.isEmpty()) {
					builder.withMediaUrls(mediaInputList);
				}

				// If you receive extra tools for this turn:
				Object toolsObj = map.get("tools");
				if (toolsObj instanceof List<?>) {
					builder.withTools((List<Map<String, Object>>) toolsObj);
				}

				result.add(builder.build());
				continue;
			}

			// -------- ASSISTANT --------
			if ("assistant".equals(role)) {
				Object toolCallsObj = map.get("tool_calls");

				// -- If assistant provides tool_calls, flatten as tool_responses
				if (toolCallsObj instanceof List<?> && !((List<?>) toolCallsObj).isEmpty()) {
					List<Map<String, Object>> flattenedTools = new ArrayList<>();
					for (Object elem : (List<?>) toolCallsObj) {
						if (elem instanceof Map) {
							Map<?, ?> callMap = (Map<?, ?>) elem;
							Map<String, Object> flatTool = new HashMap<>();
							flatTool.put("id", asStringOrNull(callMap.get("id"))); // tool_call id
							flatTool.put("type", asStringOrNull(callMap.get("type")));
							// Vertex/Gemini extended-thinking signature, attached upstream
							Object thoughtSig = callMap.get("thought_signature");
							if (thoughtSig instanceof String && !((String) thoughtSig).isEmpty()) {
								flatTool.put("thought_signature", thoughtSig);
							}
							// openAI: "function": {...}
							Object functionObj = callMap.get("function");
							if ("function".equals(flatTool.get("type")) && functionObj instanceof Map) {
								Map<?, ?> funcMap = (Map<?, ?>) functionObj;
								flatTool.put("name", asStringOrNull(funcMap.get("name")));
								flatTool.put("arguments", toolArgumentsForPy(funcMap.get("arguments")));
							} else {
								// For non-function tools, flatten as key-values
								for (Map.Entry<?, ?> entry : callMap.entrySet()) {
									if (!"id".equals(entry.getKey()) && !"type".equals(entry.getKey())) {
										flatTool.put(String.valueOf(entry.getKey()), entry.getValue());
									}
								}
							}
							flattenedTools.add(flatTool);
						}
					}
					ResponseMessage.Builder builder = ResponseMessage.builder();
					builder.withType(MessageType.RESPONSE_TOOL); // This marks as RESPONSE_TOOL
					builder.withText(asStringOrNull(content)); // Preserves the content/text if present
					builder.withToolResponses(flattenedTools);
					result.add(builder.build());
					continue;
				}
				// -- Otherwise: classic assistant response
				ResponseMessage.Builder builder = ResponseMessage.builder();
				builder.withText(asStringOrNull(content));
				result.add(builder.build());
				continue;
			}

			// -------- TOOL/FUNCTION CALL (user-provided tools executed) --------
			if ("function".equals(role) || "tool".equals(role)) {
				String toolName = asStringOrNull(map.get("name"));
				String toolResult = asStringOrNull(map.get("content"));
				String toolCallId = asStringOrNull(map.get("tool_call_id"));

				// Add as tool execution message (in my earlier pattern)
				AbstractMessage toolExecMsg = InputMessage.toolExecution(room, toolCallId, toolName, toolResult, null,
						null, false);
				result.add(toolExecMsg);
				continue;
			}

		}

		// Flush any function_calls that were at the tail of the list
		if (!pendingFunctionCalls.isEmpty()) {
			ResponseMessage.Builder fcBuilder = ResponseMessage.builder();
			fcBuilder.withType(MessageType.RESPONSE_TOOL);
			fcBuilder.withToolResponses(new ArrayList<>(pendingFunctionCalls));
			result.add(fcBuilder.build());
		}

		// ------ Attach system prompt to last input message, if any ------
		if (systemPrompt != null) {
			// find the last InputMessage in result
			for (int i = result.size() - 1; i >= 0; i--) {
				AbstractMessage m = result.get(i);
				if (m instanceof InputMessage) {
					((InputMessage) m).setSystemPrompt(systemPrompt);
					break;
				}
			}
		}

		return result;
	}

	/**
	 * Converts OpenAI-style tool definitions to MCP-compatible tool definitions.
	 * Built-in non-function tools are passed through unchanged.
	 *
	 * @param inputTools tool definitions from incoming API payload
	 * @return MCP-compatible tool definitions
	 */
	public static List<Map<String, Object>> convertOpenAIToMCPTools(List<Map<String, Object>> inputTools) {
		List<Map<String, Object>> newTools = new ArrayList<>();
		for (Map<String, Object> tool : inputTools) {
			String type = (String) tool.get("type");

			// Built-in tools (from OpenAI) (web_search, code_interpreter, file_search,
			// etc.)
			// have a non-"function" type and no nested function/inputSchema/parameters
			// definition.
			// Pass these through unchanged
			// Maybe a better way to identify these eventually? But I have to pass these on
			// as is..
			if (type != null && !"function".equals(type) && !tool.containsKey("function")
					&& !tool.containsKey("inputSchema") && !tool.containsKey("parameters")) {
				newTools.add(new LinkedHashMap<>(tool));
				continue;
			}

			Map<String, Object> result = new LinkedHashMap<>();
			String name = null, description = null, title = null;
			Map<String, Object> inputSchema = null;

			// Handle OpenAI style with nested "function"
			if (tool.containsKey("function") && tool.get("function") instanceof Map) {
				@SuppressWarnings("unchecked")
				Map<String, Object> function = (Map<String, Object>) tool.get("function");
				name = function.containsKey("name") ? (String) function.get("name") : (String) tool.get("name");
				description = function.containsKey("description") ? (String) function.get("description")
						: (String) tool.get("description");
				Object params = function.get("parameters");
				if (params instanceof Map) {
					inputSchema = new LinkedHashMap<>((Map) params);
				}
			} else {
				// Already MCP-style or close-to
				name = (String) tool.get("name");
				description = (String) tool.get("description");
				title = (String) tool.get("title");
				if (tool.containsKey("inputSchema") && tool.get("inputSchema") instanceof Map) {
					inputSchema = new LinkedHashMap<>((Map) tool.get("inputSchema"));
				} else if (tool.containsKey("parameters") && tool.get("parameters") instanceof Map) {
					inputSchema = new LinkedHashMap<>((Map) tool.get("parameters"));
				}
			}

			if (title == null || title.trim().isEmpty()) {
				title = MCPUtility.formatToTitleCase(name);
			}

			result.put("name", name);
			result.put("description", description);
			result.put("title", title);
			if (inputSchema != null) {
				result.put("inputSchema", inputSchema);
			}
			newTools.add(result);
		}
		return newTools;
	}

	/**
	 * Normalizes tool-choice input (string/object) into MCP tool-choice shape.
	 *
	 * @param toolChoiceInput tool-choice value from caller payload
	 * @return normalized MCP tool-choice map
	 */
	public static Map<String, Object> toMCPToolChoice(Object toolChoiceInput) {
		// Handle String
		if (toolChoiceInput instanceof String) {
			String val = ((String) toolChoiceInput).trim().toLowerCase();
			switch (val) {
			case "auto":
				return makeToolChoice(ToolChoiceType.AUTO, null);
			case "none":
				return makeToolChoice(ToolChoiceType.NONE, null);
			case "required":
				return makeToolChoice(ToolChoiceType.REQUIRED, null);
			default:
				// "any" or unknown: treat as auto
				return makeToolChoice(ToolChoiceType.AUTO, null);
			}
		}

		// Handle Map
		if (toolChoiceInput instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> obj = new HashMap<>((Map<String, Object>) toolChoiceInput); // Defensive copy

			// --- Already in MCP format ---
			Object typeObj = obj.get("type");
			if (typeObj instanceof String) {
				String type = ((String) typeObj).toLowerCase();
				switch (type) {
				case "auto":
				case "any": // map OpenAI "any" to MCP/AUTO
					return makeToolChoice(ToolChoiceType.AUTO, null);
				case "none":
					return makeToolChoice(ToolChoiceType.NONE, null);
				case "required":
					return makeToolChoice(ToolChoiceType.REQUIRED, null);
				case "forced":
					// (assume correct MCP style)
					Object nameF = obj.get("name");
					return makeToolChoice(ToolChoiceType.FORCED, nameF != null ? nameF.toString() : null);
				case "function":
					// OpenAI style object: {"type":"function", "name":"..."}
					Object forcedName = obj.get("name");
					if (forcedName instanceof String) {
						return makeToolChoice(ToolChoiceType.FORCED, forcedName.toString());
					}
					// Don't handle allowed_tools for now, skip
					break;
				default:
					// Fallback
					return makeToolChoice(ToolChoiceType.AUTO, null);
				}
			}
		}
		// Fallback
		return makeToolChoice(ToolChoiceType.AUTO, null);
	}

	/**
	 * Returns the object as a string when the value is a {@link String}; otherwise
	 * returns {@code null}.
	 *
	 * @param o value to inspect
	 * @return string value or {@code null}
	 */
	private static String asStringOrNull(Object o) {
		return (o instanceof String) ? (String) o : null;
	}

	/**
	 * Extracts text content from mixed content payloads (plain string or list of
	 * typed content maps).
	 *
	 * @param o content payload
	 * @return concatenated text content or {@code null}
	 */
	private static String parseContentMap(Object o) {
		if (o instanceof List<?>) {
			// OpenAI-style: content is a list of dicts with type text, ignore images
			StringBuilder textBuilder = new StringBuilder();
			for (Object part : (List<?>) o) {
				if (!(part instanceof Map)) {
					continue;
				}
				Map<?, ?> partMap = (Map<?, ?>) part;
				String type = asStringOrNull(partMap.get("type"));
				if ("text".equals(type) || "input_text".equals(type) || "output_text".equals(type)) {
					textBuilder.append(asStringOrNull(partMap.get("text")));
				}
			}
			return textBuilder.toString();
		} else {
			// Regular string
			return asStringOrNull(o);
		}
	}

	// ---- Utility/Convenience methods (maintain if needed) ----

	/**
	 * Backward-compatible alias for {@link #toJsonArray(List)}.
	 *
	 * @param msgs messages to serialize
	 * @return DB-safe message JSON
	 */
	public static String getMessagesForDatabase(List<AbstractMessage> msgs) {
		return toJsonArray(msgs);
	}

	/**
	 * Backward-compatible alias for {@link #toJsonArrayWithImageData(List)}.
	 *
	 * @param msgs messages to serialize
	 * @return execution payload message JSON
	 */
	public static String getMessagesForPy(List<AbstractMessage> msgs) {
		return toJsonArrayWithImageData(msgs);
	}

	/**
	 * Supported tool-choice strategy values.
	 */
	public enum ToolChoiceType {
		FORCED, AUTO, REQUIRED, NONE
	}

	/**
	 * Creates an MCP tool-choice payload.
	 *
	 * @param type tool-choice strategy
	 * @param name forced tool name (used only when {@code type == FORCED})
	 * @return tool-choice map
	 */
	public static Map<String, Object> makeToolChoice(ToolChoiceType type, String name) {
		Map<String, Object> toolChoice = new HashMap<>();
		toolChoice.put("type", type.name().toLowerCase());
		if (type == ToolChoiceType.FORCED && name != null && !name.isEmpty()) {
			toolChoice.put("name", name);
		}
		return toolChoice;
	}

	/**
	 * API compatibility: add legacy flat fields into a map built from a response
	 * JSON.
	 *
	 * @param msg    response message source
	 * @param target output map to enrich (created when null)
	 * @return enriched map containing legacy response fields
	 */
	@Deprecated
	public static Map<String, Object> applyLegacyResponseFields(ResponseMessage msg, Map<String, Object> target) {
		if (target == null) {
			target = new LinkedHashMap<>();
		}
		if (msg == null) {
			return target;
		}
		if (msg.getMessageType() != null) {
			target.put("type", msg.getMessageType().name());
		}
		String content = msg.getContent();
		if (content != null) {
			target.put("content", content);
		}
		String thinking = msg.getThinking();
		if (thinking != null) {
			target.put("thinking", thinking);
		}
		List<Map<String, Object>> toolResponses = msg.getToolResponses();
		if (toolResponses == null) {
			toolResponses = new ArrayList<>();
		}
		target.put("tool_responses", toolResponses);
		return target;
	}

}
