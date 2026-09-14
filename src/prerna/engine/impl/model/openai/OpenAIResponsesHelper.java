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
package prerna.engine.impl.model.openai;

import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.impl.model.ModelPixelInvoker;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse.ToolResponse;

/**
 * Helper class for formatting OpenAI Responses API responses, both the single
 * payload and the event SSE conversation. Simplified for raw passthrough mode -
 * just forwards OpenAI events directly.
 */
public final class OpenAIResponsesHelper {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	public static void writeSSEEvent(Map<String, Object> rawEvent, Writer writer) throws IOException {
		String eventType = (String) rawEvent.get("type");
		if (eventType != null) {
			writer.write("event: " + eventType + "\n");
		}
		writer.write("data: " + GSON.toJson(rawEvent) + "\n\n");
		writer.flush();
	}

	/**
	 * Attaches a Responses-API usage object to the inner {@code response} map of a
	 * {@code response.completed} (or similar) event. No-op if all token counts are
	 * null. Use this so streaming clients see real
	 * {@code response.usage.input_tokens} / {@code output_tokens} instead of the
	 * field being absent.
	 */
	@SuppressWarnings("unchecked")
	public static void attachUsage(Map<String, Object> event, Integer inputTokens, Integer outputTokens,
			Integer cachedTokens, Integer reasoningTokens) {
		if (inputTokens == null && outputTokens == null && cachedTokens == null && reasoningTokens == null) {
			return;
		}
		Object respObj = event.get("response");
		if (!(respObj instanceof Map)) {
			return;
		}
		Map<String, Object> resp = (Map<String, Object>) respObj;

		Map<String, Object> usage = new HashMap<>();
		if (inputTokens != null) {
			usage.put("input_tokens", inputTokens);
		}
		if (outputTokens != null) {
			usage.put("output_tokens", outputTokens);
		}
		if (inputTokens != null && outputTokens != null) {
			usage.put("total_tokens", inputTokens + outputTokens);
		}
		if (cachedTokens != null) {
			Map<String, Object> inputDetails = new HashMap<>();
			inputDetails.put("cached_tokens", cachedTokens);
			usage.put("input_tokens_details", inputDetails);
		}
		if (reasoningTokens != null) {
			Map<String, Object> outputDetails = new HashMap<>();
			outputDetails.put("reasoning_tokens", reasoningTokens);
			usage.put("output_tokens_details", outputDetails);
		}
		resp.put("usage", usage);
	}

	// --- 1. Top Level Response Events ---

	public static Map<String, Object> createBaseEvent(String type, int seq, String respId, String model, long ts) {
		Map<String, Object> event = new HashMap<>();
		event.put("type", type);
		event.put("sequence_number", seq);
		Map<String, Object> resp = new HashMap<>();
		resp.put("id", respId);
		resp.put("object", "response");
		resp.put("model", model);
		resp.put("created_at", (double) ts);
		resp.put("status", "in_progress");
		event.put("response", resp);
		return event;
	}

	// --- 2. Item Events ---

	public static void sendItemAdded(Writer w, int seq, String respId, String itemId, int idx, String type,
			String toolName) throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.output_item.added");
		event.put("sequence_number", seq);
		event.put("response_id", respId);
		event.put("output_index", idx);

		Map<String, Object> item = new HashMap<>();
		item.put("id", itemId);
		item.put("type", type);
		item.put("status", "in_progress");

		if ("message".equals(type)) {
			item.put("role", "assistant");
			item.put("content", new ArrayList<>());
		} else if ("function_call".equals(type)) {
			item.put("name", toolName);
			item.put("call_id", itemId);
			item.put("arguments", "");
		}

		event.put("item", item);
		writeSSEEvent(event, w);
	}

	public static void sendItemDone(Writer w, int seq, String respId, String itemId, int idx, String type,
			String finalContent, String toolName) throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.output_item.done");
		event.put("sequence_number", seq);
		event.put("response_id", respId);

		Map<String, Object> item = new HashMap<>();
		item.put("id", itemId);
		item.put("type", type);
		item.put("status", "completed");

		if ("message".equals(type)) {
			item.put("role", "assistant");
			Map<String, Object> textPart = new HashMap<>();
			textPart.put("type", "output_text");
			textPart.put("text", finalContent);
			ArrayList<Object> contentList = new ArrayList<>();
			contentList.add(textPart);
			item.put("content", contentList);
		} else if ("function_call".equals(type)) {
			item.put("call_id", itemId);
			item.put("name", toolName);
			item.put("arguments", finalContent);
		}

		event.put("item", item);
		writeSSEEvent(event, w);
	}

	// --- 3. Content Part Events ---

	public static void sendContentPartAdded(Writer w, int seq, String respId, String itemId, int outputIdx,
			int contentIdx) throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.content_part.added");
		event.put("sequence_number", seq);
		event.put("response_id", respId);
		event.put("item_id", itemId);
		event.put("output_index", outputIdx);
		event.put("content_index", contentIdx);

		Map<String, Object> part = new HashMap<>();
		part.put("type", "output_text");
		part.put("text", "");

		event.put("part", part);
		writeSSEEvent(event, w);
	}

	public static void sendContentPartDone(Writer w, int seq, String respId, String itemId, int outputIdx,
			int contentIdx, String text) throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.content_part.done");
		event.put("sequence_number", seq);
		event.put("response_id", respId);
		event.put("item_id", itemId);
		event.put("output_index", outputIdx);
		event.put("content_index", contentIdx);

		Map<String, Object> part = new HashMap<>();
		part.put("type", "output_text");
		part.put("text", text);

		event.put("part", part);
		writeSSEEvent(event, w);
	}

	// --- 4. Delta Events ---

	public static void sendTextDelta(Writer w, int seq, String respId, String itemId, int outputIdx, int contentIdx,
			String delta) throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.output_text.delta");
		event.put("sequence_number", seq);
		event.put("response_id", respId);
		event.put("item_id", itemId);
		event.put("output_index", outputIdx);
		event.put("content_index", contentIdx);
		event.put("delta", delta);
		writeSSEEvent(event, w);
	}

	public static void sendTextDone(Writer w, int seq, String respId, String itemId, int outputIdx, int contentIdx,
			String finalUniqueText) throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.output_text.done");
		event.put("sequence_number", seq);
		event.put("response_id", respId);
		event.put("item_id", itemId);
		event.put("output_index", outputIdx);
		event.put("content_index", contentIdx);
		event.put("text", finalUniqueText);
		writeSSEEvent(event, w);
	}

	public static void sendToolDelta(Writer w, int seq, String respId, String itemId, int outputIdx, String delta)
			throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.function_call_arguments.delta");
		event.put("sequence_number", seq);
		event.put("response_id", respId);
		event.put("item_id", itemId);
		event.put("output_index", outputIdx);
		event.put("call_id", itemId);
		event.put("delta", delta);
		writeSSEEvent(event, w);
	}

	// --- 5. Image Generation Events ---

	/**
	 * Sends a {@code response.image_generation_call.partial_image} SSE event,
	 * matching the wire shape the {@code openai} SDK parses
	 * ({@code event.partial_image_b64}, {@code event.partial_image_index}). Only
	 * base64 partials are expressible in this event - the OpenAI spec defines no
	 * URL alternative on partial_image, so URL-only media chunks are skipped.
	 */
	public static void sendImageGenerationPartialImage(Writer w, int seq, String respId, String itemId, int outputIdx,
			Map<String, Object> mediaInfo, Object partialImageIndex) throws IOException {
		if (mediaInfo == null) {
			return;
		}
		Object b64 = mediaInfo.get("base64Data");
		if (!(b64 instanceof String) || ((String) b64).isEmpty()) {
			return;
		}
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.image_generation_call.partial_image");
		event.put("sequence_number", seq);
		event.put("response_id", respId);
		event.put("item_id", itemId);
		event.put("output_index", outputIdx);
		event.put("partial_image_index", partialImageIndex);
		event.put("partial_image_b64", b64);
		writeSSEEvent(event, w);
	}

	/**
	 * Sends a bare {@code response.image_generation_call.completed} SSE event. The
	 * actual image bytes are not carried on this event in OpenAI's Responses API
	 * protocol - the final base64 lives on the {@code image_generation_call} item's
	 * {@code result} field, delivered via the subsequent
	 * {@code response.output_item.done} event. This event is just the lifecycle
	 * signal.
	 */
	public static void sendImageGenerationCompleted(Writer w, int seq, String respId, String itemId, int outputIdx)
			throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("type", "response.image_generation_call.completed");
		event.put("sequence_number", seq);
		event.put("response_id", respId);
		event.put("item_id", itemId);
		event.put("output_index", outputIdx);
		writeSSEEvent(event, w);
	}

	/**
	 * Normalizes Codex/Responses API message format to standard OpenAI Chat format.
	 * Converts: content: [{ "type": "input_text", "text": "..." }] To: content:
	 * "..."
	 */
	@SuppressWarnings("unchecked")
	public static Object normalizeMessages(Object input) {
		if (input instanceof String) {
			// Convert a single string input into a standard messages list
			List<Map<String, Object>> messages = new ArrayList<>();
			Map<String, Object> userMessage = new HashMap<>();
			userMessage.put("role", "user");
			userMessage.put("content", input);
			messages.add(userMessage);
			return messages;
		}
		if (!(input instanceof List)) {
			return input;
		}

		List<Map<String, Object>> messages = (List<Map<String, Object>>) input;
		List<Map<String, Object>> normalizedMessages = new ArrayList<>();

		// Buffer to collect consecutive function calls into one assistant message
		List<Map<String, Object>> pendingToolCalls = new ArrayList<>();

		for (Map<String, Object> message : messages) {
			String type = (String) message.get("type");

			if ("function_call".equals(type)) {
				Map<String, Object> toolCall = new HashMap<>();
				toolCall.put("id", message.get("call_id"));
				toolCall.put("type", "function");

				Map<String, Object> function = new HashMap<>();
				function.put("name", message.get("name"));
				Object args = message.get("arguments");
				function.put("arguments", args != null ? args.toString() : "{}");
				toolCall.put("function", function);

				pendingToolCalls.add(toolCall);

			} else if ("function_call_output".equals(type)) {
				if (!pendingToolCalls.isEmpty()) {
					Map<String, Object> assistantMsg = new HashMap<>();
					assistantMsg.put("role", "assistant");
					assistantMsg.put("tool_calls", new ArrayList<>(pendingToolCalls));
					normalizedMessages.add(assistantMsg);
					pendingToolCalls.clear();
				}

				Map<String, Object> toolMsg = new HashMap<>();
				toolMsg.put("role", "tool");
				toolMsg.put("tool_call_id", message.get("call_id"));
				toolMsg.put("content", message.get("output"));
				normalizedMessages.add(toolMsg);

			} else {
				if (!pendingToolCalls.isEmpty()) {
					Map<String, Object> assistantMsg = new HashMap<>();
					assistantMsg.put("role", "assistant");
					assistantMsg.put("tool_calls", new ArrayList<>(pendingToolCalls));
					normalizedMessages.add(assistantMsg);
					pendingToolCalls.clear();
				}

				Map<String, Object> normalizedMsg = new HashMap<>(message);

				normalizedMsg.remove("type");

				Object content = normalizedMsg.get("content");
				if (content instanceof List) {
					List<Map<String, Object>> contentList = (List<Map<String, Object>>) content;
					StringBuilder flattenedText = new StringBuilder();
					for (Map<String, Object> part : contentList) {
						if (part.containsKey("text")) {
							if (flattenedText.length() > 0) {
								flattenedText.append("\n");
							}
							flattenedText.append(part.get("text").toString());
						}
					}
					normalizedMsg.put("content", flattenedText.toString());
				}

				if (!normalizedMsg.containsKey("role")) {
					String role = (String) message.get("role");
					if (role == null) {
						role = "user"; // Default
					}
					normalizedMsg.put("role", role);
				}

				normalizedMessages.add(normalizedMsg);
			}
		}

		if (!pendingToolCalls.isEmpty()) {
			Map<String, Object> assistantMsg = new HashMap<>();
			assistantMsg.put("role", "assistant");
			assistantMsg.put("tool_calls", new ArrayList<>(pendingToolCalls));
			normalizedMessages.add(assistantMsg);
		}

		return normalizedMessages;
	}

	/**
	 * Process AskModelEngineResponse into native OpenAI Responses API format Used
	 * for non-streaming responses only.
	 *
	 * @param engineId
	 * @param llmResponse
	 * @return
	 */
	public static Map<String, Object> processAskModelEngineResponse(String engineId,
			AskModelEngineResponse llmResponse) {
		String messageId = llmResponse.getMessageId();
		Integer promptTokens = llmResponse.getNumberOfTokensInPrompt();
		Integer responseTokens = llmResponse.getNumberOfTokensInResponse();

		Map<String, Object> responsesMap = new HashMap<>();
		responsesMap.put("id", messageId);
		responsesMap.put("model", engineId);
		responsesMap.put("object", "response");

		long unixTimestamp = Instant.now().getEpochSecond();
		responsesMap.put("created_at", unixTimestamp);

		Map<String, Object> usage = new HashMap<>();
		if (promptTokens != null) {
			usage.put("input_tokens", promptTokens);
		}
		if (responseTokens != null) {
			usage.put("output_tokens", responseTokens);
		}
		if (promptTokens != null && responseTokens != null) {
			usage.put("total_tokens", promptTokens + responseTokens);
		}
		responsesMap.put("usage", usage);

		List<Map<String, Object>> output = new ArrayList<>();

		if (AskModelEngineResponse.TOOL.equals(llmResponse.getMessageType())) {
			AskToolModelEngineResponse toolResponse = (AskToolModelEngineResponse) llmResponse;
			List<ToolResponse> tools = toolResponse.getTools();

			for (ToolResponse t : tools) {
				Map<String, Object> functionCall = new HashMap<>();
				functionCall.put("type", "function_call");
				// id is the output item id; call_id is the tool call correlation id
				functionCall.put("id", t.getId());
				functionCall.put("call_id", t.getId());
				functionCall.put("name", t.getName());
				functionCall.put("arguments", GSON.toJson(t.getArguments()));
				functionCall.put("status", "completed");
				output.add(functionCall);
			}

			responsesMap.put("status", "completed");
		} else {
			@SuppressWarnings("unchecked")
			List<MessagePart> parts = llmResponse.getParts();
			boolean hasImageParts = parts.stream().anyMatch(p -> p instanceof MediaMessagePart);

			if (hasImageParts) {
				for (MessagePart part : parts) {
					if (!(part instanceof MediaMessagePart)) {
						continue;
					}
					MediaMessagePart mediaPart = (MediaMessagePart) part;
					if (mediaPart.getMediaInfo() == null) {
						continue;
					}
					String b64 = mediaPart.getMediaInfo().getBase64Data();
					String url = mediaPart.getMediaInfo().getSourceUrl();
					Map<String, Object> imgOutput = new HashMap<>();
					imgOutput.put("type", "image_generation_call");
					imgOutput.put("id", "img_" + UUID.randomUUID().toString());
					imgOutput.put("status", "completed");
					if (b64 != null && !b64.isEmpty()) {
						imgOutput.put("result", b64);
					} else if (url != null && !url.isEmpty()) {
						imgOutput.put("result", url);
					}
					output.add(imgOutput);
				}
			} else {
				String response = llmResponse.getStringResponse();

				Map<String, Object> textPart = new HashMap<>();
				textPart.put("type", "output_text");
				textPart.put("text", response);

				List<Map<String, Object>> content = new ArrayList<>();
				content.add(textPart);

				Map<String, Object> messageItem = new HashMap<>();
				messageItem.put("type", "message");
				messageItem.put("id", "msg_" + UUID.randomUUID().toString());
				messageItem.put("status", "completed");
				messageItem.put("role", "assistant");
				messageItem.put("content", content);
				output.add(messageItem);
			}

			responsesMap.put("status", "completed");
		}

		responsesMap.put("output", output);

		if (llmResponse.getThinking() != null && !llmResponse.getThinking().isEmpty()) {
			Map<String, Object> reasoning = new HashMap<>();
			reasoning.put("type", "reasoning");
			reasoning.put("id", "rs_" + UUID.randomUUID().toString());
			List<Map<String, Object>> summaries = new ArrayList<>();
			Map<String, Object> summary = new HashMap<>();
			summary.put("type", "summary_text");
			summary.put("text", llmResponse.getThinking());
			summaries.add(summary);
			reasoning.put("summary", summaries);
			output.add(reasoning);
		}

		return responsesMap;
	}

	/**
	 * Serialize a value the same way every event in this class does, for the stream
	 * object building tool argument deltas.
	 *
	 * @param value the value to serialize
	 * @return the json text
	 */
	static String toJson(Object value) {
		return GSON.toJson(value);
	}

	/**
	 * Drain an already-dispatched model job and write the whole Responses API SSE
	 * conversation - the {@code response.created} / {@code response.in_progress}
	 * handshake, the message / function_call / image items with their deltas, and
	 * the terminal {@code response.completed} carrying usage - into {@code writer},
	 * blocking until the model is done.
	 *
	 * <p>
	 * This is the driver for a caller holding an open http response. A caller that
	 * cannot hold the response open - the reactor serving python - drives the same
	 * {@link OpenAIResponsesStream} itself, one drain per request, and gets
	 * identical bytes.
	 *
	 * <p>
	 * The job is cleared and removed before returning, so callers only own the
	 * writer. {@link IOException} is left to the caller: an http caller reads it as
	 * a possible client disconnect, an in-process caller as a real failure.
	 *
	 * @param engineId          the model engine id, echoed as the response's
	 *                          "model"
	 * @param responseId        the {@code resp_} id carried by every event
	 * @param creationTimestamp the epoch second stamped on the response object
	 * @param jobId             the job id returned by
	 *                          {@code ModelPixelInvoker.startAsyncModelRequest}
	 * @param writer            the sink for the SSE conversation
	 * @throws IOException if writing to {@code writer} fails
	 */
	public static void streamJobToWriter(String engineId, String responseId, long creationTimestamp, String jobId,
			Writer writer) throws IOException {
		OpenAIResponsesStream stream = new OpenAIResponsesStream(engineId, responseId, creationTimestamp, jobId);
		try {
			while (!stream.drain(writer)) {
				try {
					Thread.sleep(ModelPixelInvoker.STREAM_POLL_INTERVAL_MS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		} finally {
			stream.close();
		}
	}

	private OpenAIResponsesHelper() {
	}
}