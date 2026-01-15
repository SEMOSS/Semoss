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
package prerna.engine.impl.model;

import static prerna.engine.impl.model.ModelEngineSharedUtils.asString;
import static prerna.engine.impl.model.ModelEngineSharedUtils.firstDouble;
import static prerna.engine.impl.model.ModelEngineSharedUtils.firstLong;
import static prerna.engine.impl.model.ModelEngineSharedUtils.firstNonBlank;
import static prerna.engine.impl.model.ModelEngineSharedUtils.normalizeToolArgs;
import static prerna.engine.impl.model.ModelEngineSharedUtils.parseBoolean;
import static prerna.engine.impl.model.ModelEngineSharedUtils.parseLong;
import static prerna.engine.impl.model.ModelEngineSharedUtils.stackTraceToString;
import static prerna.engine.impl.model.ModelEngineSharedUtils.stripSchemaTitles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.openai.azure.AzureOpenAIServiceVersion;
import com.openai.azure.AzureUrlPathMode;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonValue;
import com.openai.core.http.StreamResponse;
import com.openai.credential.BearerTokenCredential;
import com.openai.helpers.ChatCompletionAccumulator;
import com.openai.helpers.ResponseAccumulator;
import com.openai.models.ChatModel;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionChunk;
import com.openai.models.chat.completions.ChatCompletionContentPart;
import com.openai.models.chat.completions.ChatCompletionContentPartImage;
import com.openai.models.chat.completions.ChatCompletionContentPartImage.ImageUrl;
import com.openai.models.chat.completions.ChatCompletionContentPartText;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionFunctionTool;
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessage;
import com.openai.models.chat.completions.ChatCompletionMessageFunctionToolCall;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionNamedToolChoice;
import com.openai.models.chat.completions.ChatCompletionStreamOptions;
import com.openai.models.chat.completions.ChatCompletionSystemMessageParam;
import com.openai.models.chat.completions.ChatCompletionTool;
import com.openai.models.chat.completions.ChatCompletionToolChoiceOption;
import com.openai.models.chat.completions.ChatCompletionToolMessageParam;
import com.openai.models.chat.completions.ChatCompletionUserMessageParam;
import com.openai.models.completions.CompletionUsage;
import com.openai.models.responses.EasyInputMessage;
import com.openai.models.responses.FunctionTool;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCreateParams;
import com.openai.models.responses.ResponseFunctionToolCall;
import com.openai.models.responses.ResponseInputContent;
import com.openai.models.responses.ResponseInputImage;
import com.openai.models.responses.ResponseInputItem;
import com.openai.models.responses.ResponseInputText;
import com.openai.models.responses.ResponseOutputItem;
import com.openai.models.responses.ResponseStreamEvent;
import com.openai.models.responses.Tool;
import com.openai.models.responses.ToolChoiceFunction;
import com.openai.models.responses.ToolChoiceOptions;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.om.Insight;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.util.Constants;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;

/**
 * In-process OpenAI engine using the official OpenAI Java SDK (`com.openai:*`).
 *
 * Supports:
 * - OpenAI API (default)
 * - Azure OpenAI (via `azureServiceVersion` and `azureUrlPathMode`)
 * - OpenAI-compatible servers (e/TGI/Ollama/etc) via `BASE_URL` / `ENDPOINT`
 *
 * This engine consumes SEMOSS `message_json` generated by
 * `MessageUtils.toJsonArrayWithImageData(...)` and converts it to either:
 * - Chat Completions (`CHAT_TYPE=chat-completion`, default)
 * - Responses (`CHAT_TYPE=responses`)
 */
public class OpenAiJavaEngine extends AbstractJavaModelEngine {

	private static final Logger classLogger = LogManager.getLogger(OpenAiJavaEngine.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String SMSS_KEY_PROVIDER = "PROVIDER";
	private static final String SMSS_KEY_CHAT_TYPE = "CHAT_TYPE";
	private static final String SMSS_KEY_BASE_URL = "BASE_URL";
	private static final String SMSS_KEY_ENDPOINT = "ENDPOINT";
	private static final String SMSS_KEY_AUTH_TOKEN = "AUTH_TOKEN";

	private static final String SMSS_KEY_AZURE_URL_PATH_MODE = "AZURE_URL_PATH_MODE";
	private static final String SMSS_KEY_AZURE_SERVICE_VERSION = "AZURE_SERVICE_VERSION";
	private static final String SMSS_KEY_AZURE_API_VERSION = "AZURE_API_VERSION";
	private static final String SMSS_KEY_API_VERSION = "API_VERSION";

	private OpenAIClient client;
	private String modelName;
	private String chatType;
	private Long defaultMaxTokens;

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.OPEN_AI;
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.modelName = smssProp.getProperty(Constants.MODEL);
		if (this.modelName == null || this.modelName.trim().isEmpty()) {
			throw new IllegalArgumentException("Missing required SMSS key: " + Constants.MODEL);
		}

		this.chatType = smssProp.getProperty(SMSS_KEY_CHAT_TYPE, "chat-completion").trim().toLowerCase();
		this.defaultMaxTokens = parseLong(smssProp.getProperty(Constants.MAX_TOKENS));

		String provider = smssProp.getProperty(SMSS_KEY_PROVIDER);
		String baseUrl = firstNonBlank(smssProp.getProperty(SMSS_KEY_BASE_URL), smssProp.getProperty(SMSS_KEY_ENDPOINT));
		boolean isAzure = isAzure(provider, baseUrl);

		OpenAIOkHttpClient.Builder builder = OpenAIOkHttpClient.builder();

		if (baseUrl != null && !baseUrl.isBlank()) {
			builder.baseUrl(baseUrl.trim());
		}

		if (isAzure) {
			AzureUrlPathMode urlPathMode = parseAzureUrlPathMode(smssProp.getProperty(SMSS_KEY_AZURE_URL_PATH_MODE));
			if (urlPathMode != null) {
				builder.azureUrlPathMode(urlPathMode);
			}

			String serviceVersion = firstNonBlank(smssProp.getProperty(SMSS_KEY_AZURE_SERVICE_VERSION),
					smssProp.getProperty(SMSS_KEY_AZURE_API_VERSION), smssProp.getProperty(SMSS_KEY_API_VERSION));
			if (serviceVersion != null && !serviceVersion.isBlank()) {
				builder.azureServiceVersion(AzureOpenAIServiceVersion.fromString(serviceVersion.trim()));
			}
		}

		String apiKey = firstNonBlank(smssProp.getProperty(OPEN_AI_KEY), smssProp.getProperty(Constants.API_KEY),
				smssProp.getProperty("API_KEY"), smssProp.getProperty("AZURE_OPENAI_KEY"));
		String authToken = firstNonBlank(smssProp.getProperty(SMSS_KEY_AUTH_TOKEN), smssProp.getProperty("BEARER_TOKEN"));

		if (apiKey != null && !apiKey.isBlank()) {
			builder.apiKey(apiKey.trim());
		} else if (authToken != null && !authToken.isBlank()) {
			builder.credential(BearerTokenCredential.create(authToken.trim()));
		} else {
			// Some OpenAI-compatible servers do not require auth. The SDK requires a credential;
			// provide a dummy key (many servers ignore it).
			builder.apiKey("DUMMY");
		}

		this.client = builder.build();
	}

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			String roomId, Map<String, Object> parameters) {
		try {
			if (this.client == null) {
				throw new IllegalStateException("OpenAI client is not initialized.");
			}

			boolean stream = parseBoolean(parameters != null ? parameters.get("stream") : null, true);
			if (parameters != null && parameters.containsKey("streaming")) {
				stream = parseBoolean(parameters.get("streaming"), stream);
			}

			String messageJson = parameters != null ? asString(parameters.get("message_json")) : null;
			if (isResponsesChatType(this.chatType)) {
				return askWithResponses(messageJson, question, context, insight.getInsightId(), parameters, stream);
			}
			return askWithChatCompletions(messageJson, question, context, insight.getInsightId(), parameters, stream);
		} catch (Throwable t) {
			classLogger.error(Constants.STACKTRACE, t);
			return new AskErrorModelEngineResponse(t.getMessage(), t.getClass().getSimpleName(), 0, "openai",
					this.modelName, stackTraceToString(t));
		}
	}

	private AskModelEngineResponse askWithChatCompletions(String messageJson, String question, String context,
			String insightId, Map<String, Object> parameters, boolean stream) {
		ChatCompletionCreateParams request = buildChatCompletionRequest(messageJson, question, context, parameters, stream);

		ChatCompletion response;
		String streamedText = null;
		if (stream) {
			StreamingChatResult r = streamChatCompletion(request, insightId);
			response = r.finalCompletion;
			streamedText = r.text;
		} else {
			response = this.client.chat().completions().create(request);
		}

		CompletionUsage usage = response != null ? response.usage().orElse(null) : null;
		int promptTokens = usage != null ? (int) usage.promptTokens() : 0;
		int responseTokens = usage != null ? (int) usage.completionTokens() : 0;

		List<Map<String, Object>> toolCalls = extractChatCompletionToolCalls(response);
		if (!toolCalls.isEmpty()) {
			return new AskToolModelEngineResponse(toolCalls, promptTokens, responseTokens);
		}

		String text = streamedText != null ? streamedText : extractChatCompletionText(response);
		return new AskStringModelEngineResponse(text, promptTokens, responseTokens);
	}

	private AskModelEngineResponse askWithResponses(String messageJson, String question, String context, String insightId,
			Map<String, Object> parameters, boolean stream) {
		ResponseCreateParams request = buildResponsesRequest(messageJson, question, context, parameters);

		Response response;
		String streamedText = null;
		if (stream) {
			StreamingResponseResult r = streamResponses(request, insightId);
			response = r.finalResponse;
			streamedText = r.text;
		} else {
			response = this.client.responses().create(request);
		}

		long promptTokens = response != null ? response.usage().map(u -> u.inputTokens()).orElse(0L) : 0L;
		long responseTokens = response != null ? response.usage().map(u -> u.outputTokens()).orElse(0L) : 0L;

		List<Map<String, Object>> toolCalls = extractResponseToolCalls(response);
		if (!toolCalls.isEmpty()) {
			return new AskToolModelEngineResponse(toolCalls, (int) promptTokens, (int) responseTokens);
		}

		String text = streamedText != null ? streamedText : extractResponseText(response);
		AskStringModelEngineResponse out = new AskStringModelEngineResponse(text, (int) promptTokens, (int) responseTokens);

		String thinking = extractResponseThinking(response);
		if (thinking != null && !thinking.isBlank()) {
			out.setThinking(thinking);
		}

		return out;
	}

	private ChatCompletionCreateParams buildChatCompletionRequest(String messageJson, String question, String context,
			Map<String, Object> parameters, boolean stream) {
		ChatCompletionCreateParams.Builder b = ChatCompletionCreateParams.builder().model(ChatModel.of(this.modelName));

		if (stream) {
			b.streamOptions(ChatCompletionStreamOptions.builder().includeUsage(true).build());
		}

		Long maxCompletionTokens = firstLong(parameters, "max_new_tokens", "max_completion_tokens");
		Long maxTokens = firstLong(parameters, "max_tokens");

		if (maxCompletionTokens == null && maxTokens == null) {
			maxCompletionTokens = this.defaultMaxTokens;
		}

		if (maxCompletionTokens != null) {
			b.maxCompletionTokens(maxCompletionTokens);
		} else if (maxTokens != null) {
			b.maxTokens(maxTokens);
		}

		Double temperature = firstDouble(parameters, "temperature");
		if (temperature != null) {
			b.temperature(temperature);
		}

		Double topP = firstDouble(parameters, "top_p");
		if (topP != null) {
			b.topP(topP);
		}

		Object stopObj = parameters != null ? parameters.get("stop_sequences") : null;
		if (stopObj instanceof List<?>) {
			List<String> stops = toStringList((List<?>) stopObj);
			if (!stops.isEmpty()) {
				b.stop(ChatCompletionCreateParams.Stop.ofStrings(stops));
			}
		}

		List<ChatCompletionTool> tools = convertMcpToolsToChatCompletionTools(parameters != null ? parameters.get("tools") : null);
		for (ChatCompletionTool t : tools) {
			b.addTool(t);
		}

		ChatCompletionToolChoiceOption toolChoice = convertChatCompletionToolChoice(parameters != null ? parameters.get("tool_choice") : null);
		if (toolChoice != null) {
			b.toolChoice(toolChoice);
		}

		if (context != null && !context.isBlank()) {
			b.addMessage(ChatCompletionSystemMessageParam.builder().content(context).build());
		}

		List<AbstractMessage> semossMessages = extractSemossMessages(parameters);
		if (semossMessages != null && !semossMessages.isEmpty()) {
			for (ChatCompletionMessageParam m : buildChatCompletionMessagesFromSemossMessages(semossMessages)) {
				b.addMessage(m);
			}
		} else if (messageJson != null && !messageJson.isBlank()) {
			for (ChatCompletionMessageParam m : buildChatCompletionMessagesFromMessageJson(messageJson)) {
				b.addMessage(m);
			}
		} else if (question != null && !question.isBlank()) {
			b.addMessage(ChatCompletionUserMessageParam.builder().content(question).build());
		} else {
			throw new IllegalArgumentException("Missing `message_json` (and no fallback `question` provided).");
		}

		return b.build();
	}

	private ResponseCreateParams buildResponsesRequest(String messageJson, String question, String context,
			Map<String, Object> parameters) {
		ResponseCreateParams.Builder b = ResponseCreateParams.builder().model(this.modelName);

		Long maxOutputTokens = firstLong(parameters, "max_new_tokens", "max_completion_tokens", "max_tokens");
		if (maxOutputTokens == null) {
			maxOutputTokens = this.defaultMaxTokens;
		}
		if (maxOutputTokens != null) {
			b.maxOutputTokens(maxOutputTokens);
		}

		Double temperature = firstDouble(parameters, "temperature");
		if (temperature != null) {
			b.temperature(temperature);
		}

		Double topP = firstDouble(parameters, "top_p");
		if (topP != null) {
			b.topP(topP);
		}

		List<Tool> tools = convertMcpToolsToResponseTools(parameters != null ? parameters.get("tools") : null);
		if (!tools.isEmpty()) {
			b.tools(tools);
		}

		ResponseCreateParams.ToolChoice toolChoice = convertResponsesToolChoice(parameters != null ? parameters.get("tool_choice") : null);
		if (toolChoice != null) {
			b.toolChoice(toolChoice);
		}

		if (context != null && !context.isBlank()) {
			b.instructions(context);
		}

		List<AbstractMessage> semossMessages = extractSemossMessages(parameters);
		if (semossMessages != null && !semossMessages.isEmpty()) {
			b.inputOfResponse(buildResponseInputItemsFromSemossMessages(semossMessages));
		} else if (messageJson != null && !messageJson.isBlank()) {
			b.inputOfResponse(buildResponseInputItemsFromMessageJson(messageJson));
		} else if (question != null && !question.isBlank()) {
			b.input(question);
		} else {
			throw new IllegalArgumentException("Missing `message_json` (and no fallback `question` provided).");
		}

		return b.build();
	}

	private static final class StreamingChatResult {
		private final ChatCompletion finalCompletion;
		private final String text;

		private StreamingChatResult(ChatCompletion finalCompletion, String text) {
			this.finalCompletion = finalCompletion;
			this.text = text;
		}
	}

	private StreamingChatResult streamChatCompletion(ChatCompletionCreateParams request, String insightId) {
		StringBuilder streamedText = new StringBuilder();
		ChatCompletionAccumulator accumulator = ChatCompletionAccumulator.create();

		try (StreamResponse<ChatCompletionChunk> stream = this.client.chat().completions().createStreaming(request)) {
			java.util.Iterator<ChatCompletionChunk> it = stream.stream().iterator();
			while (it.hasNext()) {
				ChatCompletionChunk chunk = it.next();
				accumulator.accumulate(chunk);
				for (ChatCompletionChunk.Choice choice : chunk.choices()) {
					String delta = choice.delta().content().orElse(null);
					if (delta != null && !delta.isEmpty()) {
						streamedText.append(delta);
						PixelJobManager.getManager().addPartialOut(insightId, delta);
					}
				}
			}
		}

		return new StreamingChatResult(accumulator.chatCompletion(), streamedText.toString());
	}

	private static final class StreamingResponseResult {
		private final Response finalResponse;
		private final String text;

		private StreamingResponseResult(Response finalResponse, String text) {
			this.finalResponse = finalResponse;
			this.text = text;
		}
	}

	private StreamingResponseResult streamResponses(ResponseCreateParams request, String insightId) {
		StringBuilder streamedText = new StringBuilder();
		ResponseAccumulator accumulator = ResponseAccumulator.create();

		try (StreamResponse<ResponseStreamEvent> stream = this.client.responses().createStreaming(request)) {
			java.util.Iterator<ResponseStreamEvent> it = stream.stream().iterator();
			while (it.hasNext()) {
				ResponseStreamEvent event = it.next();
				accumulator.accumulate(event);

				var textDelta = event.outputTextDelta().orElse(null);
				if (textDelta != null) {
					String delta = textDelta.delta();
					if (delta != null && !delta.isEmpty()) {
						streamedText.append(delta);
						PixelJobManager.getManager().addPartialOut(insightId, delta);
					}
				}
			}
		}

		return new StreamingResponseResult(accumulator.response(), streamedText.toString());
	}

	private List<ChatCompletionMessageParam> buildChatCompletionMessagesFromMessageJson(String messageJson) {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> msgs = GSON.fromJson(messageJson, List.class);
		if (msgs == null) {
			return List.of();
		}

		List<ChatCompletionMessageParam> out = new ArrayList<>();

		for (Map<String, Object> message : msgs) {
			String type = asString(message.get("type"));
			if (type == null) {
				continue;
			}

			if ("INPUT_TEXT".equals(type) || "INPUT_MEDIA".equals(type)) {
				String text = firstNonBlank(asString(message.get("inputPrompt")), asString(message.get("inputUIPrompt")));
				List<ChatCompletionContentPart> parts = new ArrayList<>();
				if (text != null && !text.isBlank()) {
					parts.add(ChatCompletionContentPart.ofText(ChatCompletionContentPartText.builder().text(text).build()));
				}
				parts.addAll(buildChatCompletionImageParts(message.get("mediaInputs")));

				ChatCompletionUserMessageParam.Builder user = ChatCompletionUserMessageParam.builder();
				if (parts.isEmpty()) {
					user.content("");
				} else if (parts.size() == 1 && parts.get(0).isText()) {
					user.content(parts.get(0).asText().text());
				} else {
					user.contentOfArrayOfContentParts(parts);
				}
				out.add(ChatCompletionMessageParam.ofUser(user.build()));
				continue;
			}

			if ("RESPONSE_TEXT".equals(type)) {
				String text = asString(message.get("content"));
				if (text == null) {
					text = "";
				}
				out.add(ChatCompletionMessageParam.ofAssistant(ChatCompletionAssistantMessageParam.builder().content(text).build()));
				continue;
			}

			if ("RESPONSE_TOOL".equals(type)) {
				Object toolResponsesObj = message.get("tool_responses");
				if (toolResponsesObj instanceof List<?>) {
					@SuppressWarnings("unchecked")
					List<Map<String, Object>> toolResponses = (List<Map<String, Object>>) toolResponsesObj;
					List<ChatCompletionMessageToolCall> calls = new ArrayList<>();
					for (int i = 0; i < toolResponses.size(); i++) {
						Map<String, Object> tool = toolResponses.get(i);
						String id = firstNonBlank(asString(tool.get("id")), String.valueOf(i));
						String name = asString(tool.get("name"));
						if (name == null || name.isBlank()) {
							continue;
						}
						Map<String, Object> args = normalizeToolArgs(tool.get("arguments"));
						String argsJson = GSON.toJson(args);

						ChatCompletionMessageFunctionToolCall.Function fn = ChatCompletionMessageFunctionToolCall.Function
								.builder().name(name).arguments(argsJson).build();
						ChatCompletionMessageFunctionToolCall call = ChatCompletionMessageFunctionToolCall.builder().id(id)
								.function(fn).build();
						calls.add(ChatCompletionMessageToolCall.ofFunction(call));
					}

					if (!calls.isEmpty()) {
						out.add(ChatCompletionMessageParam.ofAssistant(ChatCompletionAssistantMessageParam.builder().toolCalls(calls).build()));
					}
				}
				continue;
			}

			if ("INPUT_TOOL_EXEC".equals(type)) {
				String toolCallId = asString(message.get("tool_call_id"));
				if (toolCallId == null || toolCallId.isBlank()) {
					continue;
				}
				String result = firstNonBlank(asString(message.get("inputUIPrompt")), asString(message.get("content")), "");
				out.add(ChatCompletionMessageParam.ofTool(ChatCompletionToolMessageParam.builder().toolCallId(toolCallId)
								.content(ChatCompletionToolMessageParam.Content.ofText(result)).build()));
				continue;
			}
		}

		return out;
	}

	@SuppressWarnings("unchecked")
	private static List<AbstractMessage> extractSemossMessages(Map<String, Object> parameters) {
		if (parameters == null) {
			return null;
		}
		Object o = parameters.get(AbstractModelEngine.SEMOSS_MESSAGES_PARAM);
		if (!(o instanceof List<?>)) {
			return null;
		}
		List<?> raw = (List<?>) o;
		for (Object item : raw) {
			if (item != null && !(item instanceof AbstractMessage)) {
				return null;
			}
		}
		return (List<AbstractMessage>) raw;
	}

	private List<ChatCompletionMessageParam> buildChatCompletionMessagesFromSemossMessages(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return List.of();
		}
		List<ChatCompletionMessageParam> out = new ArrayList<>();
		for (AbstractMessage m : msgs) {
			if (m == null) {
				continue;
			}
			MessageType type = m.getMessageType();
			if (type == null) {
				continue;
			}

			if (type == MessageType.INPUT_TEXT || type == MessageType.INPUT_MEDIA) {
				InputMessage im = (InputMessage) m;
				String text = firstNonBlank(im.getInputPrompt(), im.getInputUIPrompt());
				List<ChatCompletionContentPart> parts = new ArrayList<>();
				if (text != null && !text.isBlank()) {
					parts.add(ChatCompletionContentPart.ofText(ChatCompletionContentPartText.builder().text(text).build()));
				}
				parts.addAll(buildChatCompletionImagePartsFromMedia(im.hasMediaInputs() ? im.getMediaInfos() : List.of()));

				ChatCompletionUserMessageParam.Builder user = ChatCompletionUserMessageParam.builder();
				if (parts.isEmpty()) {
					user.content("");
				} else if (parts.size() == 1 && parts.get(0).isText()) {
					user.content(parts.get(0).asText().text());
				} else {
					user.contentOfArrayOfContentParts(parts);
				}
				out.add(ChatCompletionMessageParam.ofUser(user.build()));
				continue;
			}

			if (type == MessageType.RESPONSE_TEXT) {
				ResponseMessage rm = (ResponseMessage) m;
				String text = rm.getContent();
				out.add(ChatCompletionMessageParam.ofAssistant(
						ChatCompletionAssistantMessageParam.builder().content(text != null ? text : "").build()));
				continue;
			}

			if (type == MessageType.RESPONSE_TOOL) {
				ResponseMessage rm = (ResponseMessage) m;
				List<Map<String, Object>> toolResponses = rm.getToolResponses();
				List<ChatCompletionMessageToolCall> calls = new ArrayList<>();
				for (int i = 0; i < toolResponses.size(); i++) {
					Map<String, Object> tool = toolResponses.get(i);
					String id = firstNonBlank(asString(tool.get("id")), String.valueOf(i));
					String name = asString(tool.get("name"));
					if (name == null || name.isBlank()) {
						continue;
					}
					Map<String, Object> args = normalizeToolArgs(tool.get("arguments"));
					String argsJson = GSON.toJson(args);

					ChatCompletionMessageFunctionToolCall.Function fn = ChatCompletionMessageFunctionToolCall.Function.builder()
							.name(name).arguments(argsJson).build();
					ChatCompletionMessageFunctionToolCall call = ChatCompletionMessageFunctionToolCall.builder().id(id)
							.function(fn).build();
					calls.add(ChatCompletionMessageToolCall.ofFunction(call));
				}
				if (!calls.isEmpty()) {
					out.add(ChatCompletionMessageParam.ofAssistant(
							ChatCompletionAssistantMessageParam.builder().toolCalls(calls).build()));
				}
				continue;
			}

			if (type == MessageType.INPUT_TOOL_EXEC) {
				InputMessage im = (InputMessage) m;
				String toolCallId = im.getToolCallId();
				if (toolCallId == null || toolCallId.isBlank()) {
					continue;
				}
				String result = firstNonBlank(im.getInputPrompt(), im.getInputUIPrompt(), "");
				out.add(ChatCompletionMessageParam.ofTool(ChatCompletionToolMessageParam.builder().toolCallId(toolCallId)
						.content(ChatCompletionToolMessageParam.Content.ofText(result)).build()));
				continue;
			}
		}
		return out;
	}

	private List<ResponseInputItem> buildResponseInputItemsFromMessageJson(String messageJson) {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> msgs = GSON.fromJson(messageJson, List.class);
		if (msgs == null) {
			return List.of();
		}

		List<ResponseInputItem> out = new ArrayList<>();

		for (Map<String, Object> message : msgs) {
			String type = asString(message.get("type"));
			if (type == null) {
				continue;
			}

			if ("INPUT_TEXT".equals(type) || "INPUT_MEDIA".equals(type)) {
				String text = firstNonBlank(asString(message.get("inputPrompt")), asString(message.get("inputUIPrompt")));
				List<ResponseInputContent> contents = new ArrayList<>();
				if (text != null && !text.isBlank()) {
					contents.add(ResponseInputContent.ofInputText(ResponseInputText.builder().text(text).build()));
				}
				contents.addAll(buildResponseInputImageContents(message.get("mediaInputs")));

				EasyInputMessage.Builder user = EasyInputMessage.builder().role(EasyInputMessage.Role.USER);
				if (contents.isEmpty()) {
					user.content("");
				} else if (contents.size() == 1 && contents.get(0).isInputText()) {
					user.content(contents.get(0).asInputText().text());
				} else {
					user.contentOfResponseInputMessageContentList(contents);
				}

				out.add(ResponseInputItem.ofEasyInputMessage(user.build()));
				continue;
			}

			if ("RESPONSE_TEXT".equals(type)) {
				String text = asString(message.get("content"));
				if (text == null) {
					text = "";
				}
				out.add(ResponseInputItem.ofEasyInputMessage(
						EasyInputMessage.builder().role(EasyInputMessage.Role.ASSISTANT).content(text).build()));
				continue;
			}

			if ("RESPONSE_TOOL".equals(type)) {
				Object toolResponsesObj = message.get("tool_responses");
				if (toolResponsesObj instanceof List<?>) {
					@SuppressWarnings("unchecked")
					List<Map<String, Object>> toolResponses = (List<Map<String, Object>>) toolResponsesObj;
					for (int i = 0; i < toolResponses.size(); i++) {
						Map<String, Object> tool = toolResponses.get(i);
						String callId = firstNonBlank(asString(tool.get("id")), String.valueOf(i));
						String name = asString(tool.get("name"));
						if (name == null || name.isBlank()) {
							continue;
						}
						Map<String, Object> args = normalizeToolArgs(tool.get("arguments"));
						String argsJson = GSON.toJson(args);

						ResponseFunctionToolCall call = ResponseFunctionToolCall.builder().callId(callId).name(name)
								.arguments(argsJson).build();
						out.add(ResponseInputItem.ofFunctionCall(call));
					}
				}
				continue;
			}

			if ("INPUT_TOOL_EXEC".equals(type)) {
				String toolCallId = asString(message.get("tool_call_id"));
				if (toolCallId == null || toolCallId.isBlank()) {
					continue;
				}
				String result = firstNonBlank(asString(message.get("inputUIPrompt")), asString(message.get("content")), "");
				out.add(ResponseInputItem.ofFunctionCallOutput(
						ResponseInputItem.FunctionCallOutput.builder().callId(toolCallId)
								.output(ResponseInputItem.FunctionCallOutput.Output.ofString(result)).build()));
				continue;
			}
		}

		return out;
	}

	private List<ResponseInputItem> buildResponseInputItemsFromSemossMessages(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return List.of();
		}
		List<ResponseInputItem> out = new ArrayList<>();
		for (AbstractMessage m : msgs) {
			if (m == null) {
				continue;
			}
			MessageType type = m.getMessageType();
			if (type == null) {
				continue;
			}

			if (type == MessageType.INPUT_TEXT || type == MessageType.INPUT_MEDIA) {
				InputMessage im = (InputMessage) m;
				String text = firstNonBlank(im.getInputPrompt(), im.getInputUIPrompt());
				List<ResponseInputContent> contents = new ArrayList<>();
				if (text != null && !text.isBlank()) {
					contents.add(ResponseInputContent.ofInputText(ResponseInputText.builder().text(text).build()));
				}
				contents.addAll(buildResponseInputImageContentsFromMedia(im.hasMediaInputs() ? im.getMediaInfos() : List.of()));

				EasyInputMessage.Builder user = EasyInputMessage.builder().role(EasyInputMessage.Role.USER);
				if (contents.isEmpty()) {
					user.content("");
				} else if (contents.size() == 1 && contents.get(0).isInputText()) {
					user.content(contents.get(0).asInputText().text());
				} else {
					user.contentOfResponseInputMessageContentList(contents);
				}
				out.add(ResponseInputItem.ofEasyInputMessage(user.build()));
				continue;
			}

			if (type == MessageType.RESPONSE_TEXT) {
				ResponseMessage rm = (ResponseMessage) m;
				String text = rm.getContent();
				out.add(ResponseInputItem.ofEasyInputMessage(
						EasyInputMessage.builder().role(EasyInputMessage.Role.ASSISTANT).content(text != null ? text : "")
								.build()));
				continue;
			}

			if (type == MessageType.RESPONSE_TOOL) {
				ResponseMessage rm = (ResponseMessage) m;
				List<Map<String, Object>> toolResponses = rm.getToolResponses();
				for (int i = 0; i < toolResponses.size(); i++) {
					Map<String, Object> tool = toolResponses.get(i);
					String callId = firstNonBlank(asString(tool.get("id")), String.valueOf(i));
					String name = asString(tool.get("name"));
					if (name == null || name.isBlank()) {
						continue;
					}
					Map<String, Object> args = normalizeToolArgs(tool.get("arguments"));
					String argsJson = GSON.toJson(args);

					ResponseFunctionToolCall call = ResponseFunctionToolCall.builder().callId(callId).name(name).arguments(argsJson)
							.build();
					out.add(ResponseInputItem.ofFunctionCall(call));
				}
				continue;
			}

			if (type == MessageType.INPUT_TOOL_EXEC) {
				InputMessage im = (InputMessage) m;
				String toolCallId = im.getToolCallId();
				if (toolCallId == null || toolCallId.isBlank()) {
					continue;
				}
				String result = firstNonBlank(im.getInputPrompt(), im.getInputUIPrompt(), "");
				out.add(ResponseInputItem.ofFunctionCallOutput(ResponseInputItem.FunctionCallOutput.builder().callId(toolCallId)
						.output(ResponseInputItem.FunctionCallOutput.Output.ofString(result)).build()));
				continue;
			}
		}
		return out;
	}

	private List<ChatCompletionContentPart> buildChatCompletionImageParts(Object mediaInputsObj) {
		if (!(mediaInputsObj instanceof List<?>)) {
			return List.of();
		}
		List<ChatCompletionContentPart> parts = new ArrayList<>();
		for (Object o : (List<?>) mediaInputsObj) {
			if (!(o instanceof Map)) {
				continue;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> media = (Map<String, Object>) o;
			String url = firstNonBlank(asString(media.get("sourceUrl")), asDataUri(media));
			if (url == null || url.isBlank()) {
				continue;
			}
			ImageUrl imageUrl = ImageUrl.builder().url(url).build();
			parts.add(ChatCompletionContentPart.ofImageUrl(ChatCompletionContentPartImage.builder().imageUrl(imageUrl).build()));
		}
		return parts;
	}

	private static List<ChatCompletionContentPart> buildChatCompletionImagePartsFromMedia(List<MessageInputMedia> mediaInputs) {
		if (mediaInputs == null || mediaInputs.isEmpty()) {
			return List.of();
		}
		List<ChatCompletionContentPart> parts = new ArrayList<>();
		for (MessageInputMedia media : mediaInputs) {
			if (media == null) {
				continue;
			}
			String url = firstNonBlank(media.getSourceUrl(), media.getFullDataUrl());
			if (url == null || url.isBlank()) {
				continue;
			}
			ImageUrl imageUrl = ImageUrl.builder().url(url).build();
			parts.add(ChatCompletionContentPart.ofImageUrl(ChatCompletionContentPartImage.builder().imageUrl(imageUrl).build()));
		}
		return parts;
	}

	private List<ResponseInputContent> buildResponseInputImageContents(Object mediaInputsObj) {
		if (!(mediaInputsObj instanceof List<?>)) {
			return List.of();
		}
		List<ResponseInputContent> parts = new ArrayList<>();
		for (Object o : (List<?>) mediaInputsObj) {
			if (!(o instanceof Map)) {
				continue;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> media = (Map<String, Object>) o;
			String url = firstNonBlank(asString(media.get("sourceUrl")), asDataUri(media));
			if (url == null || url.isBlank()) {
				continue;
			}
			// `detail` is required by the OpenAI Java SDK for Responses image inputs.
			parts.add(ResponseInputContent.ofInputImage(
					ResponseInputImage.builder().imageUrl(url).detail(ResponseInputImage.Detail.AUTO).build()));
		}
		return parts;
	}

	private static List<ResponseInputContent> buildResponseInputImageContentsFromMedia(List<MessageInputMedia> mediaInputs) {
		if (mediaInputs == null || mediaInputs.isEmpty()) {
			return List.of();
		}
		List<ResponseInputContent> parts = new ArrayList<>();
		for (MessageInputMedia media : mediaInputs) {
			if (media == null) {
				continue;
			}
			String url = firstNonBlank(media.getSourceUrl(), media.getFullDataUrl());
			if (url == null || url.isBlank()) {
				continue;
			}
			parts.add(ResponseInputContent.ofInputImage(
					ResponseInputImage.builder().imageUrl(url).detail(ResponseInputImage.Detail.AUTO).build()));
		}
		return parts;
	}

	private static String asDataUri(Map<String, Object> media) {
		String base64Data = asString(media.get("base64Data"));
		if (base64Data == null || base64Data.isBlank()) {
			return null;
		}
		String mimeType = asString(media.get("mimeType"));
		if (mimeType == null || mimeType.isBlank()) {
			mimeType = "application/octet-stream";
		}
		// Normalize in case upstream provided raw bytes.
		try {
			Base64.getDecoder().decode(base64Data);
			return "data:" + mimeType + ";base64," + base64Data;
		} catch (Exception e) {
			return null;
		}
	}

	private static List<ChatCompletionTool> convertMcpToolsToChatCompletionTools(Object toolsObj) {
		if (!(toolsObj instanceof List<?>)) {
			return List.of();
		}
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> mcpTools = (List<Map<String, Object>>) toolsObj;
		List<ChatCompletionTool> out = new ArrayList<>();
		for (Map<String, Object> tool : mcpTools) {
			String name = asString(tool.get("name"));
			if (name == null || name.isBlank()) {
				continue;
			}
			String description = asString(tool.get("description"));
			Object schemaObj = tool.get("inputSchema");
			FunctionParameters parameters = buildFunctionParameters(schemaObj);

			FunctionDefinition.Builder fn = FunctionDefinition.builder().name(name);
			if (description != null && !description.isBlank()) {
				fn.description(description);
			}
			if (parameters != null) {
				fn.parameters(parameters);
			}

			ChatCompletionFunctionTool fnTool = ChatCompletionFunctionTool.builder().function(fn.build()).build();
			out.add(ChatCompletionTool.ofFunction(fnTool));
		}
		return out;
	}

	private static List<Tool> convertMcpToolsToResponseTools(Object toolsObj) {
		if (!(toolsObj instanceof List<?>)) {
			return List.of();
		}
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> mcpTools = (List<Map<String, Object>>) toolsObj;
		List<Tool> out = new ArrayList<>();
		for (Map<String, Object> tool : mcpTools) {
			String name = asString(tool.get("name"));
			if (name == null || name.isBlank()) {
				continue;
			}
			String description = asString(tool.get("description"));
			Object schemaObj = tool.get("inputSchema");
			FunctionTool.Parameters parameters = buildResponseFunctionParameters(schemaObj);

			FunctionTool.Builder fn = FunctionTool.builder().name(name);
			if (description != null && !description.isBlank()) {
				fn.description(description);
			}
			if (parameters != null) {
				fn.parameters(parameters);
			}
			// Required by the OpenAI Java SDK as of 4.15.0 (Responses function tools).
			fn.strict(false);

			out.add(Tool.ofFunction(fn.build()));
		}
		return out;
	}

	private static FunctionParameters buildFunctionParameters(Object schemaObj) {
		if (!(schemaObj instanceof Map)) {
			return null;
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> schema = (Map<String, Object>) schemaObj;
		FunctionParameters.Builder b = FunctionParameters.builder();
		for (Map.Entry<String, Object> e : schema.entrySet()) {
			if ("title".equals(e.getKey())) {
				continue;
			}
			b.putAdditionalProperty(e.getKey(), JsonValue.from(stripSchemaTitles(e.getValue())));
		}
		return b.build();
	}

	private static FunctionTool.Parameters buildResponseFunctionParameters(Object schemaObj) {
		if (!(schemaObj instanceof Map)) {
			return null;
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> schema = (Map<String, Object>) schemaObj;
		FunctionTool.Parameters.Builder b = FunctionTool.Parameters.builder();
		for (Map.Entry<String, Object> e : schema.entrySet()) {
			if ("title".equals(e.getKey())) {
				continue;
			}
			b.putAdditionalProperty(e.getKey(), JsonValue.from(stripSchemaTitles(e.getValue())));
		}
		return b.build();
	}

	private static ChatCompletionToolChoiceOption convertChatCompletionToolChoice(Object toolChoiceObj) {
		if (!(toolChoiceObj instanceof Map)) {
			return null;
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) toolChoiceObj;
		String type = asString(map.get("type"));
		String name = asString(map.get("name"));
		if (type == null) {
			return null;
		}
		type = type.toLowerCase();

		if ("forced".equals(type) && name != null && !name.isBlank()) {
			ChatCompletionNamedToolChoice choice = ChatCompletionNamedToolChoice.builder()
					.function(ChatCompletionNamedToolChoice.Function.builder().name(name).build()).build();
			return ChatCompletionToolChoiceOption.ofNamedToolChoice(choice);
		}

		if ("required".equals(type)) {
			return ChatCompletionToolChoiceOption.ofAuto(ChatCompletionToolChoiceOption.Auto.REQUIRED);
		}
		if ("auto".equals(type)) {
			return ChatCompletionToolChoiceOption.ofAuto(ChatCompletionToolChoiceOption.Auto.AUTO);
		}
		if ("none".equals(type)) {
			return ChatCompletionToolChoiceOption.ofAuto(ChatCompletionToolChoiceOption.Auto.NONE);
		}

		return null;
	}

	private static ResponseCreateParams.ToolChoice convertResponsesToolChoice(Object toolChoiceObj) {
		if (!(toolChoiceObj instanceof Map)) {
			return null;
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) toolChoiceObj;
		String type = asString(map.get("type"));
		String name = asString(map.get("name"));
		if (type == null) {
			return null;
		}
		type = type.toLowerCase();

		if ("forced".equals(type) && name != null && !name.isBlank()) {
			return ResponseCreateParams.ToolChoice.ofFunction(ToolChoiceFunction.builder().name(name).build());
		}
		if ("required".equals(type)) {
			return ResponseCreateParams.ToolChoice.ofOptions(ToolChoiceOptions.REQUIRED);
		}
		if ("auto".equals(type)) {
			return ResponseCreateParams.ToolChoice.ofOptions(ToolChoiceOptions.AUTO);
		}
		if ("none".equals(type)) {
			return ResponseCreateParams.ToolChoice.ofOptions(ToolChoiceOptions.NONE);
		}
		return null;
	}

	private static List<Map<String, Object>> extractChatCompletionToolCalls(ChatCompletion response) {
		if (response == null || response.choices() == null || response.choices().isEmpty()) {
			return List.of();
		}
		ChatCompletionMessage message = response.choices().get(0).message();
		if (message == null) {
			return List.of();
		}
		List<ChatCompletionMessageToolCall> toolCalls = message.toolCalls().orElse(List.of());
		if (toolCalls == null || toolCalls.isEmpty()) {
			return List.of();
		}

		List<Map<String, Object>> out = new ArrayList<>();
		for (ChatCompletionMessageToolCall call : toolCalls) {
			if (!call.isFunction()) {
				continue;
			}
			ChatCompletionMessageFunctionToolCall fn = call.asFunction();
			Map<String, Object> m = new HashMap<>();
			m.put("id", fn.id());
			m.put("type", "function");
			m.put("name", fn.function().name());
			m.put("arguments", parseJsonToMap(fn.function().arguments()));
			out.add(m);
		}
		return out;
	}

	private static List<Map<String, Object>> extractResponseToolCalls(Response response) {
		if (response == null) {
			return List.of();
		}
		List<Map<String, Object>> out = new ArrayList<>();
		for (ResponseOutputItem item : response.output()) {
			if (item == null || !item.isFunctionCall()) {
				continue;
			}
			ResponseFunctionToolCall call = item.asFunctionCall();
			Map<String, Object> m = new HashMap<>();
			m.put("id", call.callId());
			m.put("type", "function");
			m.put("name", call.name());
			m.put("arguments", parseJsonToMap(call.arguments()));
			out.add(m);
		}
		return out;
	}

	private static String extractChatCompletionText(ChatCompletion response) {
		if (response == null || response.choices() == null || response.choices().isEmpty()) {
			return "";
		}
		ChatCompletionMessage message = response.choices().get(0).message();
		if (message == null) {
			return "";
		}
		return message.content().orElse("");
	}

	private static String extractResponseText(Response response) {
		if (response == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (ResponseOutputItem item : response.output()) {
			if (item == null || !item.isMessage()) {
				continue;
			}
			for (var c : item.asMessage().content()) {
				if (c != null && c.isOutputText()) {
					sb.append(c.asOutputText().text());
				}
			}
		}
		return sb.toString();
	}

	private static String extractResponseThinking(Response response) {
		if (response == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for (ResponseOutputItem item : response.output()) {
			if (item == null || !item.isReasoning()) {
				continue;
			}
			var reasoning = item.asReasoning();
			if (reasoning.summary() != null) {
				for (var s : reasoning.summary()) {
					if (s != null && s.text() != null && !s.text().isBlank()) {
						if (sb.length() > 0) {
							sb.append('\n');
						}
						sb.append(s.text());
					}
				}
			}
		}
		String out = sb.toString();
		return out.isEmpty() ? null : out;
	}

	private static Map<String, Object> parseJsonToMap(String json) {
		if (json == null || json.isBlank()) {
			return new HashMap<>();
		}
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = GSON.fromJson(json, Map.class);
			return map != null ? map : new HashMap<>();
		} catch (Exception e) {
			Map<String, Object> out = new HashMap<>();
			out.put("raw", json);
			return out;
		}
	}

	private static boolean isResponsesChatType(String chatType) {
		return chatType != null && (chatType.equals("responses") || chatType.equals("response"));
	}

	private static boolean isAzure(String provider, String baseUrl) {
		if (provider != null && !provider.isBlank()) {
			String p = provider.trim().toLowerCase();
			if ("azure".equals(p) || "azure-openai".equals(p)) {
				return true;
			}
		}
		if (baseUrl == null) {
			return false;
		}
		String u = baseUrl.toLowerCase();
		return u.contains("cognitiveservices.azure.com") || u.contains("openai.azure.com");
	}

	private static AzureUrlPathMode parseAzureUrlPathMode(String mode) {
		if (mode == null || mode.isBlank()) {
			return null;
		}
		try {
			return AzureUrlPathMode.valueOf(mode.trim().toUpperCase());
		} catch (Exception e) {
			return null;
		}
	}

	private static List<String> toStringList(List<?> values) {
		List<String> out = new ArrayList<>();
		for (Object o : values) {
			if (o == null) {
				continue;
			}
			String s = o.toString();
			if (!s.isBlank()) {
				out.add(s);
			}
		}
		return out;
	}

	@Override
	protected InstructModelEngineResponse instructCall(String task, String context, List<Map<String, Object>> projectData,
			Insight insight, Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Instruct is not yet implemented for OpenAiJavaEngine.");
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Embeddings are not yet implemented for OpenAiJavaEngine.");
	}

	@Override
	protected EmbeddingsModelEngineResponse imageEmbeddingsCall(List<String> imagesToEmbed, Insight insight,
			Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Image embeddings are not yet implemented for OpenAiJavaEngine.");
	}

	@Override
	public void close() throws IOException {
		if (this.client != null) {
			this.client.close();
		}
	}
}
