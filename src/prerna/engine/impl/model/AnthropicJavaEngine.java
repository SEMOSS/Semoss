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
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.backends.Backend;
import com.anthropic.core.JsonValue;
import com.anthropic.core.http.StreamResponse;
import com.anthropic.helpers.MessageAccumulator;
import com.anthropic.models.messages.Base64ImageSource;
import com.anthropic.models.messages.ContentBlock;
import com.anthropic.models.messages.ContentBlockParam;
import com.anthropic.models.messages.ImageBlockParam;
import com.anthropic.models.messages.Message;
import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.MessageParam;
import com.anthropic.models.messages.RawMessageStreamEvent;
import com.anthropic.models.messages.TextBlock;
import com.anthropic.models.messages.TextBlockParam;
import com.anthropic.models.messages.ThinkingBlock;
import com.anthropic.models.messages.ThinkingBlockParam;
import com.anthropic.models.messages.Tool;
import com.anthropic.models.messages.ToolChoice;
import com.anthropic.models.messages.ToolChoiceAny;
import com.anthropic.models.messages.ToolChoiceAuto;
import com.anthropic.models.messages.ToolChoiceNone;
import com.anthropic.models.messages.ToolChoiceTool;
import com.anthropic.models.messages.ToolResultBlockParam;
import com.anthropic.models.messages.ToolUnion;
import com.anthropic.models.messages.ToolUseBlock;
import com.anthropic.models.messages.ToolUseBlockParam;
import com.anthropic.models.messages.WebSearchTool20250305;
import com.anthropic.vertex.backends.VertexBackend;
import com.anthropic.bedrock.backends.BedrockBackend;
import com.google.auth.oauth2.GoogleCredentials;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.regions.Region;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

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
 * In-process Anthropic (Claude) engine using the official Anthropic Java SDK.
 *
 * Important: Anthropic's Java SDK targets Anthropic's API endpoints. It does not
 * call Google Vertex "publisher/anthropic" endpoints. Existing SMSS engines
 * labeled "Anthropic-Vertex" are currently routed through the Python runtime via
 * `genai_client.AnthropicClient(provider='google')`.
 *
 * Expected SMSS keys for this Java engine:
 * - {@link Constants#MODEL} (required)
 * - {@link Constants#API_KEY} (required) OR AUTH_TOKEN (optional alternative)
 * - BASE_URL (optional; defaults to Anthropic)
 * - {@link Constants#MAX_TOKENS} (optional default)
 */
public class AnthropicJavaEngine extends AbstractModelEngine {

	private static final Logger classLogger = LogManager.getLogger(AnthropicJavaEngine.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String SMSS_KEY_BASE_URL = "BASE_URL";
	private static final String SMSS_KEY_AUTH_TOKEN = "AUTH_TOKEN";
	private static final String SMSS_KEY_THINKING = "THINKING";
	private static final String SMSS_KEY_THINKING_BUDGET = "THINKING_BUDGET";
	private static final String SMSS_KEY_PROVIDER = "PROVIDER";
	private static final String SMSS_KEY_PROJECT = "PROJECT";
	private static final String SMSS_KEY_REGION = "REGION";
	private static final String SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS = "SERVICE_ACCOUNT_CREDENTIALS";
	private static final String SMSS_KEY_AWS_REGION = "AWS_REGION";
	private static final String SMSS_KEY_AWS_BEARER_TOKEN_BEDROCK = "AWS_BEARER_TOKEN_BEDROCK";
	private static final String SMSS_KEY_VERTEX_PROJECT_ID = "ANTHROPIC_VERTEX_PROJECT_ID";

	private AnthropicClient client;
	private String modelName;
	private Long defaultMaxTokens;

	@Override
	public ModelTypeEnum getModelType() {
		// Keep VERTEX for catalog parity with existing Claude SMSS files (MODEL_TYPE=VERTEX).
		return ModelTypeEnum.VERTEX;
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.modelName = smssProp.getProperty(Constants.MODEL);
		if (this.modelName == null || this.modelName.trim().isEmpty()) {
			throw new IllegalArgumentException("Missing required SMSS key: " + Constants.MODEL);
		}

		this.defaultMaxTokens = parseLong(smssProp.getProperty(Constants.MAX_TOKENS));

		String provider = smssProp.getProperty(SMSS_KEY_PROVIDER, "anthropic").trim().toLowerCase();
		AnthropicOkHttpClient.Builder builder = AnthropicOkHttpClient.builder();

		Backend backend = resolveBackend(provider, smssProp);
		if (backend != null) {
			builder.backend(backend);
		} else {
			String baseUrl = smssProp.getProperty(SMSS_KEY_BASE_URL);
			if (baseUrl != null && !baseUrl.isBlank()) {
				builder.baseUrl(baseUrl.trim());
			}

			String apiKey = smssProp.getProperty(Constants.API_KEY);
			String authToken = smssProp.getProperty(SMSS_KEY_AUTH_TOKEN);

			if ((apiKey == null || apiKey.isBlank()) && (authToken == null || authToken.isBlank())) {
				throw new IllegalArgumentException(
						"AnthropicJavaEngine requires API_KEY (or AUTH_TOKEN) for direct Anthropic API usage.");
			}

			if (apiKey != null && !apiKey.isBlank()) {
				builder.apiKey(apiKey.trim());
			} else {
				builder.authToken(authToken.trim());
			}
		}

		this.client = builder.build();
	}

	private Backend resolveBackend(String provider, Properties smssProp) {
		if (provider == null || provider.isBlank() || "anthropic".equals(provider) || "direct".equals(provider)) {
			return null;
		}

		if ("google".equals(provider) || "vertex".equals(provider) || "gcp".equals(provider)) {
			return resolveVertexBackend(smssProp);
		}

		if ("bedrock".equals(provider) || "aws".equals(provider)) {
			return resolveBedrockBackend(smssProp);
		}

		throw new IllegalArgumentException("Unsupported PROVIDER for AnthropicJavaEngine: " + provider);
	}

	private Backend resolveVertexBackend(Properties smssProp) {
		String project = firstNonBlank(smssProp.getProperty(SMSS_KEY_PROJECT), smssProp.getProperty(SMSS_KEY_VERTEX_PROJECT_ID));
		String region = smssProp.getProperty(SMSS_KEY_REGION);
		String saJson = smssProp.getProperty(SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS);

		// If nothing explicit is provided, fall back to ADC + env vars (Anthropic SDK behavior).
		if ((project == null || project.isBlank()) && (region == null || region.isBlank())
				&& (saJson == null || saJson.isBlank())) {
			return VertexBackend.fromEnv();
		}

		if (project == null || project.isBlank()) {
			throw new IllegalArgumentException("Vertex backend requires PROJECT (or ANTHROPIC_VERTEX_PROJECT_ID).");
		}
		if (region == null || region.isBlank()) {
			throw new IllegalArgumentException("Vertex backend requires REGION.");
		}
		if (saJson == null || saJson.isBlank()) {
			// Allow ADC even if project/region are set explicitly.
			return VertexBackend.builder().region(region.trim()).project(project.trim()).build();
		}

		try {
			GoogleCredentials creds = GoogleCredentials
					.fromStream(new ByteArrayInputStream(saJson.getBytes(StandardCharsets.UTF_8)))
					.createScoped(List.of("https://www.googleapis.com/auth/cloud-platform"));
			return VertexBackend.builder().googleCredentials(creds).region(region.trim()).project(project.trim()).build();
		} catch (IOException e) {
			throw new IllegalArgumentException("Failed to parse SERVICE_ACCOUNT_CREDENTIALS for Vertex backend.", e);
		}
	}

	private Backend resolveBedrockBackend(Properties smssProp) {
		String awsRegion = smssProp.getProperty(SMSS_KEY_AWS_REGION);
		String awsAccessKey = smssProp.getProperty(AbstractModelEngine.AWS_ACCESS_KEY);
		String awsSecretKey = smssProp.getProperty(AbstractModelEngine.AWS_SECRET_KEY);
		String bearerToken = smssProp.getProperty(SMSS_KEY_AWS_BEARER_TOKEN_BEDROCK);

		BedrockBackend.Builder b = BedrockBackend.builder();

		if (awsRegion != null && !awsRegion.isBlank()) {
			b.region(Region.of(awsRegion.trim()));
		}

		// Bedrock supports either AWS credentials OR an API key/bearer token (but not both).
		if (bearerToken != null && !bearerToken.isBlank()) {
			b.apiKey(bearerToken.trim());
			return b.build();
		}

		if (awsAccessKey != null && !awsAccessKey.isBlank() && awsSecretKey != null && !awsSecretKey.isBlank()) {
			AwsCredentials creds = AwsBasicCredentials.create(awsAccessKey.trim(), awsSecretKey.trim());
			b.awsCredentials(creds);
			return b.build();
		}

		// Fall back to AWS default provider chains.
		return BedrockBackend.fromEnv();
	}

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			String roomId, Map<String, Object> parameters) {
		try {
			if (this.client == null) {
				throw new IllegalStateException("Anthropic client is not initialized.");
			}

			boolean stream = parseBoolean(parameters != null ? parameters.get("stream") : null, true);
			if (parameters != null && parameters.containsKey("streaming")) {
				stream = parseBoolean(parameters.get("streaming"), stream);
			}

			String messageJson = parameters != null ? asString(parameters.get("message_json")) : null;
			List<MessageParam> messages;
			List<AbstractMessage> semossMessages = extractSemossMessages(parameters);
			if (semossMessages != null && !semossMessages.isEmpty()) {
				messages = buildMessagesFromSemossMessages(semossMessages);
			} else if (messageJson != null && !messageJson.isBlank()) {
				messages = buildMessagesFromMessageJson(messageJson);
			} else if (question != null && !question.isBlank()) {
				messages = List.of(MessageParam.builder().role(MessageParam.Role.USER).content(question).build());
			} else {
				throw new IllegalArgumentException("Missing `message_json` (and no fallback `question` provided).");
			}

			MessageCreateParams request = buildRequest(messages, context, parameters);

			Message response;
			String streamedText = null;
			if (stream) {
				StreamingResult r = streamMessage(request, insight.getInsightId());
				response = r.finalMessage;
				streamedText = r.text;
			} else {
				response = this.client.messages().create(request);
			}

			long promptTokens = response.usage().inputTokens();
			long responseTokens = response.usage().outputTokens();

			List<Map<String, Object>> toolCalls = extractToolCalls(response);
			if (!toolCalls.isEmpty()) {
				return new AskToolModelEngineResponse(toolCalls, (int) promptTokens, (int) responseTokens);
			}

			String text = streamedText != null ? streamedText : extractText(response);
			AskStringModelEngineResponse out = new AskStringModelEngineResponse(text, (int) promptTokens,
					(int) responseTokens);

			String thinking = extractThinking(response);
			if (thinking != null && !thinking.isBlank()) {
				out.setThinking(thinking);
			}

			return out;
		} catch (Throwable t) {
			classLogger.error(Constants.STACKTRACE, t);
			return new AskErrorModelEngineResponse(t.getMessage(), t.getClass().getSimpleName(), 0, "anthropic",
					this.modelName, stackTraceToString(t));
		}
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

	private static final class StreamingResult {
		private final Message finalMessage;
		private final String text;

		private StreamingResult(Message finalMessage, String text) {
			this.finalMessage = finalMessage;
			this.text = text;
		}
	}

	private StreamingResult streamMessage(MessageCreateParams request, String insightId) {
		StringBuilder streamedText = new StringBuilder();
		MessageAccumulator accumulator = MessageAccumulator.create();

		try (StreamResponse<RawMessageStreamEvent> stream = this.client.messages().createStreaming(request)) {
			java.util.Iterator<RawMessageStreamEvent> it = stream.stream().iterator();
			while (it.hasNext()) {
				RawMessageStreamEvent event = it.next();
				accumulator.accumulate(event);

				if (event.isContentBlockDelta()) {
					var deltaEvent = event.asContentBlockDelta();
					var delta = deltaEvent.delta();
					if (delta.isText()) {
						String chunk = delta.asText().text();
						if (chunk != null && !chunk.isEmpty()) {
							streamedText.append(chunk);
							PixelJobManager.getManager().addPartialOut(insightId, chunk);
						}
					}
				}
			}
		}

		Message finalMessage = accumulator.message();
		return new StreamingResult(finalMessage, streamedText.toString());
	}

	private MessageCreateParams buildRequest(List<MessageParam> messages, String context, Map<String, Object> parameters) {
		MessageCreateParams.Builder b = MessageCreateParams.builder().model(this.modelName);

		Long maxTokens = firstLong(parameters, "max_new_tokens", "max_completion_tokens", "max_tokens");
		if (maxTokens == null) {
			maxTokens = this.defaultMaxTokens;
		}
		if (maxTokens == null) {
			maxTokens = 1024L;
		}
		b.maxTokens(maxTokens);

		if (context != null && !context.isBlank()) {
			b.system(context);
		}

		Double temperature = firstDouble(parameters, "temperature");
		if (temperature != null) {
			b.temperature(temperature);
		}

		Double topP = firstDouble(parameters, "top_p");
		if (topP != null) {
			b.topP(topP);
		}

		Long topK = firstLong(parameters, "top_k");
		if (topK != null) {
			b.topK(topK);
		}

		Object stopObj = parameters != null ? parameters.get("stop_sequences") : null;
		if (stopObj instanceof List<?>) {
			List<String> stops = new ArrayList<>();
			for (Object o : (List<?>) stopObj) {
				if (o != null) {
					stops.add(o.toString());
				}
			}
			if (!stops.isEmpty()) {
				b.stopSequences(stops);
			}
		}

		applyThinkingConfig(b, parameters);
		applyToolsConfig(b, parameters);

		for (MessageParam m : messages) {
			b.addMessage(m);
		}
		return b.build();
	}

	private void applyThinkingConfig(MessageCreateParams.Builder b, Map<String, Object> parameters) {
		boolean thinking = parseBoolean(parameters != null ? parameters.get("thinking") : null,
				parseBoolean(this.smssProp.getProperty(SMSS_KEY_THINKING), false));
		Long budget = firstLong(parameters, "thinking_budget");
		if (budget == null) {
			budget = parseLong(this.smssProp.getProperty(SMSS_KEY_THINKING_BUDGET));
		}

		if (thinking) {
			if (budget == null) {
				budget = 1024L;
			}
			// "extended thinking"
			b.enabledThinking(budget);
		}
	}

	private void applyToolsConfig(MessageCreateParams.Builder b, Map<String, Object> parameters) {
		if (parameters == null) {
			return;
		}

		Object toolsObj = parameters.get("tools");
		if (toolsObj instanceof List<?>) {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> mcpTools = (List<Map<String, Object>>) toolsObj;
			for (ToolUnion toolUnion : convertMcpTools(mcpTools)) {
				if (toolUnion.isTool()) {
					b.addTool(toolUnion.asTool());
				} else if (toolUnion.isWebSearchTool20250305()) {
					b.addTool(toolUnion.asWebSearchTool20250305());
				}
			}
		}

		Object builtInToolsObj = parameters.get("built_in_tools");
		if (builtInToolsObj instanceof List<?>) {
			for (Object t : (List<?>) builtInToolsObj) {
				if (t == null) {
					continue;
				}
				String name = t.toString().trim().toLowerCase();
				if ("web_search".equals(name)) {
					b.addTool(WebSearchTool20250305.builder().maxUses(5L).build());
				}
			}
		}

		Object toolChoiceObj = parameters.get("tool_choice");
		ToolChoice toolChoice = convertToolChoice(toolChoiceObj,
				parseBoolean(parameters.get("thinking"), parseBoolean(this.smssProp.getProperty(SMSS_KEY_THINKING), false)));
		if (toolChoice != null) {
			b.toolChoice(toolChoice);
		}
	}

	private static ToolChoice convertToolChoice(Object toolChoiceObj, boolean thinkingEnabled) {
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

		// When thinking is enabled, match the Python behavior: only auto/none.
		if (thinkingEnabled && ("required".equals(type) || "forced".equals(type))) {
			return ToolChoice.ofAuto(ToolChoiceAuto.builder().build());
		}

		if ("auto".equals(type)) {
			return ToolChoice.ofAuto(ToolChoiceAuto.builder().build());
		}
		if ("required".equals(type)) {
			return ToolChoice.ofAny(ToolChoiceAny.builder().build());
		}
		if ("forced".equals(type) && name != null && !name.isBlank()) {
			return ToolChoice.ofTool(ToolChoiceTool.builder().name(name).build());
		}
		if ("none".equals(type)) {
			return ToolChoice.ofNone(ToolChoiceNone.builder().build());
		}
		return null;
	}

	private static List<ToolUnion> convertMcpTools(List<Map<String, Object>> mcpTools) {
		List<ToolUnion> out = new ArrayList<>();
		for (Map<String, Object> tool : mcpTools) {
			String name = asString(tool.get("name"));
			if (name == null || name.isBlank()) {
				continue;
			}
			String description = asString(tool.get("description"));

			Object schemaObj = tool.get("inputSchema");
			if (!(schemaObj instanceof Map)) {
				continue;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> inputSchema = (Map<String, Object>) schemaObj;

			Tool.InputSchema schema = buildToolInputSchema(inputSchema);
			Tool anthropicTool = Tool.builder().name(name).description(description).inputSchema(schema).build();
			out.add(ToolUnion.ofTool(anthropicTool));
		}
		return out;
	}

	private static Tool.InputSchema buildToolInputSchema(Map<String, Object> inputSchema) {
		Tool.InputSchema.Builder b = Tool.InputSchema.builder();

		Object type = inputSchema.get("type");
		b.type(JsonValue.from(type != null ? type : "object"));

		Object requiredObj = inputSchema.get("required");
		if (requiredObj instanceof List<?>) {
			for (Object r : (List<?>) requiredObj) {
				if (r != null) {
					b.addRequired(r.toString());
				}
			}
		}

		Object propsObj = inputSchema.get("properties");
		if (propsObj instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> props = (Map<String, Object>) propsObj;
			Tool.InputSchema.Properties.Builder propsBuilder = Tool.InputSchema.Properties.builder();
			for (Map.Entry<String, Object> e : props.entrySet()) {
				String propName = e.getKey();
				Object propDef = stripSchemaTitles(e.getValue());
				propsBuilder.putAdditionalProperty(propName, JsonValue.from(propDef));
			}
			b.properties(propsBuilder.build());
		}

		return b.build();
	}

	private List<MessageParam> buildMessagesFromMessageJson(String messageJson) {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> msgs = GSON.fromJson(messageJson, List.class);
		if (msgs == null) {
			return List.of();
		}

		List<MessageParam> out = new ArrayList<>();
		List<ContentBlockParam> pendingToolResults = new ArrayList<>();

		for (int i = 0; i < msgs.size(); i++) {
			Map<String, Object> message = msgs.get(i);
			String type = asString(message.get("type"));
			if (type == null) {
				continue;
			}

			boolean isLast = i == msgs.size() - 1;

			if ("INPUT_TEXT".equals(type) || "INPUT_MEDIA".equals(type)) {
				List<ContentBlockParam> parts = new ArrayList<>();
				String text = asString(message.get("inputPrompt"));
				if (text == null) {
					text = asString(message.get("inputUIPrompt"));
				}
				if (text != null && !text.isBlank()) {
					parts.add(ContentBlockParam.ofText(TextBlockParam.builder().text(text).build()));
				}
				parts.addAll(buildMediaParts(message.get("mediaInputs")));
				out.add(MessageParam.builder().role(MessageParam.Role.USER).contentOfBlockParams(parts).build());
				continue;
			}

			if ("RESPONSE_TEXT".equals(type)) {
				String text = asString(message.get("content"));
				if (text == null) {
					text = "";
				}
				out.add(MessageParam.builder().role(MessageParam.Role.ASSISTANT).content(text).build());
				continue;
			}

			if ("RESPONSE_TOOL".equals(type)) {
				List<ContentBlockParam> parts = new ArrayList<>();

				String thinking = asString(message.get("thinking"));
				if (thinking != null && !thinking.isBlank()) {
					ThinkingBlockParam.Builder tb = ThinkingBlockParam.builder().thinking(thinking);
					String signature = asString(message.get("thinking_signature"));
					if (signature != null && !signature.isBlank()) {
						tb.signature(signature);
					}
					parts.add(ContentBlockParam.ofThinking(tb.build()));
				}

				Object toolResponsesObj = message.get("tool_responses");
				if (toolResponsesObj instanceof List<?>) {
					@SuppressWarnings("unchecked")
					List<Map<String, Object>> toolResponses = (List<Map<String, Object>>) toolResponsesObj;
					for (Map<String, Object> tool : toolResponses) {
						String id = asString(tool.get("id"));
						String name = asString(tool.get("name"));
						Object argsObj = tool.get("arguments");
						Map<String, Object> args = normalizeToolArgs(argsObj);
						if (id == null || id.isBlank() || name == null || name.isBlank()) {
							continue;
						}
						ToolUseBlockParam.Input input = buildToolUseInput(args);
						ToolUseBlockParam toolUse = ToolUseBlockParam.builder().id(id).name(name)
								.input(input).build();
						parts.add(ContentBlockParam.ofToolUse(toolUse));
					}
				}

				out.add(MessageParam.builder().role(MessageParam.Role.ASSISTANT).contentOfBlockParams(parts).build());
				continue;
			}

			if ("INPUT_TOOL_EXEC".equals(type)) {
				String toolCallId = asString(message.get("tool_call_id"));
				String result = asString(message.get("inputUIPrompt"));
				if (result == null) {
					result = "";
				}
				if (toolCallId != null && !toolCallId.isBlank()) {
					pendingToolResults
							.add(ContentBlockParam.ofToolResult(ToolResultBlockParam.builder().toolUseId(toolCallId)
									.content(result).build()));
				}

				boolean nextIsToolExec = (!isLast) && "INPUT_TOOL_EXEC".equals(asString(msgs.get(i + 1).get("type")));
				if (isLast || !nextIsToolExec) {
					if (!pendingToolResults.isEmpty()) {
						out.add(MessageParam.builder().role(MessageParam.Role.USER)
								.contentOfBlockParams(new ArrayList<>(pendingToolResults)).build());
						pendingToolResults.clear();
					}
				}
				continue;
			}
		}

		return out;
	}

	private List<MessageParam> buildMessagesFromSemossMessages(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return List.of();
		}

		List<MessageParam> out = new ArrayList<>();
		List<ContentBlockParam> pendingToolResults = new ArrayList<>();

		for (int i = 0; i < msgs.size(); i++) {
			AbstractMessage m = msgs.get(i);
			if (m == null) {
				continue;
			}
			MessageType type = m.getMessageType();
			if (type == null) {
				continue;
			}

			boolean isLast = i == msgs.size() - 1;

			if (type == MessageType.INPUT_TEXT || type == MessageType.INPUT_MEDIA) {
				InputMessage im = (InputMessage) m;
				List<ContentBlockParam> parts = new ArrayList<>();
				String text = firstNonBlank(im.getInputPrompt(), im.getInputUIPrompt());
				if (text != null && !text.isBlank()) {
					parts.add(ContentBlockParam.ofText(TextBlockParam.builder().text(text).build()));
				}
				parts.addAll(buildMediaPartsFromMedia(im.hasMediaInputs() ? im.getMediaInfos() : List.of()));
				out.add(MessageParam.builder().role(MessageParam.Role.USER).contentOfBlockParams(parts).build());
				continue;
			}

			if (type == MessageType.RESPONSE_TEXT) {
				ResponseMessage rm = (ResponseMessage) m;
				String text = rm.getContent();
				out.add(MessageParam.builder().role(MessageParam.Role.ASSISTANT).content(text != null ? text : "").build());
				continue;
			}

			if (type == MessageType.RESPONSE_TOOL) {
				ResponseMessage rm = (ResponseMessage) m;
				List<ContentBlockParam> parts = new ArrayList<>();

				String thinking = rm.getThinking();
				if (thinking != null && !thinking.isBlank()) {
					ThinkingBlockParam.Builder tb = ThinkingBlockParam.builder().thinking(thinking);
					parts.add(ContentBlockParam.ofThinking(tb.build()));
				}

				List<Map<String, Object>> toolResponses = rm.getToolResponses();
				for (Map<String, Object> tool : toolResponses) {
					String id = asString(tool.get("id"));
					String name = asString(tool.get("name"));
					Object argsObj = tool.get("arguments");
					Map<String, Object> args = normalizeToolArgs(argsObj);
					if (id == null || id.isBlank() || name == null || name.isBlank()) {
						continue;
					}
					ToolUseBlockParam.Input input = buildToolUseInput(args);
					ToolUseBlockParam toolUse = ToolUseBlockParam.builder().id(id).name(name).input(input).build();
					parts.add(ContentBlockParam.ofToolUse(toolUse));
				}

				out.add(MessageParam.builder().role(MessageParam.Role.ASSISTANT).contentOfBlockParams(parts).build());
				continue;
			}

			if (type == MessageType.INPUT_TOOL_EXEC) {
				InputMessage im = (InputMessage) m;
				String toolCallId = im.getToolCallId();
				String result = firstNonBlank(im.getInputUIPrompt(), im.getInputPrompt(), "");
				if (toolCallId != null && !toolCallId.isBlank()) {
					pendingToolResults.add(ContentBlockParam.ofToolResult(
							ToolResultBlockParam.builder().toolUseId(toolCallId).content(result).build()));
				}

				boolean nextIsToolExec = (!isLast) && (msgs.get(i + 1) != null)
						&& MessageType.INPUT_TOOL_EXEC == msgs.get(i + 1).getMessageType();
				if (isLast || !nextIsToolExec) {
					if (!pendingToolResults.isEmpty()) {
						out.add(MessageParam.builder().role(MessageParam.Role.USER)
								.contentOfBlockParams(new ArrayList<>(pendingToolResults)).build());
						pendingToolResults.clear();
					}
				}
				continue;
			}
		}

		return out;
	}

	private List<ContentBlockParam> buildMediaParts(Object mediaInputsObj) {
		if (!(mediaInputsObj instanceof List<?>)) {
			return List.of();
		}
		List<ContentBlockParam> parts = new ArrayList<>();
		for (Object o : (List<?>) mediaInputsObj) {
			if (!(o instanceof Map)) {
				continue;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> media = (Map<String, Object>) o;
			String mimeType = asString(media.get("mimeType"));
			String base64Data = asString(media.get("base64Data"));

			if (mimeType == null || base64Data == null || mimeType.isBlank() || base64Data.isBlank()) {
				continue;
			}
			if (!mimeType.toLowerCase().startsWith("image/")) {
				continue;
			}

			Base64ImageSource.MediaType mt = base64MediaType(mimeType);
			if (mt == null) {
				continue;
			}
			// Validate base64 (avoid sending invalid payloads)
			try {
				Base64.getDecoder().decode(base64Data);
			} catch (IllegalArgumentException e) {
				continue;
			}

			Base64ImageSource src = Base64ImageSource.builder().mediaType(mt).data(base64Data).build();
			ImageBlockParam img = ImageBlockParam.builder().source(ImageBlockParam.Source.ofBase64(src)).build();
			parts.add(ContentBlockParam.ofImage(img));
		}
		return parts;
	}

	private List<ContentBlockParam> buildMediaPartsFromMedia(List<MessageInputMedia> mediaInputs) {
		if (mediaInputs == null || mediaInputs.isEmpty()) {
			return List.of();
		}
		List<ContentBlockParam> parts = new ArrayList<>();
		for (MessageInputMedia media : mediaInputs) {
			if (media == null) {
				continue;
			}
			String mimeType = media.getMimeType();
			String base64Data = media.getBase64Data();

			if (mimeType == null || base64Data == null || mimeType.isBlank() || base64Data.isBlank()) {
				continue;
			}
			if (!mimeType.toLowerCase().startsWith("image/")) {
				continue;
			}

			Base64ImageSource.MediaType mt = base64MediaType(mimeType);
			if (mt == null) {
				continue;
			}
			try {
				Base64.getDecoder().decode(base64Data);
			} catch (IllegalArgumentException e) {
				continue;
			}

			Base64ImageSource src = Base64ImageSource.builder().mediaType(mt).data(base64Data).build();
			ImageBlockParam img = ImageBlockParam.builder().source(ImageBlockParam.Source.ofBase64(src)).build();
			parts.add(ContentBlockParam.ofImage(img));
		}
		return parts;
	}

	private static Base64ImageSource.MediaType base64MediaType(String mimeType) {
		String mt = mimeType.trim().toLowerCase();
		switch (mt) {
		case "image/jpeg":
		case "image/jpg":
			return Base64ImageSource.MediaType.IMAGE_JPEG;
		case "image/png":
			return Base64ImageSource.MediaType.IMAGE_PNG;
		case "image/gif":
			return Base64ImageSource.MediaType.IMAGE_GIF;
		case "image/webp":
			return Base64ImageSource.MediaType.IMAGE_WEBP;
		default:
			return null;
		}
	}

	private static ToolUseBlockParam.Input buildToolUseInput(Map<String, Object> args) {
		ToolUseBlockParam.Input.Builder b = ToolUseBlockParam.Input.builder();
		if (args == null || args.isEmpty()) {
			return b.build();
		}
		for (Map.Entry<String, Object> e : args.entrySet()) {
			if (e.getKey() == null) {
				continue;
			}
			b.putAdditionalProperty(e.getKey(), JsonValue.from(e.getValue()));
		}
		return b.build();
	}

	private static List<Map<String, Object>> extractToolCalls(Message response) {
		List<Map<String, Object>> tools = new ArrayList<>();
		for (ContentBlock block : response.content()) {
			if (block.isToolUse()) {
				ToolUseBlock t = block.asToolUse();
				Map<String, Object> toolMap = new HashMap<>();
				toolMap.put("id", t.id());
				toolMap.put("type", "function");
				toolMap.put("name", t.name());
				toolMap.put("arguments", t._input().convert(Map.class));
				tools.add(toolMap);
			}
		}
		return tools;
	}

	private static String extractText(Message response) {
		StringBuilder sb = new StringBuilder();
		for (ContentBlock block : response.content()) {
			if (block.isText()) {
				TextBlock t = block.asText();
				sb.append(t.text());
			}
		}
		return sb.toString();
	}

	private static String extractThinking(Message response) {
		StringBuilder sb = new StringBuilder();
		for (ContentBlock block : response.content()) {
			if (block.isThinking()) {
				ThinkingBlock t = block.asThinking();
				sb.append(t.thinking());
			}
		}
		String out = sb.toString();
		return out.isEmpty() ? null : out;
	}

	@Override
	protected InstructModelEngineResponse instructCall(String task, String context, List<Map<String, Object>> projectData,
			Insight insight, Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Instruct is not yet implemented for AnthropicJavaEngine.");
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Embeddings are not supported for AnthropicJavaEngine.");
	}

	@Override
	protected EmbeddingsModelEngineResponse imageEmbeddingsCall(List<String> imagesToEmbed, Insight insight,
			Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Image embeddings are not supported for AnthropicJavaEngine.");
	}

	@Override
	public void close() throws IOException {
		if (this.client != null) {
			this.client.close();
		}
	}
}
