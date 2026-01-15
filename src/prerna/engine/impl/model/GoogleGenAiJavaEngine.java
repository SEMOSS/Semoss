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
import static prerna.engine.impl.model.ModelEngineSharedUtils.normalizeToolArgs;
import static prerna.engine.impl.model.ModelEngineSharedUtils.parseBoolean;
import static prerna.engine.impl.model.ModelEngineSharedUtils.stackTraceToString;
import static prerna.engine.impl.model.ModelEngineSharedUtils.stripSchemaTitles;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.genai.Client;
import com.google.genai.ResponseStream;
import com.google.genai.types.Content;
import com.google.genai.types.EnterpriseWebSearch;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionDeclaration;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.GoogleMaps;
import com.google.genai.types.GoogleSearch;
import com.google.genai.types.GoogleSearchRetrieval;
import com.google.genai.types.Part;
import com.google.genai.types.ThinkingConfig;
import com.google.genai.types.Tool;
import com.google.genai.types.ToolCodeExecution;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import com.fasterxml.jackson.annotation.JsonFormat;

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

/**
 * In-process Google GenAI (Gemini) engine using the Java SDK (`com.google.genai`).
 *
 * This is intended to be a drop-in alternative to the existing Python-backed
 * `VertexEngine` flow: SEMOSS still provides `message_json` and tool schema
 * payloads; this engine converts them into Google GenAI `Content`/`Part` and
 * returns SEMOSS `AskModelEngineResponse`.
 *
 * Expected SMSS keys (aligned with existing Gemini/Vertex SMSS files):
 * - {@link Constants#MODEL} (required)
 * - PROJECT (required for Vertex auth)
 * - REGION (required for Vertex auth)
 * - SERVICE_ACCOUNT_CREDENTIALS (service account JSON string; required for Vertex auth)
 * - {@link Constants#API_KEY} (optional; Gemini API auth alternative)
 */
public class GoogleGenAiJavaEngine extends AbstractModelEngine {

	private static final Logger classLogger = LogManager.getLogger(GoogleGenAiJavaEngine.class);
	private static final AtomicBoolean JACKSON_LOGGED = new AtomicBoolean(false);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String SMSS_KEY_PROJECT = "PROJECT";
	private static final String SMSS_KEY_REGION = "REGION";
	private static final String SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS = "SERVICE_ACCOUNT_CREDENTIALS";
	private static final String SMSS_KEY_THINKING = "THINKING";
	private static final String SMSS_KEY_THINKING_BUDGET = "THINKING_BUDGET";

	private static final List<String> DEFAULT_GCP_SCOPES = List.of("https://www.googleapis.com/auth/cloud-platform",
			"https://www.googleapis.com/auth/generative-language");

	private Client client;
	private String modelName;

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.VERTEX;
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.modelName = smssProp.getProperty(Constants.MODEL);
		if (this.modelName == null || this.modelName.trim().isEmpty()) {
			throw new IllegalArgumentException("Missing required SMSS key: " + Constants.MODEL);
		}

		String apiKey = smssProp.getProperty(Constants.API_KEY);
		if (apiKey != null && !apiKey.trim().isEmpty()) {
			this.client = Client.builder().apiKey(apiKey.trim()).build();
			return;
		}

		String project = smssProp.getProperty(SMSS_KEY_PROJECT);
		String region = smssProp.getProperty(SMSS_KEY_REGION);
		String serviceAccountJson = smssProp.getProperty(SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS);

		if (project == null || project.trim().isEmpty() || region == null || region.trim().isEmpty()
				|| serviceAccountJson == null || serviceAccountJson.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"Vertex auth requires PROJECT, REGION, and SERVICE_ACCOUNT_CREDENTIALS (or set API_KEY for Gemini API).");
		}

		GoogleCredentials credentials = GoogleCredentials
				.fromStream(new ByteArrayInputStream(serviceAccountJson.getBytes(StandardCharsets.UTF_8)))
				.createScoped(DEFAULT_GCP_SCOPES);

		this.client = Client.builder().vertexAI(true).project(project.trim()).location(region.trim())
				.credentials(credentials).build();
	}

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			String roomId, Map<String, Object> parameters) {
		try {
			logJacksonClasspathOnce();

			if (this.client == null) {
				throw new IllegalStateException("Google GenAI client is not initialized.");
			}

			boolean stream = parseBoolean(parameters != null ? parameters.get("stream") : null, true);
			if (parameters != null && parameters.containsKey("streaming")) {
				stream = parseBoolean(parameters.get("streaming"), stream);
			}

			String messageJson = parameters != null ? asString(parameters.get("message_json")) : null;
			List<Content> contents;
			if (messageJson != null && !messageJson.trim().isEmpty()) {
				contents = buildContentsFromMessageJson(messageJson);
			} else if (question != null && !question.trim().isEmpty()) {
				contents = List.of(Content.builder().role("user").parts(Part.fromText(question)).build());
			} else {
				throw new IllegalArgumentException("Missing `message_json` (and no fallback `question` provided).");
			}

			GenerateContentConfig config = buildConfig(context, parameters);

			GenerateContentResponse finalResponse;
			String fullTextOverride = null;
			if (stream) {
				StreamingResult streamed = generateWithStreaming(insight.getInsightId(), contents, config);
				finalResponse = streamed.lastResponse;
				fullTextOverride = streamed.fullText;
			} else {
				finalResponse = this.client.models.generateContent(this.modelName, contents, config);
			}

			int promptTokens = finalResponse != null ? finalResponse.usageMetadata().flatMap(u -> u.promptTokenCount()).orElse(0) : 0;
			int responseTokens = finalResponse != null ? finalResponse.usageMetadata().flatMap(u -> u.candidatesTokenCount()).orElse(0) : 0;

			List<FunctionCall> functionCalls = finalResponse != null ? finalResponse.functionCalls() : List.of();
			if (functionCalls != null && !functionCalls.isEmpty()) {
				List<Map<String, Object>> tools = new ArrayList<>();
				for (int i = 0; i < functionCalls.size(); i++) {
					FunctionCall fc = functionCalls.get(i);
					Map<String, Object> toolMap = new HashMap<>();
					toolMap.put("id", fc.id().orElse(String.valueOf(i)));
					toolMap.put("type", "function");
					toolMap.put("name", fc.name().orElse(null));
					toolMap.put("arguments", fc.args().orElse(new HashMap<>()));
					tools.add(toolMap);
				}
				return new AskToolModelEngineResponse(tools, promptTokens, responseTokens);
			}

			String text = fullTextOverride != null ? fullTextOverride
					: (finalResponse != null && finalResponse.text() != null ? finalResponse.text() : "");
			AskStringModelEngineResponse response = new AskStringModelEngineResponse(text, promptTokens, responseTokens);

			String thinking = finalResponse != null ? extractThinking(finalResponse) : null;
			if (thinking != null && !thinking.isBlank()) {
				response.setThinking(thinking);
			}

			return response;
		} catch (Throwable t) {
			classLogger.error(Constants.STACKTRACE, t);
			return new AskErrorModelEngineResponse(t.getMessage(), t.getClass().getSimpleName(), 0, "google",
					this.modelName, stackTraceToString(t));
		}
	}

	private static void logJacksonClasspathOnce() {
		if (!JACKSON_LOGGED.compareAndSet(false, true)) {
			return;
		}
		try {
			String resourcePath = "com/fasterxml/jackson/annotation/JsonFormat$Feature.class";
			java.util.Enumeration<java.net.URL> all = JsonFormat.Feature.class.getClassLoader().getResources(resourcePath);
			List<java.net.URL> urls = java.util.Collections.list(all);
			classLogger.info("google-genai runtime jackson-annotations resource hits (count={}) = {}", urls.size(), urls);

			java.net.URL jsonFormatSource = JsonFormat.Feature.class.getProtectionDomain().getCodeSource().getLocation();
			classLogger.info("google-genai runtime jackson-annotations source = {}", jsonFormatSource);
		} catch (Throwable t) {
			classLogger.warn("Unable to determine jackson-annotations CodeSource", t);
		}
	}

	private static final class StreamingResult {
		private final GenerateContentResponse lastResponse;
		private final String fullText;

		private StreamingResult(GenerateContentResponse lastResponse, String fullText) {
			this.lastResponse = lastResponse;
			this.fullText = fullText;
		}
	}

	private StreamingResult generateWithStreaming(String insightId, List<Content> contents, GenerateContentConfig config) {
		StringBuilder fullText = new StringBuilder();
		GenerateContentResponse last = null;

		try (ResponseStream<GenerateContentResponse> stream = this.client.models.generateContentStream(this.modelName,
				contents, config)) {
			for (GenerateContentResponse partial : stream) {
				last = partial;
				String partText = partial.text();
				if (partText != null && !partText.isEmpty()) {
					fullText.append(partText);
					PixelJobManager.getManager().addPartialOut(insightId, partText);
				}
			}
		}

		return new StreamingResult(last, fullText.toString());
	}

	private GenerateContentConfig buildConfig(String context, Map<String, Object> parameters) {
		Integer maxOutputTokens = firstInt(parameters, "max_new_tokens", "max_completion_tokens", "max_tokens");
		if (maxOutputTokens == null) {
			maxOutputTokens = parseInt(this.smssProp.getProperty(Constants.MAX_TOKENS));
		}
		Float temperature = firstFloat(parameters, "temperature");
		Float topP = firstFloat(parameters, "top_p");
		Float topK = firstFloat(parameters, "top_k");

		GenerateContentConfig.Builder builder = GenerateContentConfig.builder();

		if (context != null && !context.trim().isEmpty()) {
			builder.systemInstruction(Content.fromParts(Part.fromText(context)));
		}
		if (maxOutputTokens != null) {
			builder.maxOutputTokens(maxOutputTokens);
		}
		if (temperature != null) {
			builder.temperature(temperature);
		}
		if (topP != null) {
			builder.topP(topP);
		}
		if (topK != null) {
			builder.topK(topK);
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
				builder.stopSequences(stops);
			}
		}

		List<Tool> tools = buildTools(parameters);
		if (!tools.isEmpty()) {
			builder.tools(tools);
		}

		ThinkingConfig thinkingConfig = resolveThinkingConfig(parameters);
		if (thinkingConfig != null) {
			builder.thinkingConfig(thinkingConfig);
		}

		return builder.build();
	}

	private ThinkingConfig resolveThinkingConfig(Map<String, Object> parameters) {
		Boolean thinking = null;
		Integer thinkingBudget = null;

		if (parameters != null) {
			Object t = parameters.get("thinking");
			if (t != null) {
				thinking = parseBoolean(t, false);
			}
			Object b = parameters.get("thinking_budget");
			if (b instanceof Number) {
				thinkingBudget = ((Number) b).intValue();
			} else if (b != null) {
				try {
					thinkingBudget = Integer.parseInt(b.toString());
				} catch (Exception ignore) {
				}
			}
		}

		if (thinking == null) {
			thinking = parseBoolean(this.smssProp.getProperty(SMSS_KEY_THINKING), false);
		}
		if (thinkingBudget == null) {
			String tb = this.smssProp.getProperty(SMSS_KEY_THINKING_BUDGET);
			if (tb != null && !tb.trim().isEmpty()) {
				try {
					thinkingBudget = Integer.parseInt(tb.trim());
				} catch (Exception ignore) {
				}
			}
		}

		if (thinking == null || !thinking) {
			return null;
		}

		ThinkingConfig.Builder builder = ThinkingConfig.builder().includeThoughts(true);
		if (thinkingBudget != null) {
			builder.thinkingBudget(thinkingBudget);
		}
		return builder.build();
	}

	private List<Tool> buildTools(Map<String, Object> parameters) {
		List<Tool> allTools = new ArrayList<>();
		if (parameters == null) {
			return allTools;
		}

		// MCP tools from SEMOSS (`tools` param) -> Google function declarations.
		Object toolsObj = parameters.get("tools");
		if (toolsObj instanceof List<?>) {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> mcpTools = (List<Map<String, Object>>) toolsObj;
			List<FunctionDeclaration> decls = new ArrayList<>();
			for (Map<String, Object> tool : mcpTools) {
				String name = asString(tool.get("name"));
				if (name == null || name.trim().isEmpty()) {
					continue;
				}
				String description = asString(tool.get("description"));
				Object inputSchema = tool.get("inputSchema");
				Object parametersJsonSchema = stripSchemaTitles(inputSchema);

				FunctionDeclaration.Builder decl = FunctionDeclaration.builder().name(name).description(description);
				if (parametersJsonSchema != null) {
					decl.parametersJsonSchema(parametersJsonSchema);
				}
				decls.add(decl.build());
			}

			if (!decls.isEmpty()) {
				allTools.add(Tool.builder().functionDeclarations(decls).build());
			}
		}

		// Built-in tools (`built_in_tools` param): match the Python builder behavior.
		Object builtInToolsObj = parameters.get("built_in_tools");
		if (builtInToolsObj instanceof List<?>) {
			for (Object t : (List<?>) builtInToolsObj) {
				if (t == null) {
					continue;
				}
				String name = t.toString().trim().toLowerCase();
				if (name.isEmpty()) {
					continue;
				}
				if ("web_search".equals(name)) {
					// Default to enterprise web search to match Python.
					allTools.add(Tool.builder().enterpriseWebSearch(EnterpriseWebSearch.builder().build()).build());
				} else if ("google_search".equals(name)) {
					allTools.add(Tool.builder().googleSearch(GoogleSearch.builder().build()).build());
				} else if ("websearch_retrieval".equals(name)) {
					allTools.add(Tool.builder().googleSearchRetrieval(GoogleSearchRetrieval.builder().build()).build());
				} else if ("google_maps".equals(name)) {
					allTools.add(Tool.builder().googleMaps(GoogleMaps.builder().build()).build());
				} else if ("code_execution".equals(name)) {
					allTools.add(Tool.builder().codeExecution(ToolCodeExecution.builder().build()).build());
				}
			}
		}

		return allTools;
	}

	private List<Content> buildContentsFromMessageJson(String messageJson) {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> msgs = GSON.fromJson(messageJson, List.class);
		if (msgs == null) {
			return List.of();
		}

		List<Content> contents = new ArrayList<>();
		Map<String, String> toolIdToName = new HashMap<>();
		List<Part> pendingToolResponses = new ArrayList<>();
		int expectedToolCount = 0;

		for (Map<String, Object> message : msgs) {
			String type = asString(message.get("type"));
			if (type == null) {
				continue;
			}

			if ("INPUT_TEXT".equals(type) || "INPUT_MEDIA".equals(type)) {
				List<Part> parts = new ArrayList<>();
				String text = asString(message.get("inputPrompt"));
				if (text == null) {
					text = asString(message.get("inputUIPrompt"));
				}
				if (text != null && !text.isEmpty()) {
					parts.add(Part.fromText(text));
				}
				parts.addAll(buildMediaParts(message.get("mediaInputs")));
				contents.add(Content.builder().role("user").parts(parts).build());
				continue;
			}

			if ("RESPONSE_TOOL".equals(type)) {
				Object toolResponsesObj = message.get("tool_responses");
				if (toolResponsesObj instanceof List<?>) {
					List<Part> parts = new ArrayList<>();
					@SuppressWarnings("unchecked")
					List<Map<String, Object>> toolResponses = (List<Map<String, Object>>) toolResponsesObj;
					expectedToolCount = toolResponses.size();

					for (Map<String, Object> tool : toolResponses) {
						String id = asString(tool.get("id"));
						String name = asString(tool.get("name"));
						Object argsObj = tool.get("arguments");
						Map<String, Object> args = normalizeToolArgs(argsObj);

						if (id != null && name != null) {
							toolIdToName.put(id, name);
						}
						if (name != null && !name.isBlank()) {
							parts.add(Part.fromFunctionCall(name, args));
						}
					}

					contents.add(Content.builder().role("model").parts(parts).build());
				}
				continue;
			}

			if ("INPUT_TOOL_EXEC".equals(type)) {
				if (expectedToolCount <= 0) {
					continue;
				}
				String toolCallId = asString(message.get("tool_call_id"));
				String toolName = toolCallId != null ? toolIdToName.get(toolCallId) : null;
				String result = asString(message.get("inputUIPrompt"));
				if (result == null) {
					result = "";
				}

				if (toolName != null) {
					pendingToolResponses.add(Part.fromFunctionResponse(toolName, Map.of("result", result)));
					if (pendingToolResponses.size() == expectedToolCount) {
						contents.add(Content.builder().role("user").parts(pendingToolResponses).build());
						pendingToolResponses = new ArrayList<>();
						expectedToolCount = 0;
					}
				}
				continue;
			}

			if ("RESPONSE_TEXT".equals(type)) {
				String text = asString(message.get("content"));
				if (text == null) {
					text = "";
				}
				contents.add(Content.builder().role("model").parts(Part.fromText(text)).build());
				continue;
			}
		}

		return contents;
	}

	private List<Part> buildMediaParts(Object mediaInputsObj) {
		if (!(mediaInputsObj instanceof List<?>)) {
			return List.of();
		}
		List<Part> parts = new ArrayList<>();
		for (Object o : (List<?>) mediaInputsObj) {
			if (!(o instanceof Map)) {
				continue;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> media = (Map<String, Object>) o;
			String sourceUrl = asString(media.get("sourceUrl"));
			String mimeType = asString(media.get("mimeType"));
			if (mimeType == null || mimeType.isBlank()) {
				mimeType = "application/octet-stream";
			}

			if (sourceUrl != null && !sourceUrl.isBlank()) {
				parts.add(Part.fromUri(sourceUrl, mimeType));
				continue;
			}

			String base64Data = asString(media.get("base64Data"));
			if (base64Data != null && !base64Data.isBlank()) {
				byte[] bytes = Base64.getDecoder().decode(base64Data);
				parts.add(Part.fromBytes(bytes, mimeType));
			}
		}
		return parts;
	}

	private static Integer firstInt(Map<String, Object> parameters, String... keys) {
		if (parameters == null) {
			return null;
		}
		for (String key : keys) {
			Object v = parameters.get(key);
			Integer parsed = parseInt(v);
			if (parsed != null) {
				return parsed;
			}
		}
		return null;
	}

	private static Float firstFloat(Map<String, Object> parameters, String... keys) {
		if (parameters == null) {
			return null;
		}
		for (String key : keys) {
			Object v = parameters.get(key);
			Float parsed = parseFloat(v);
			if (parsed != null) {
				return parsed;
			}
		}
		return null;
	}

	private static Integer parseInt(Object v) {
		if (v instanceof Number) {
			return ((Number) v).intValue();
		}
		if (v == null) {
			return null;
		}
		try {
			return Integer.parseInt(v.toString().trim());
		} catch (Exception e) {
			return null;
		}
	}

	private static Float parseFloat(Object v) {
		if (v instanceof Number) {
			return ((Number) v).floatValue();
		}
		if (v == null) {
			return null;
		}
		try {
			return Float.parseFloat(v.toString().trim());
		} catch (Exception e) {
			return null;
		}
	}

	private static String extractThinking(GenerateContentResponse response) {
		StringBuilder sb = new StringBuilder();
		for (Part p : response.parts()) {
			if (p.text().isPresent() && p.thought().isPresent() && Boolean.TRUE.equals(p.thought().get())) {
				sb.append(p.text().get());
			}
		}
		String out = sb.toString();
		return out.isEmpty() ? null : out;
	}

	@Override
	protected InstructModelEngineResponse instructCall(String task, String context, List<Map<String, Object>> projectData,
			Insight insight, Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Instruct is not yet implemented for GoogleGenAiJavaEngine.");
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Embeddings are not yet implemented for GoogleGenAiJavaEngine.");
	}

	@Override
	protected EmbeddingsModelEngineResponse imageEmbeddingsCall(List<String> imagesToEmbed, Insight insight,
			Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Image embeddings are not yet implemented for GoogleGenAiJavaEngine.");
	}

	@Override
	public void close() throws IOException {
		if (this.client != null) {
			this.client.close();
		}
	}
}
