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
package prerna.reactor.model;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.ModelPixelInvoker;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.openai.AbstractOpenAISseStream;
import prerna.engine.impl.model.openai.OpenAIChatCompletionsHelper;
import prerna.engine.impl.model.openai.OpenAIChatCompletionsStream;
import prerna.engine.impl.model.openai.OpenAIEmbeddingsHelper;
import prerna.engine.impl.model.openai.OpenAIImagesHelper;
import prerna.engine.impl.model.openai.OpenAIImagesStream;
import prerna.engine.impl.model.openai.OpenAIModelsHelper;
import prerna.engine.impl.model.openai.OpenAIResponsesHelper;
import prerna.engine.impl.model.openai.OpenAIResponsesStream;
import prerna.engine.impl.model.openai.OpenAIStreamRegistry;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Serve an OpenAI-protocol request to a caller that is already inside an
 * authenticated insight, so it never needs an access/secret key or a round trip
 * over http.
 *
 * <p>
 * This is the in-process twin of {@code OpenAIEndpoints}: both translate the
 * same request bodies through the same {@code prerna.engine.impl.model.openai}
 * helpers and invoke the model through the same {@link ModelPixelInvoker}, so
 * the bytes handed back here are the bytes that endpoint would have written to
 * the wire. The caller today is smss_openai.py, which installs an httpx
 * transport under the real {@code openai} python SDK and turns every request
 * the SDK makes into this pixel.
 *
 * <p>
 * Usage:
 *
 * <pre>
 * OpenAIPassthrough(path = "chat/completions", payload = "&lt;base64 of the request json&gt;");
 * </pre>
 *
 * The payload is base64-encoded UTF-8 json so that arbitrary prompt text
 * (quotes, newlines, pixel syntax) survives the trip through the pixel parser.
 * {@code stream}, {@code model} and {@code room_id} are read out of that json
 * exactly as the http endpoint reads them out of the request body.
 *
 * <p>
 * The return is a map describing an http response, which the caller turns back
 * into one:
 *
 * <pre>
 * { "status": 200, "contentType": "application/json", "body": "&lt;json&gt;" }
 * </pre>
 *
 * <p>
 * A streaming request answers with a job id in place of the body, because a
 * pixel cannot hold a response open the way the http endpoint can:
 *
 * <pre>
 * { "status": 200, "contentType": "text/event-stream", "jobId": "..." }
 * </pre>
 *
 * The caller then drives {@code OpenAIStreamPoll} with that job id, appending
 * each slice of SSE to its own stream until the poll reports done.
 *
 * Errors come back the same way, carrying an OpenAI-shaped error body and the
 * status the http endpoint would have used, so the calling SDK raises its
 * normal typed exception instead of a pixel error.
 */
public class OpenAIPassthroughReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(OpenAIPassthroughReactor.class);

	// one-off keys for this reactor, intentionally not in ReactorKeysEnum
	private static final String PATH = "path";
	private static final String PAYLOAD = "payload";

	private static final String STATUS = "status";
	private static final String CONTENT_TYPE = "contentType";
	private static final String BODY = "body";
	private static final String JOB_ID = "jobId";

	private static final String JSON_CONTENT_TYPE = "application/json";
	private static final String SSE_CONTENT_TYPE = "text/event-stream";

	public OpenAIPassthroughReactor() {
		this.keysToGet = new String[] { PATH, PAYLOAD };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String path = normalizePath(this.keyValue.get(PATH));
		if (path == null) {
			return errorResponse(400, "Bad Request: the 'path' input is required");
		}

		Map<String, Object> dataMap;
		try {
			dataMap = parsePayload(this.keyValue.get(PAYLOAD));
		} catch (Exception e) {
			classLogger.error("Failed to parse the OpenAI request payload for path '{}'", path, e);
			return errorResponse(400, "Error processing JSON data: " + e.getMessage());
		}

		User user = this.insight.getUser();
		if (user == null) {
			return errorResponse(401, "Could not determine the user making this request");
		}

		if (path.equals("chat/completions")) {
			return chatCompletions(user, dataMap);
		} else if (path.equals("responses")) {
			return responses(user, dataMap);
		} else if (path.equals("embeddings")) {
			return embeddings(user, dataMap);
		} else if (path.equals("images/generations")) {
			return imagesGenerations(user, dataMap);
		} else if (path.equals("models")) {
			return jsonResponse(200, OpenAIModelsHelper.listModels(user));
		} else if (path.startsWith("models/")) {
			String modelId = path.substring("models/".length());
			Map<String, Object> model = OpenAIModelsHelper.retrieveModel(user, modelId);
			if (model == null) {
				return errorResponse(404, "Could not find model = '" + modelId + "'");
			}
			return jsonResponse(200, model);
		}

		return errorResponse(404, "Unsupported OpenAI path '" + path + "'");
	}

	/**
	 * Serve {@code /chat/completions}. Mirrors
	 * {@code OpenAIEndpoints.runModelChatCompletion}.
	 */
	private NounMetadata chatCompletions(User user, Map<String, Object> dataMap) {
		boolean isStreamingRequest = isStreaming(dataMap);

		String engineId = (String) dataMap.remove("model");
		if (engineId == null || engineId.isEmpty()) {
			return errorResponse(400, "Bad Request: the request is missing the required 'model' field.");
		}

		Object fullPrompt = dataMap.remove("messages");
		if (fullPrompt == null) {
			return errorResponse(400, "Please provide 'messages'.");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			return errorResponse(403,
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		IModelEngine engine = Utility.getModel(engineId);
		Room room = RoomUtils.createRoomIfNotExists(getRoomId(dataMap), this.insight, engine, null);

		// this is if you are passing full prompt but want us to maintain the history
		boolean appendFullPrompt = Boolean.parseBoolean(dataMap.remove("append_full_prompt") + "");

		dataMap.put(AbstractModelEngine.FULL_PROMPT, fullPrompt);
		dataMap.put(AbstractModelEngine.APPEND_FULL_PROMPT, appendFullPrompt);

		if (!isStreamingRequest) {
			AskModelEngineResponse<?> llmResponse;
			try {
				llmResponse = ModelPixelInvoker.askModelSync(engine, this.insight, room, dataMap);
			} catch (Exception e) {
				classLogger.error("Chat completions synchronous model call failed for engine '{}'", engineId, e);
				return errorResponse(400, e.getMessage());
			}
			return jsonResponse(200, OpenAIChatCompletionsHelper.processAskModelEngineResponse(engineId, llmResponse));
		}

		long creationTimestamp = Instant.now().getEpochSecond();
		String messageId = "chatcmpl-" + GUID.v7().toUUID().toString();
		String jobId = ModelPixelInvoker.startAsyncModelRequest(engine, this.insight, room, dataMap,
				resolveSessionId());
		return streamResponse(new OpenAIChatCompletionsStream(engineId, messageId, creationTimestamp, jobId));
	}

	/**
	 * Serve {@code /responses}. Mirrors {@code OpenAIEndpoints.runResponses}.
	 */
	private NounMetadata responses(User user, Map<String, Object> dataMap) {
		boolean isStreamingRequest = isStreaming(dataMap);

		String engineId = (String) dataMap.remove("model");
		if (engineId == null || engineId.isEmpty()) {
			return errorResponse(400, "Missing 'model' field.");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			return errorResponse(403, "Model " + engineId + " inaccessible.");
		}

		IModelEngine engine = Utility.getModel(engineId);
		Object messages = OpenAIResponsesHelper.normalizeMessages(dataMap.remove("input"));

		Room room = RoomUtils.createRoomIfNotExists(getRoomId(dataMap), this.insight, engine, null);

		dataMap.put(AbstractModelEngine.FULL_PROMPT, messages);

		long creationTimestamp = Instant.now().getEpochSecond();
		String responseId = "resp_" + GUID.v7().toUUID().toString();

		if (!isStreamingRequest) {
			AskModelEngineResponse<?> llmResponse;
			try {
				llmResponse = ModelPixelInvoker.askModelSync(engine, this.insight, room, dataMap);
			} catch (Exception e) {
				classLogger.error("Responses synchronous model call failed for engine '{}'", engineId, e);
				return errorResponse(400, e.getMessage());
			}
			return jsonResponse(200, OpenAIResponsesHelper.processAskModelEngineResponse(engineId, llmResponse));
		}

		String jobId = ModelPixelInvoker.startAsyncModelRequest(engine, this.insight, room, dataMap,
				resolveSessionId());
		return streamResponse(new OpenAIResponsesStream(engineId, responseId, creationTimestamp, jobId));
	}

	/**
	 * Serve {@code /embeddings}. Mirrors
	 * {@code OpenAIEndpoints.runModelEmbeddings}.
	 */
	private NounMetadata embeddings(User user, Map<String, Object> dataMap) {
		String engineId = (String) dataMap.remove("model");
		if (engineId == null || engineId.isEmpty()) {
			return errorResponse(400, "Bad Request: the request is missing the required 'model' field.");
		}

		Object input = dataMap.remove("input");
		List<String> stringsToEncode = new ArrayList<>();
		if (input instanceof String) {
			stringsToEncode.add((String) input);
		} else if (input instanceof List) {
			for (Object value : (List<?>) input) {
				stringsToEncode.add(value + "");
			}
		}
		if (stringsToEncode.isEmpty()) {
			return errorResponse(400, "Bad Request: the request is missing the required 'input' field.");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			return errorResponse(403,
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		IModelEngine engine = Utility.getModel(engineId);
		EmbeddingsModelEngineResponse embeddingsResponse;
		try {
			embeddingsResponse = engine.embeddings(stringsToEncode, this.insight, dataMap);
		} catch (Exception e) {
			classLogger.error("Embeddings call failed for engine '{}'", engineId, e);
			return errorResponse(400, e.getMessage());
		}

		return jsonResponse(200, OpenAIEmbeddingsHelper.processEmbeddingsResponse(engineId, embeddingsResponse));
	}

	/**
	 * Serve {@code /images/generations}. Mirrors
	 * {@code OpenAIEndpoints.runImagesGenerations}.
	 */
	private NounMetadata imagesGenerations(User user, Map<String, Object> dataMap) {
		boolean isStreamingRequest = isStreaming(dataMap);

		String engineId = (String) dataMap.remove("model");
		if (engineId == null || engineId.isEmpty()) {
			return errorResponse(400, "Missing required field 'model'.");
		}

		String prompt = (String) dataMap.remove("prompt");
		if (prompt == null || prompt.isEmpty()) {
			return errorResponse(400, "Missing required field 'prompt'.");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			return errorResponse(403, "Model " + engineId + " does not exist or user does not have access.");
		}

		IModelEngine engine = Utility.getModel(engineId);
		Room room = RoomUtils.createRoomIfNotExists(getRoomId(dataMap), this.insight, engine, null);

		List<Map<String, Object>> messages = new ArrayList<>();
		Map<String, Object> userMsg = new HashMap<>();
		userMsg.put("role", "user");
		userMsg.put("content", prompt);
		messages.add(userMsg);
		dataMap.put(AbstractModelEngine.FULL_PROMPT, messages);

		String outputFormat = (String) dataMap.get("output_format");
		String quality = (String) dataMap.get("quality");
		String size = (String) dataMap.get("size");

		long creationTimestamp = Instant.now().getEpochSecond();

		if (!isStreamingRequest) {
			AskModelEngineResponse<?> llmResponse;
			try {
				llmResponse = ModelPixelInvoker.askModelSync(engine, this.insight, room, dataMap);
			} catch (Exception e) {
				classLogger.error("Images synchronous model call failed for engine '{}'", engineId, e);
				return errorResponse(400, e.getMessage());
			}
			return jsonResponse(200, OpenAIImagesHelper.buildNonStreamingResponse(creationTimestamp, llmResponse));
		}

		String jobId = ModelPixelInvoker.startAsyncModelRequest(engine, this.insight, room, dataMap,
				resolveSessionId());
		return streamResponse(new OpenAIImagesStream(engineId, creationTimestamp, jobId, outputFormat, quality, size));
	}

	/**
	 * Reduce the path the caller asked for to the bare operation: no leading or
	 * trailing slash and no api version prefix, so "/v1/chat/completions" and
	 * "chat/completions" are the same request.
	 *
	 * @param path the raw path input
	 * @return the normalized path, or {@code null} if nothing was passed
	 */
	private String normalizePath(String path) {
		if (path == null || (path = path.trim()).isEmpty()) {
			return null;
		}
		while (path.startsWith("/")) {
			path = path.substring(1);
		}
		while (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		if (path.startsWith("v1/")) {
			path = path.substring("v1/".length());
		}
		return path.isEmpty() ? null : path;
	}

	/**
	 * Decode the base64 payload into the request body map. A missing payload is an
	 * empty body, which is what the {@code models} paths send.
	 *
	 * @param payload the base64-encoded UTF-8 json request body
	 * @return the parsed request body
	 */
	private Map<String, Object> parsePayload(String payload) {
		if (payload == null || (payload = payload.trim()).isEmpty()) {
			return new HashMap<>();
		}
		String json = new String(Base64.getDecoder().decode(payload), StandardCharsets.UTF_8);
		Map<String, Object> dataMap = GSON.fromJson(json, new TypeToken<Map<String, Object>>() {
		}.getType());
		if (dataMap == null) {
			return new HashMap<>();
		}
		// these are http/session concerns the caller does not get to set - it is
		// already executing inside its own insight
		dataMap.remove("client_metadata");
		dataMap.remove("insight_id");
		return dataMap;
	}

	/**
	 * Read the {@code stream} flag from the request body. The flag is left in
	 * place: it travels on to the model engine, which is what makes the engine emit
	 * the partial chunks the streaming helpers poll for.
	 */
	private boolean isStreaming(Map<String, Object> dataMap) {
		Object stream = dataMap.get("stream");
		return stream != null && Boolean.parseBoolean(stream + "");
	}

	/**
	 * Read and remove the optional {@code room_id} from the request body. When it
	 * is absent {@code RoomUtils} makes a new room for this insight.
	 */
	private String getRoomId(Map<String, Object> dataMap) {
		Object roomId = dataMap.remove("room_id");
		return roomId == null ? null : roomId + "";
	}

	/**
	 * The session the calling insight belongs to, needed to register the async job.
	 * Falls back to the insight id when the executing thread has no session, which
	 * only happens for insights that are not attached to a logged in session.
	 */
	private String resolveSessionId() {
		String sessionId = ThreadStore.getSessionId();
		return sessionId == null ? this.insight.getInsightId() : sessionId;
	}

	private NounMetadata jsonResponse(int status, Map<String, Object> body) {
		return response(status, JSON_CONTENT_TYPE, GSON.toJson(body));
	}

	/**
	 * Answer a streaming request with the job to poll rather than a body.
	 *
	 * <p>
	 * A pixel cannot hold a response open the way the http endpoint can, so the
	 * stream is registered and the caller drives it with {@code OpenAIStreamPoll},
	 * appending each slice until that call reports done. Buffering the whole
	 * conversation here instead would mean the caller waits for the last token
	 * before seeing the first.
	 *
	 * @param stream the stream assembling this conversation
	 * @return the job id for the caller to poll
	 */
	private NounMetadata streamResponse(AbstractOpenAISseStream stream) {
		OpenAIStreamRegistry.register(stream);
		Map<String, Object> response = new HashMap<>();
		response.put(STATUS, 200);
		response.put(CONTENT_TYPE, SSE_CONTENT_TYPE);
		response.put(JOB_ID, stream.getJobId());
		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Build the OpenAI error body the calling SDK expects, so it can raise the
	 * typed exception that matches the status.
	 */
	private NounMetadata errorResponse(int status, String message) {
		Map<String, Object> error = new HashMap<>();
		error.put("message", message == null ? "Unknown error" : message);
		error.put("type", status == 403 || status == 401 ? "invalid_request_error" : "api_error");
		Map<String, Object> body = new HashMap<>();
		body.put("error", error);
		return jsonResponse(status, body);
	}

	private NounMetadata response(int status, String contentType, String body) {
		Map<String, Object> response = new HashMap<>();
		response.put(STATUS, status);
		response.put(CONTENT_TYPE, contentType);
		response.put(BODY, body);
		return new NounMetadata(response, PixelDataType.MAP);
	}
}
