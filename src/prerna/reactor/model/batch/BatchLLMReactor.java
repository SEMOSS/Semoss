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
package prerna.reactor.model.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.batch.ModelBatchManager;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.BatchSubmissionResponse;
import prerna.om.ThreadStore;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Submit a batch of LLM prompts using the provider's native batch API.
 * Mirrors LLMReactor's UX: accepts plain strings or {command, context} maps.
 * Python normalizes these to the provider wire format (OpenAI body / Anthropic params).
 */
public class BatchLLMReactor extends AbstractModelBatchReactor {

	public BatchLLMReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.BATCH_REQUESTS.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey()
		};
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		// batch-level params go as Python kwargs
		Map<String, Object> batchParams = new HashMap<>();

		// shared model params (max_tokens, temperature, etc.) merged into every request
		Map<String, Object> sharedModelParams = baseParams();

		List<Map<String, Object>> requests = buildRequests(sharedModelParams);
		if (requests.isEmpty()) {
			throw new IllegalArgumentException("At least one request is required");
		}

		IModelEngine engine = ModelBatchManager.resolveEngine(getUser(), engineId);

		// optional: seed every request with a room's history + tools (one-shot, no tool execution)
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId != null && !roomId.isEmpty()) {
			enrichWithRoomContext(requests, engine, roomId, sharedModelParams);
		} else if (requestsHaveMedia(requests)) {
			enrichWithMedia(requests, engine, sharedModelParams);
		}

		BatchSubmissionResponse response = engine.submitBatch(requests, batchParams);
		if (response.getProviderBatchId() != null) {
			ModelBatchManager.recordBatchSubmission(getUser(), engine, response.getProviderBatchId(), requests,
					this.insight.getInsightId(), ThreadStore.getSessionId());
		}
		return new NounMetadata(response.toMap(), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private List<Map<String, Object>> buildRequests(Map<String, Object> sharedModelParams) {
		List<Map<String, Object>> out = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.BATCH_REQUESTS.getKey());
		if (grs != null && !grs.isEmpty()) {
			for (int i = 0; i < grs.size(); i++) {
				addRequest(out, grs.get(i), sharedModelParams);
			}
		}
		return out;
	}

	/**
	 * When a roomId is supplied, seed every request with that room's conversation
	 * history and MCP tools, reusing the same builders as the synchronous ask path.
	 * Each request gets a "message_json" (history + this request's command) and the
	 * shared "tools" list. The plain "command" text is left in place so it is still
	 * stored as the MESSAGE input for display. The batch remains one-shot; tool_use
	 * blocks in the results are surfaced but never executed.
	 */
	private void enrichWithRoomContext(List<Map<String, Object>> requests, IModelEngine engine, String roomId,
			Map<String, Object> sharedModelParams) {
		String firstCommand = requests.isEmpty() ? null : String.valueOf(requests.get(0).get("command"));
		Room room = RoomUtils.createRoomIfNotExists(roomId, this.insight, engine, firstCommand);
		// tools shared across all requests; mirrors Room.appendToolsToParams
		int maxLength = MCPUtility.getMaxToolNameLength(engine);
		List<Map<String, Object>> tools = room.getAllToolsJsonForRoom(maxLength);
		String systemPrompt = room.getSystemPromptForModel();
		for (Map<String, Object> req : requests) {
			Object commandObj = req.get("command");
			if (commandObj == null) {
				continue;
			}
			InputMessage.Builder builder = InputMessage.builder(room)
					.withSystemPrompt(systemPrompt)
					.withText(commandObj.toString())
					.withModelType(engine.getModelType())
					.withParamMap(new HashMap<>(sharedModelParams));
			attachMedia(builder, req, room);
			String messageJson = RoomMessageStore.messageHistoryWithNewMessage(room, builder.build());
			req.put("message_json", messageJson);
			if (tools != null && !tools.isEmpty()) {
				req.put("tools", tools);
			}
		}
	}

	/**
	 * When no roomId is supplied but one or more requests carry an "image"/"url"
	 * attachment, build a one-shot message per request so the image is actually
	 * sent to the provider. A single ephemeral room (not the insight's real
	 * conversation room) is created purely to host the file copies the media
	 * pipeline requires; it has no prior history, so each request's message_json
	 * is just that request's own text + media, independent of the others.
	 */
	private void enrichWithMedia(List<Map<String, Object>> requests, IModelEngine engine,
			Map<String, Object> sharedModelParams) {
		String firstCommand = requests.isEmpty() ? null : String.valueOf(requests.get(0).get("command"));
		String ephemeralRoomId = "batchmedia_" + GUID.v7().toUUID();
		Room room = RoomUtils.createRoomForStatelessAsk(ephemeralRoomId, this.insight, engine, firstCommand);
		for (Map<String, Object> req : requests) {
			if (!hasMedia(req)) {
				continue;
			}
			Object commandObj = req.get("command");
			if (commandObj == null) {
				continue;
			}
			InputMessage.Builder builder = InputMessage.builder(room)
					.withSystemPrompt((String) req.get("context"))
					.withText(commandObj.toString())
					.withModelType(engine.getModelType())
					.withParamMap(new HashMap<>(sharedModelParams));
			attachMedia(builder, req, room);
			req.put("message_json", RoomMessageStore.messageHistoryWithNewMessage(room, builder.build()));
		}
	}

	private boolean requestsHaveMedia(List<Map<String, Object>> requests) {
		for (Map<String, Object> req : requests) {
			if (hasMedia(req)) {
				return true;
			}
		}
		return false;
	}

	private boolean hasMedia(Map<String, Object> req) {
		return req.containsKey(ReactorKeysEnum.IMAGE.getKey()) || req.containsKey(ReactorKeysEnum.URL.getKey());
	}

	/**
	 * Pulls "image" (insight-relative filenames or base64 data URIs) and "url"
	 * (remote image URLs) off the request map and attaches them to the message
	 * builder, mirroring LLMReactor's image handling. The keys are removed from
	 * the request afterward so they never leak into the provider-native body as
	 * stray params.
	 */
	private void attachMedia(InputMessage.Builder builder, Map<String, Object> req, Room room) {
		List<String> images = toStringList(req.remove(ReactorKeysEnum.IMAGE.getKey()));
		List<String> urls = toStringList(req.remove(ReactorKeysEnum.URL.getKey()));
		if (!images.isEmpty()) {
			List<String> copiedImages = RoomUtils.copyFilesToRoomFolder(images, room, this.insight);
			builder.withMediaInputs(copiedImages, room);
		}
		if (!urls.isEmpty()) {
			builder.withMediaUrls(urls);
		}
	}

	@SuppressWarnings("unchecked")
	private List<String> toStringList(Object val) {
		List<String> out = new ArrayList<>();
		if (val == null) {
			return out;
		}
		if (val instanceof List) {
			for (Object elem : (List<Object>) val) {
				if (elem != null) {
					out.add(elem.toString());
				}
			}
		} else {
			out.add(val.toString());
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	private void addRequest(List<Map<String, Object>> out, Object val, Map<String, Object> sharedModelParams) {
		if (val == null) {
			return;
		}
		if (val instanceof Map) {
			out.add(normalizeEntry((Map<String, Object>) val, out.size(), sharedModelParams));
		} else if (val instanceof List) {
			for (Object elem : (List<Object>) val) {
				addRequest(out, elem, sharedModelParams);
			}
		} else if (val instanceof String) {
			String s = ((String) val).trim();
			if (s.isEmpty()) {
				return;
			}
			if (s.startsWith("[")) {
				List<Object> parsed = GSON.fromJson(s, new TypeToken<List<Object>>() {}.getType());
				if (parsed != null) {
					for (Object elem : parsed) {
						addRequest(out, elem, sharedModelParams);
					}
				}
			} else if (s.startsWith("{")) {
				Map<String, Object> parsed = GSON.fromJson(s, new TypeToken<Map<String, Object>>() {}.getType());
				if (parsed != null) {
					out.add(normalizeEntry(parsed, out.size(), sharedModelParams));
				}
			} else {
				// plain prompt string
				Map<String, Object> req = new HashMap<>();
				if (sharedModelParams != null) {
					req.putAll(sharedModelParams);
				}
				req.put("command", s);
				req.put("custom_id", "req-" + out.size());
				out.add(req);
			}
		}
	}

	private Map<String, Object> normalizeEntry(Map<String, Object> req, int idx,
			Map<String, Object> sharedModelParams) {
		Map<String, Object> out = new HashMap<>();
		// shared params as defaults; per-request keys win
		if (sharedModelParams != null) {
			out.putAll(sharedModelParams);
		}
		out.putAll(req);
		if (!out.containsKey("custom_id")) {
			out.put("custom_id", "req-" + idx);
		}
		return out;
	}

	@Override
	public String getReactorDescription() {
		return "Submit a batch of LLM prompts to the provider's native batch API. "
				+ "Accepts plain strings or {command, context} maps; returns a batch id to poll for results.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.BATCH_REQUESTS.getKey())) {
			return "List of prompts. Each entry is a plain string, or a map with 'command' (required), "
					+ "'context' (optional system prompt), 'image' (optional insight-relative filename(s) or "
					+ "base64 data URI(s)), and 'url' (optional remote image URL(s)). Shared paramValues "
					+ "(max_tokens, temperature, etc.) are applied to every request as defaults.";
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "Optional room id; when set, every request is seeded with the room's conversation "
					+ "history and MCP tools (one-shot, tools are not executed).";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Shared model parameters (max_tokens, temperature, etc.) applied to every request as defaults. "
					+ "Per-request values take precedence.";
		}
		return super.getDescriptionForKey(key);
	}
}
