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
package prerna.reactor.agent.runtime;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.om.ThreadStore;
import prerna.sablecc2.comm.PixelJobManager;

final class SemossAgentStream {

	private static final String STREAM_TYPE_CONTENT = "content";
	static final String ASSISTANT_CONTENT_EVENT_ID = "semoss-stream-content";
	private static final String STATUS_SUCCESS = "success";
	private static final String STATUS_ERROR = "error";

	private SemossAgentStream() {
	}

	static void userPrompt(String roomId, String text) {
		Map<String, Object> data = base("user-prompt");
		data.put("promptId", "semoss-user-" + roomId + "-" + System.nanoTime());
		data.put("text", text != null ? text : "");
		emit(data);
	}

	static void assistantText(String eventId, String text) {
		if (text == null || text.isBlank()) {
			return;
		}
		Map<String, Object> data = base("assistant-text");
		data.put("eventId", eventId != null ? eventId : "semoss-assistant-" + System.nanoTime());
		data.put("text", text);
		data.put("isPartial", false);
		emit(data);
	}

	static void toolInvocation(String toolCallId, String toolName, String description) {
		Map<String, Object> data = base("tool-invocation");
		data.put("toolUseId", valueOrEmpty(toolCallId));
		data.put("eventId", "semoss-tool-invocation-" + valueOrEmpty(toolCallId));
		data.put("toolName", valueOrEmpty(toolName));
		if (description != null && !description.isBlank()) {
			data.put("description", description);
		}
		emit(data);
	}

	static void toolResult(String toolCallId, String toolName, boolean success, long durationMs, String content) {
		Map<String, Object> data = base("tool-result");
		data.put("toolUseId", valueOrEmpty(toolCallId));
		data.put("eventId", "semoss-tool-result-" + valueOrEmpty(toolCallId));
		data.put("toolName", valueOrEmpty(toolName));
		data.put("status", success ? STATUS_SUCCESS : STATUS_ERROR);
		data.put("durationMs", durationMs);
		if (content != null && !content.isBlank()) {
			data.put("content", content);
		}
		emit(data);
	}

	static void agentResult(String roomId, int iterations, int reflectionsUsed, int toolCount) {
		Map<String, Object> data = base("agent-result");
		data.put("uuid", "semoss-result-" + roomId + "-" + System.nanoTime());
		data.put("sessionId", roomId);
		data.put("numTurns", iterations);
		data.put("reflectionsUsed", reflectionsUsed);
		data.put("toolCount", toolCount);
		emit(data);
	}

	private static Map<String, Object> base(String kind) {
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("kind", kind);
		data.put("timestamp", Instant.now().toString());
		return data;
	}

	private static void emit(Map<String, Object> data) {
		String jobId = ThreadStore.getJobId();
		if (jobId == null || jobId.isBlank()) {
			return;
		}

		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", STREAM_TYPE_CONTENT);
		envelope.put("data", data);
		PixelJobManager.getManager().addStreamOut(jobId, envelope);
	}

	private static String valueOrEmpty(String value) {
		return value != null ? value : "";
	}
}
