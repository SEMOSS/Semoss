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
package prerna.reactor.agent.stream;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builders for the canonical agent stream item maps. Construction and
 * null-safety only.
 */
public final class AgentStreamItems {

	public static final String KIND_MESSAGE = "message";
	public static final String KIND_REASONING = "reasoning";
	public static final String KIND_TOOL = "tool";
	public static final String KIND_SUBAGENT = "subagent";

	public static final String TOOL_QUEUED = "QUEUED";
	public static final String TOOL_RUNNING = "RUNNING";
	public static final String TOOL_INPUT_REQUIRED = "INPUT_REQUIRED";
	public static final String TOOL_COMPLETED = "COMPLETED";
	public static final String TOOL_FAILED = "FAILED";
	public static final String TOOL_REJECTED = "REJECTED";
	public static final String TOOL_CANCELLED = "CANCELLED";

	public static final int MAX_TOOL_OUTPUT_CHARS = 12_000;
	public static final int MAX_RESULT_PREVIEW_CHARS = 2_000;

	private AgentStreamItems() {
	}

	public static String messageItemId(String runId, int ordinal) {
		return runId + ":model:" + ordinal + ":message";
	}

	public static String reasoningItemId(String runId, int ordinal) {
		return runId + ":model:" + ordinal + ":reasoning";
	}

	public static Map<String, Object> messageItem(String id, String text, String messageId) {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("id", id);
		item.put("kind", KIND_MESSAGE);
		item.put("role", "assistant");
		item.put("text", text == null ? "" : text);
		if (messageId != null && !messageId.isBlank()) {
			item.put("messageId", messageId);
		}
		return item;
	}

	public static Map<String, Object> reasoningItem(String id, String summary) {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("id", id);
		item.put("kind", KIND_REASONING);
		item.put("summary", summary == null ? "" : summary);
		return item;
	}

	public static Map<String, Object> toolItem(String toolCallId, String name, Map<String, Object> arguments,
			Map<String, Object> metadata, String status) {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("id", toolCallId);
		item.put("kind", KIND_TOOL);
		item.put("name", name);
		item.put("arguments", arguments == null ? new LinkedHashMap<>() : arguments);
		if (metadata != null && !metadata.isEmpty()) {
			item.put("metadata", metadata);
		}
		item.put("status", status);
		return item;
	}

	public static Map<String, Object> subagentItem(String childRunId, String alias, String roomId, String workspaceId,
			String status) {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("id", childRunId);
		item.put("kind", KIND_SUBAGENT);
		item.put("childRunId", childRunId);
		if (alias != null && !alias.isBlank()) {
			item.put("alias", alias);
		}
		item.put("roomId", roomId);
		if (workspaceId != null && !workspaceId.isBlank()) {
			item.put("workspaceId", workspaceId);
		}
		item.put("status", status);
		return item;
	}

	public static String truncate(String value, int maxChars) {
		if (value == null || value.length() <= maxChars) {
			return value;
		}
		return value.substring(0, maxChars) + "\n... [truncated for live stream]";
	}
}
