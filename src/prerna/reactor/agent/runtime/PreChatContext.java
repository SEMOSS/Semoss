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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Runs one toolbox tool when a room is set up, before its first message, and
 * stores the result as a hidden note the model reads but the UI does not show.
 * <p>
 * Two surfaces use it:
 * <ul>
 * <li>the {@code semoss} agent harness, when the deployment replaces the default
 * agent tools ({@code AGENT_DEFAULT_TOOLS_MCP_ID}) and names a pre-chat tool
 * ({@code AGENT_DEFAULT_TOOLS_PRE_CHAT_TOOL});</li>
 * <li>Playground chat, from {@code playground.preChatTool} in the active theme
 * map.</li>
 * </ul>
 * The tool runs through {@link MCPUtility#executeTool}, so the caller's access to
 * the toolbox is checked and the toolbox's own {@code SMSS_FUNCTION_NAME} mapping
 * applies. A tool that is missing, {@code disabled} or {@code ask} is skipped:
 * nothing is shown to approve before the first message. Every failure is logged
 * and skipped, so the chat carries on as it would without the feature, and the
 * model can still call the tool itself.
 */
public final class PreChatContext {

	private static final Logger logger = LogManager.getLogger(PreChatContext.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	/** Result characters kept when the configuration does not set a limit. */
	public static final int DEFAULT_MAX_CHARS = 60_000;

	private PreChatContext() {
	}

	/**
	 * Which tool to run and with what arguments.
	 *
	 * @param toolboxId engine or project id of the toolbox (MCP)
	 * @param tool      tool name as listed by the toolbox
	 * @param args      tool arguments, never {@code null}
	 * @param maxChars  result characters kept in the hidden note
	 */
	public record Spec(String toolboxId, String tool, Map<String, Object> args, int maxChars) {

		public Spec {
			args = args == null ? Collections.emptyMap() : Collections.unmodifiableMap(new LinkedHashMap<>(args));
			maxChars = maxChars > 0 ? maxChars : DEFAULT_MAX_CHARS;
		}

		/**
		 * Builds a spec, or returns {@code null} when the toolbox or tool is blank.
		 */
		public static Spec of(String toolboxId, String tool, Map<String, Object> args, Integer maxChars) {
			toolboxId = trimToNull(toolboxId);
			tool = trimToNull(tool);
			if (toolboxId == null || tool == null) {
				return null;
			}
			return new Spec(toolboxId, tool, args, maxChars == null ? 0 : maxChars);
		}
	}

	/**
	 * Runs the tool and stores its result when the room has no messages yet.
	 *
	 * @param room        room about to receive its first message
	 * @param insight     caller insight; must be unbound or bound to {@code room}
	 * @param modelEngine model the first message will be sent to
	 * @param spec        tool to run; {@code null} does nothing
	 * @return the hidden pair that was stored, or an empty list
	 */
	public static List<AbstractMessage> injectIfFirstMessage(Room room, Insight insight, IModelEngine modelEngine,
			Spec spec) {
		if (spec == null || room == null || insight == null || modelEngine == null
				|| !room.getMessages().isEmpty()) {
			return new ArrayList<>();
		}
		try {
			// Room-scoped tools read the room from the insight. In chat mode the client
			// binds it with SetRoomForInsight; bind it here if it has not, and never run
			// against a different room.
			String boundRoomId = trimToNull(insight.getRoomId());
			if (boundRoomId == null) {
				insight.setRoomForInsight(room);
			} else if (!boundRoomId.equals(room.getId())) {
				logger.warn("PreChatContext: skipped {} for room={}: insight is bound to room={}", spec.tool(),
						room.getId(), boundRoomId);
				return new ArrayList<>();
			}

			JSONObject tool = PlatformAgentTools.findMcpTool(spec.toolboxId(), spec.tool());
			if (tool == null) {
				logger.warn("PreChatContext: skipped room={}: tool {} is missing or disabled in toolbox {}",
						room.getId(), spec.tool(), spec.toolboxId());
				return new ArrayList<>();
			}
			JSONObject meta = tool.optJSONObject("_meta");
			if (meta != null && MCPExecution.ASK.getValue().equalsIgnoreCase(meta.optString(MCPUtility.SMSS_MCP_EXECUTION))) {
				logger.warn("PreChatContext: skipped room={}: tool {} needs user approval", room.getId(), spec.tool());
				return new ArrayList<>();
			}
			String functionName = meta != null ? meta.optString(MCPUtility.SMSS_FUNCTION_NAME, spec.tool())
					: spec.tool();

			long start = System.currentTimeMillis();
			Object raw = MCPUtility.executeTool(spec.toolboxId(), functionName, new LinkedHashMap<>(spec.args()),
					insight);
			String output = stringify(raw);
			if (output == null || output.isBlank() || output.startsWith("Error:")) {
				logger.warn("PreChatContext: skipped room={}: tool {} returned no usable result", room.getId(),
						spec.tool());
				return new ArrayList<>();
			}

			List<AbstractMessage> added = room.appendPreChatContext(note(spec.tool(), output, spec.maxChars()),
					spec.tool(), modelEngine, insight);
			logger.info("PreChatContext: room={} tool={} chars={} stored={} ms={}", room.getId(), spec.tool(),
					output.length(), !added.isEmpty(), System.currentTimeMillis() - start);
			return added;
		} catch (Exception e) {
			logger.warn("PreChatContext: skipped {} for room={}: {}", spec.tool(), room.getId(), e.getMessage(), e);
			return new ArrayList<>();
		}
	}

	/**
	 * Wraps a tool result for the model, truncating it to {@code maxChars}.
	 */
	static String note(String toolName, String output, int maxChars) {
		boolean truncated = output.length() > maxChars;
		String body = truncated ? output.substring(0, maxChars) : output;
		StringBuilder sb = new StringBuilder();
		sb.append("<pre_chat_context tool=\"").append(xmlEscape(toolName)).append("\">\n");
		sb.append(body);
		if (truncated) {
			sb.append("\n[truncated: ").append(output.length() - maxChars).append(" more characters]");
		}
		sb.append("\n</pre_chat_context>\n\n");
		sb.append("""
				The platform ran %s when this chat started, before the user's first message; its result is above. \
				Treat it as if you had called the tool yourself. Don't call %s again unless the user adds or \
				mentions new material, asks what is available, or you need something this result doesn't cover.\
				""".formatted(toolName, toolName));
		if (truncated) {
			sb.append(" The result was truncated, so call ").append(toolName)
					.append(" if you need the part that was cut.");
		}
		sb.append(" Do not mention this note to the user.");
		return sb.toString();
	}

	static String stringify(Object raw) {
		if (raw instanceof NounMetadata noun) {
			raw = noun.getValue();
		}
		if (raw == null) {
			return null;
		}
		if (raw instanceof String s) {
			return s;
		}
		return GSON.toJson(raw);
	}

	/**
	 * Parses a JSON object of tool arguments; blank or invalid input yields an
	 * empty map.
	 */
	static Map<String, Object> parseArgs(String json) {
		json = trimToNull(json);
		if (json == null) {
			return Collections.emptyMap();
		}
		try {
			return new JSONObject(json).toMap();
		} catch (Exception e) {
			logger.warn("PreChatContext: ignoring invalid tool arguments: {}", e.getMessage());
			return Collections.emptyMap();
		}
	}

	static Integer parseInt(String value) {
		value = trimToNull(value);
		if (value == null) {
			return null;
		}
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static String trimToNull(String s) {
		if (s == null) {
			return null;
		}
		s = s.trim();
		return s.isEmpty() ? null : s;
	}

	private static String xmlEscape(String s) {
		return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
	}
}
