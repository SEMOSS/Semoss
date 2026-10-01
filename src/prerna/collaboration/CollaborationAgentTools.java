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
package prerna.collaboration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPUtility;

/**
 * Tools every agent gets in a collaboration room, next to DelegateToPerson and
 * FindPerson: the Microsoft 365 mail, calendar, Teams and OneDrive reactors in
 * collaboration_tools_mcp.json. Each call runs its reactor as the room's user.
 */
public final class CollaborationAgentTools {

	private static final Logger classLogger = LogManager.getLogger(CollaborationAgentTools.class);

	/** Stamped into each tool's _meta so an approved call can be told apart from a room tool. */
	public static final String TOOL_KIND = "semoss_collaboration_tool";

	private static final String RESOURCE = "collaboration_tools_mcp.json";

	private static volatile Map<String, JSONObject> toolsByName;

	private CollaborationAgentTools() {
	}

	/** Whether agents in this room get the collaboration tools. */
	public static boolean appliesTo(Room room) {
		return CollaborationUtils.isCollaborationRoom(room);
	}

	/** Fresh copies of the tool definitions, safe for the caller to change. */
	public static List<Map<String, Object>> definitions() {
		List<Map<String, Object>> tools = new ArrayList<>();
		for (JSONObject tool : tools().values()) {
			tools.add(tool.toMap());
		}
		return tools;
	}

	public static boolean isTool(String toolName) {
		return toolName != null && tools().containsKey(toolName);
	}

	/** Whether a stored pending action's _meta came from one of these tools. */
	public static boolean isToolMeta(Map<String, Object> toolMeta) {
		return toolMeta != null && TOOL_KIND.equals(toolMeta.get("SMSS_TOOL_KIND"));
	}

	/** Runs the tool's reactor with the model's arguments as the room's user. */
	public static ToolExecutionResult execute(String toolName, Map<String, Object> params, Insight insight) {
		JSONObject tool = tools().get(toolName);
		if (tool == null) {
			return ToolExecutionResult.error(null, "Unknown collaboration tool: " + toolName);
		}
		JSONObject meta = tool.getJSONObject("_meta");
		JSONObject inputSchema = tool.optJSONObject("inputSchema");
		JSONObject properties = inputSchema != null && inputSchema.has("properties")
				? inputSchema.getJSONObject("properties")
				: new JSONObject();
		// the pixel call drops names it does not know, so a wrong name (body for
		// message) would run with that value missing; refuse it so the model retries
		List<String> unknown = new ArrayList<>();
		if (params != null) {
			for (String name : params.keySet()) {
				if (!properties.has(name)) {
					unknown.add(name);
				}
			}
		}
		if (!unknown.isEmpty()) {
			String message = toolName + " has no argument " + String.join(", ", unknown) + ". Its arguments are: "
					+ String.join(", ", properties.keySet()) + ". Nothing was run; call it again with those names.";
			return ToolExecutionResult.error(message, message);
		}
		try {
			Object output = MCPUtility.runPixelTool(null, insight, meta.getString(MCPUtility.SMSS_FUNCTION_NAME),
					properties, params);
			return ToolExecutionResult.success(output);
		} catch (RuntimeException e) {
			String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			return ToolExecutionResult.error(message, message);
		}
	}

	private static Map<String, JSONObject> tools() {
		Map<String, JSONObject> loaded = toolsByName;
		if (loaded == null) {
			synchronized (CollaborationAgentTools.class) {
				if (toolsByName == null) {
					toolsByName = load();
				}
				loaded = toolsByName;
			}
		}
		return loaded;
	}

	private static Map<String, JSONObject> load() {
		Map<String, JSONObject> byName = new LinkedHashMap<>();
		try (InputStream in = CollaborationAgentTools.class.getResourceAsStream(RESOURCE)) {
			if (in == null) {
				classLogger.warn("Collaboration tools file {} is missing; rooms get none", RESOURCE);
				return byName;
			}
			JSONArray tools = new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8))
					.getJSONArray("tools");
			for (int i = 0; i < tools.length(); i++) {
				JSONObject tool = tools.getJSONObject(i);
				JSONObject meta = tool.optJSONObject("_meta");
				// a tool without a reactor cannot run
				if (meta == null || meta.optString(MCPUtility.SMSS_FUNCTION_NAME).isBlank()) {
					continue;
				}
				meta.put("SMSS_TOOL_KIND", TOOL_KIND);
				byName.put(tool.getString("name"), tool);
			}
		} catch (IOException | RuntimeException e) {
			classLogger.warn("Could not read collaboration tools file {}", RESOURCE, e);
		}
		return byName;
	}
}
