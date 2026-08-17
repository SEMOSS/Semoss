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
package prerna.reactor.automation.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Builds the model and MCP context used by Automation Chat.
 */
public final class AutomationGenerationUtils {

	private static final Logger classLogger = LogManager.getLogger(AutomationGenerationUtils.class);
	private static final List<String> AUTOMATION_ENGINE_TYPES =
			Arrays.asList("DATABASE", "MODEL", "VECTOR", "STORAGE", "FUNCTION");

	private AutomationGenerationUtils() {
	}

	/**
	 * Returns the first accessible model engine, or {@code null} when none exists.
	 */
	public static String findFirstModelEngine(User user) {
		try {
			List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(
					user, List.of("MODEL"), null, false, null, null, null, "1", "0", null);
			if (engines != null && !engines.isEmpty()) {
				Object id = engines.get(0).get("database_id");
				return id != null ? String.valueOf(id) : null;
			}
		} catch (Exception e) {
			classLogger.warn("Failed to discover an accessible model engine", e);
		}
		return null;
	}

	/**
	 * Builds room options from MCP-enabled engines, the Automation project catalog, and Room tools.
	 */
	public static Map<String, Object> buildEngineMcpOptions(User user, String systemPrompt, String projectId) {
		List<Map<String, Object>> mcp = new ArrayList<>();
		addAccessibleEngineTools(user, mcp);
		mcp.add(projectTool(Constants.MCP_DATABASE_MAKER, "Database Tools"));
		if (projectId != null && !projectId.isBlank()) {
			mcp.add(projectTool(projectId, "Automation Project Tools"));
		}

		Map<String, Object> roomTool = new HashMap<>();
		roomTool.put("id", MCPUtility.ROOM_MCP_ID);
		roomTool.put("name", MCPUtility.ROOM_MCP_NAME);
		roomTool.put("type", MCPUtility.ROOM_MCP_TYPE);
		roomTool.put("fromRoom", true);
		mcp.add(roomTool);

		Map<String, Object> options = new HashMap<>();
		if (systemPrompt != null && !systemPrompt.isBlank()) {
			options.put("instructions", systemPrompt);
		}
		options.put("mcp", mcp);
		return options;
	}

	private static void addAccessibleEngineTools(User user, List<Map<String, Object>> mcp) {
		try {
			List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(
					user, AUTOMATION_ENGINE_TYPES, null, false, null, null, null, "50", "0", null);
			if (engines == null) {
				return;
			}
			for (Map<String, Object> engine : engines) {
				String id = String.valueOf(engine.getOrDefault("database_id", ""));
				if (id.isBlank()) {
					continue;
				}
				IEngine loaded = Utility.getEngine(id);
				if (loaded == null || !loaded.isMCPEnabled()) {
					continue;
				}
				Map<String, Object> tool = new HashMap<>();
				tool.put("id", id);
				tool.put("name", String.valueOf(engine.getOrDefault("database_name", "")));
				tool.put("type", "ENGINE");
				mcp.add(tool);
			}
		} catch (Exception e) {
			classLogger.warn("Failed to build Automation Chat engine tools", e);
		}
	}

	private static Map<String, Object> projectTool(String id, String name) {
		Map<String, Object> tool = new HashMap<>();
		tool.put("id", id);
		tool.put("name", name);
		tool.put("type", "PROJECT");
		return tool;
	}
}
