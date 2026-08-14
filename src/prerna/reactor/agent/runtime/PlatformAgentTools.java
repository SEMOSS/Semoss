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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.IEngine;
import prerna.engine.api.ToolExecutionResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.reactor.agent.mcp.RunMCPToolReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

final class PlatformAgentTools {

	static final String PARAM_USE_DEFAULT_AGENT_TOOLS = "useDefaultAgentTools";
	private static final String PARAM_TOOLS = "tools";
	private static final String PROP_DEFAULT_TOOLS_MCP_ID = "AGENT_DEFAULT_TOOLS_MCP_ID";
	private static final String PROP_DEFAULT_TOOLS_MCP_PROJECT_ID = "AGENT_DEFAULT_TOOLS_MCP_PROJECT_ID";

	private static final Map<String, PlatformAgentToolHandlers.ToolHandler> PLATFORM_TOOLS =
			PlatformAgentToolHandlers.handlersByName();

	private PlatformAgentTools() {
	}

	static List<Map<String, Object>> resolveDefaultTools(Map<String, Object> paramMap) {
		List<Map<String, Object>> tools = new ArrayList<>();
		if (useDefaultAgentTools(paramMap)) {
			String overrideMcpId = getDefaultToolsMcpId();
			if (overrideMcpId != null) {
				tools.addAll(getMcpToolDefinitions(overrideMcpId));
			} else {
				tools.addAll(getPlatformToolDefinitions());
			}
		}
		tools.addAll(getExplicitTools(paramMap));
		return dedupeByName(tools);
	}

	static boolean isDefaultTool(String toolName) {
		if (toolName == null || toolName.trim().isEmpty()) {
			return false;
		}
		if (getDefaultToolsMcpId() != null) {
			return findMcpTool(getDefaultToolsMcpId(), toolName) != null;
		}
		return PLATFORM_TOOLS.containsKey(toolName);
	}

	static String executeDefaultTool(String toolName, Map<String, Object> params, AgentRunContext ctx) throws Exception {
		String overrideMcpId = getDefaultToolsMcpId();
		if (overrideMcpId != null) {
			JSONObject tool = findMcpTool(overrideMcpId, toolName);
			if (tool == null) {
				throw new IllegalArgumentException("Unknown default MCP tool: " + toolName);
			}
			JSONObject meta = tool.optJSONObject("_meta");
			String functionName = meta != null ? meta.optString(MCPUtility.SMSS_FUNCTION_NAME, toolName) : toolName;
			return callMcpTool(overrideMcpId, functionName, params, ctx);
		}

		PlatformAgentToolHandlers.ToolHandler handler = PLATFORM_TOOLS.get(toolName);
		if (handler == null) {
			throw new IllegalArgumentException("Unknown platform agent tool: " + toolName);
		}
		return handler.execute(params, ctx);
	}

	static ToolExecutionResult executeDefaultToolResult(String toolName, Map<String, Object> params,
			AgentRunContext ctx) throws Exception {
		String output = executeDefaultTool(toolName, params, ctx);
		if (getDefaultToolsMcpId() == null && output != null && output.startsWith("Error:")) {
			return ToolExecutionResult.error(output, output);
		}
		return ToolExecutionResult.success(output);
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> getExplicitTools(Map<String, Object> paramMap) {
		if (paramMap == null) {
			return new ArrayList<>();
		}
		Object explicit = paramMap.get(PARAM_TOOLS);
		if (explicit instanceof List<?>) {
			return new ArrayList<>((List<Map<String, Object>>) explicit);
		}
		return new ArrayList<>();
	}

	private static boolean useDefaultAgentTools(Map<String, Object> paramMap) {
		Object value = paramMap != null ? paramMap.get(PARAM_USE_DEFAULT_AGENT_TOOLS) : null;
		if (value == null) {
			return true;
		}
		String normalized = value.toString().trim().toLowerCase(java.util.Locale.ROOT);
		return !("false".equals(normalized) || "0".equals(normalized) || "no".equals(normalized));
	}

	private static String getDefaultToolsMcpId() {
		String id = Utility.getDIHelperProperty(PROP_DEFAULT_TOOLS_MCP_ID);
		if (id == null || id.trim().isEmpty()) {
			id = Utility.getDIHelperProperty(PROP_DEFAULT_TOOLS_MCP_PROJECT_ID);
		}
		return id != null && !id.trim().isEmpty() ? id.trim() : null;
	}

	private static List<Map<String, Object>> getPlatformToolDefinitions() {
		List<Map<String, Object>> tools = new ArrayList<>();
		for (PlatformAgentToolHandlers.ToolHandler handler : PLATFORM_TOOLS.values()) {
			tools.add(handler.asToolDefinition().toMap());
		}
		return tools;
	}

	private static List<Map<String, Object>> getMcpToolDefinitions(String mcpId) {
		JSONObject toolMap = getMcpToolsJson(mcpId);
		JSONArray arr = toolMap != null ? toolMap.optJSONArray(PARAM_TOOLS) : null;
		List<Map<String, Object>> tools = new ArrayList<>();
		if (arr == null) {
			return tools;
		}
		for (int i = 0; i < arr.length(); i++) {
			JSONObject tool = arr.optJSONObject(i);
			if (tool == null || isDisabled(tool)) {
				continue;
			}
			tools.add(tool.toMap());
		}
		return tools;
	}

	private static JSONObject findMcpTool(String mcpId, String toolName) {
		JSONObject toolMap = getMcpToolsJson(mcpId);
		JSONArray arr = toolMap != null ? toolMap.optJSONArray(PARAM_TOOLS) : null;
		if (arr == null) {
			return null;
		}
		for (int i = 0; i < arr.length(); i++) {
			JSONObject tool = arr.optJSONObject(i);
			if (tool != null && toolName.equals(tool.optString("name")) && !isDisabled(tool)) {
				return tool;
			}
		}
		return null;
	}

	private static JSONObject getMcpToolsJson(String mcpId) {
		IEngine engine = null;
		try {
			engine = Utility.getEngine(mcpId);
		} catch (Exception ex) {
			// fall through to project lookup
		}
		if (engine == null) {
			engine = Utility.getProject(mcpId);
		}
		if (engine == null) {
			throw new IllegalArgumentException("Invalid default agent tools MCP id: " + mcpId);
		}
		return MCPUtility.getAggregatedTools(engine);
	}

	private static boolean isDisabled(JSONObject tool) {
		JSONObject meta = tool.optJSONObject("_meta");
		Object executionValue = meta != null ? meta.opt(MCPUtility.SMSS_MCP_EXECUTION) : null;
		return MCPExecution.DISABLED.getValue().equals(executionValue);
	}

	private static List<Map<String, Object>> dedupeByName(List<Map<String, Object>> tools) {
		Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
		for (Map<String, Object> tool : tools) {
			if (tool == null) {
				continue;
			}
			Object name = tool.get("name");
			if (name != null && !name.toString().trim().isEmpty()) {
				byName.putIfAbsent(name.toString(), tool);
			}
		}
		return new ArrayList<>(byName.values());
	}

	private static String callMcpTool(String mcpId, String toolName, Map<String, Object> params, AgentRunContext ctx) {
		RunMCPToolReactor reactor = new RunMCPToolReactor();
		reactor.In();
		reactor.setInsight(ctx.getInsight());

		GenRowStruct engineGrs = new GenRowStruct();
		engineGrs.add(new NounMetadata(mcpId, PixelDataType.CONST_STRING));
		reactor.getNounStore().addNoun(ReactorKeysEnum.ENGINE.getKey(), engineGrs);

		GenRowStruct functionGrs = new GenRowStruct();
		functionGrs.add(new NounMetadata(toolName, PixelDataType.CONST_STRING));
		reactor.getNounStore().addNoun(ReactorKeysEnum.FUNCTION.getKey(), functionGrs);

		GenRowStruct paramGrs = new GenRowStruct();
		paramGrs.add(new NounMetadata(params != null ? params : java.util.Collections.emptyMap(), PixelDataType.MAP));
		reactor.getNounStore().addNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), paramGrs);

		NounMetadata result = reactor.execute();
		return result != null && result.getValue() != null ? result.getValue().toString() : "";
	}
}
