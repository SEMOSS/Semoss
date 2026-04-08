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
package prerna.reactor.agent.mcp;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.engine.impl.MCPFactory;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunMCPToolReactor extends AbstractReactor {

	// we should possibly remove the function and param values map
	public RunMCPToolReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() + "," + ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.MCP_TOOL_RESULT.getKey() };
		this.keyRequired = new int[] { 0, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// the MCP may just be returning the fully executed tool. Return that to the
		// playground. Otherwise execute the tool
		// TODO: this logic should be shifted such that the MCP FE app directly calls
		// AddPlaygroundToolExecution
		String toolExecutionResult = this.keyValue.get(this.keysToGet[3]);
		if (toolExecutionResult != null && !toolExecutionResult.trim().isEmpty()) {
			return new NounMetadata(toolExecutionResult, PixelDataType.CONST_STRING,
					PixelOperationType.MCP_TOOL_EXECUTION);
		}

		String engineId = this.keyValue.get(this.keysToGet[0].split(",")[0]);
		if (engineId == null || engineId.isEmpty()) {
			engineId = insight.getContextProjectId();
			if (engineId == null || engineId.isEmpty()) {
				engineId = insight.getProjectId();
			}
		}
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the project id or set the app context");
		}

		String toolName = this.keyValue.get(this.keysToGet[1]);
		if (toolName == null || (toolName = toolName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Tool name must be passed in to execute the mcp tool");
		}

		// Internal memory tools — dispatch without an MCP engine
		if (Room.MEMORY_TOOL_PROJECT_ID.equals(engineId)) {
			return executeMemoryTool(toolName, getMap());
		}

		IEngine engine = null;
		try {
			engine = Utility.getEngine(engineId);
		} catch (Exception ex) {
			// ignore
		}
		if (engine == null) {
			engine = Utility.getProject(engineId);
		}
		User user = this.insight.getUser();
		checkEngineEditSecurity(engine, user);

		toolName = MCPUtility.removeEngineIdFromToolsMethodName(engineId, toolName);

		// these are the params
		Map<String, Object> paramMap = getMap();

		IMCP mcp = MCPFactory.build(engine);
		return new NounMetadata(mcp.callTool(toolName, paramMap, this.insight), PixelDataType.MCP_TOOL_EXECUTION,
				PixelOperationType.MCP_TOOL_EXECUTION);
	}

	private static final List<String> VALID_MEMORY_TYPES = Arrays.asList("FACT", "PREFERENCE", "EPISODE", "SUMMARY");

	private static final String STORE_MEMORY_TOOL = "store_memory";
	private static final String RECALL_MEMORY_TOOL = "recall_memory";

	/**
	 * Dispatches a memory tool call to the appropriate handler.
	 *
	 * @param toolName the memory tool name ({@code store_memory} or {@code recall_memory})
	 * @param paramMap the tool parameters from the LLM
	 * @return tool execution result as a {@link NounMetadata} string
	 */
	private NounMetadata executeMemoryTool(String toolName, Map<String, Object> paramMap) {
		String userId = getAuthenticatedUserId();

		switch (toolName) {
		case STORE_MEMORY_TOOL:
			return handleStoreMemory(userId, paramMap);
		case RECALL_MEMORY_TOOL:
			return handleRecallMemory(userId, paramMap);
		default:
			throw new IllegalArgumentException("Unknown memory tool: " + toolName);
		}
	}

	/**
	 * Extracts and validates the authenticated user ID from the current insight.
	 *
	 * @return the user's primary login token ID
	 * @throws IllegalArgumentException if the user is not authenticated
	 */
	private String getAuthenticatedUserId() {
		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User must be authenticated to use memory tools");
		}
		return user.getPrimaryLoginToken().getId();
	}

	/**
	 * Handles the {@code store_memory} tool — persists a new memory for the user.
	 *
	 * @param userId   authenticated user identifier
	 * @param paramMap tool parameters containing {@code content} and optional {@code memoryType}
	 * @return confirmation message
	 */
	private NounMetadata handleStoreMemory(String userId, Map<String, Object> paramMap) {
		String content = paramMap != null ? (String) paramMap.get("content") : null;
		if (content == null || content.trim().isEmpty()) {
			return toolResult("Error: content is required");
		}

		String memoryType = paramMap != null ? (String) paramMap.getOrDefault("memoryType", "FACT") : "FACT";
		if (!VALID_MEMORY_TYPES.contains(memoryType.toUpperCase())) {
			memoryType = "FACT";
		}

		String memoryId = java.util.UUID.randomUUID().toString();
		String metadata = GSON.toJson(Map.of("source", "tool_call"));
		ModelInferenceLogsUtils.insertMemory(memoryId, userId, null, memoryType.toUpperCase(), content.trim(), metadata);
		return toolResult("Memory stored: " + content.trim());
	}

	/**
	 * Handles the {@code recall_memory} tool — retrieves stored memories for the user.
	 *
	 * @param userId   authenticated user identifier
	 * @param paramMap tool parameters containing optional {@code memoryType} filter
	 * @return formatted list of matching memories, or a "no memories found" message
	 */
	private NounMetadata handleRecallMemory(String userId, Map<String, Object> paramMap) {
		String memoryType = paramMap != null ? (String) paramMap.get("memoryType") : null;
		int limit = 10;

		List<Map<String, Object>> memories = ModelInferenceLogsUtils
				.getMemoriesForUser(userId, memoryType, limit, 0);
		if (memories == null || memories.isEmpty()) {
			return toolResult("No memories found.");
		}

		StringBuilder sb = new StringBuilder();
		for (Map<String, Object> mem : memories) {
			String type = (String) mem.get("memory_type");
			String content = (String) mem.get("content");
			if (content != null) {
				sb.append("- [").append(type != null ? type : "FACT").append("] ").append(content).append("\n");
			}
		}
		return toolResult(sb.toString());
	}

	/**
	 * Creates a tool execution result {@link NounMetadata}.
	 */
	private static NounMetadata toolResult(String message) {
		return new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.MCP_TOOL_EXECUTION);
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Execute a tool defined in an engine or project/app";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey()) || key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the engine or project/app";
		} else if (key.equals(ReactorKeysEnum.FUNCTION.getKey())) {
			return "The name of the tool/function to execute";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "A key-value pair map containing the parameter inputs for the tool/function";
		} else if (key.equals(ReactorKeysEnum.MCP_TOOL_RESULT.getKey())) {
			return "If this key is present, its value will be returned as the result of the MCP tool execution, and the tool will not be executed.";
		}
		return super.getDescriptionForKey(key);
	}

}
