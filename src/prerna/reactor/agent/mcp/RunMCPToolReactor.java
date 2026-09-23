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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RunMCPToolReactor extends AbstractReactor {

	// Agent HITL keys (optional). When actionId is present, this call is a
	// decision on a paused agent tool call and is delegated to
	// AgentToolDecisionHandler.
	private static final String RUN_ID_KEY = "runId";
	private static final String ROOM_ID_KEY = ReactorKeysEnum.ROOM_ID.getKey();
	private static final String TOOL_CALL_ID_KEY = "toolCallId";
	private static final String PARENT_MESSAGE_ID_KEY = ReactorKeysEnum.PARENT_MESSAGE_ID.getKey();
	private static final String ACTION_ID_KEY = "actionId";
	private static final String DECISION_KEY = "decision";

	// we should possibly remove the function and param values map
	public RunMCPToolReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() + "," + ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.MCP_TOOL_RESULT.getKey(), RUN_ID_KEY, ROOM_ID_KEY, TOOL_CALL_ID_KEY,
				PARENT_MESSAGE_ID_KEY, ReactorKeysEnum.MCP_TOOL_STATUS.getKey(), ACTION_ID_KEY, DECISION_KEY };
		// function and paramValues are optional: in the agent HITL path the tool
		// name and args are resolved from the AGENT_RUN_ACTION row via actionId.
		// toolName is still validated at runtime before execution.
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// Agent HITL: a decision on a paused tool call. The AGENT_RUN_ACTION row is
		// the source of truth; the caller only needs actionId (+ decision + edited
		// paramValues). Any other context keys are validated against the row.
		String actionId = this.keyValue.get(ACTION_ID_KEY);
		if (actionId != null && !actionId.trim().isEmpty()) {
			Map<String, String> callerContext = new HashMap<>();
			callerContext.put(AgentToolDecisionHandler.CTX_RUN_ID, this.keyValue.get(RUN_ID_KEY));
			callerContext.put(AgentToolDecisionHandler.CTX_ROOM_ID, this.keyValue.get(ROOM_ID_KEY));
			callerContext.put(AgentToolDecisionHandler.CTX_TOOL_CALL_ID, this.keyValue.get(TOOL_CALL_ID_KEY));
			callerContext.put(AgentToolDecisionHandler.CTX_PARENT_MESSAGE_ID, this.keyValue.get(PARENT_MESSAGE_ID_KEY));
			String result = new AgentToolDecisionHandler(this.insight).handleDecision(actionId,
					this.keyValue.get(DECISION_KEY), this.keyValue.get(ReactorKeysEnum.MCP_TOOL_RESULT.getKey()),
					this.keyValue.get(ReactorKeysEnum.MCP_TOOL_STATUS.getKey()), getMap(), callerContext);
			return new NounMetadata(result, PixelDataType.CONST_STRING, PixelOperationType.MCP_TOOL_EXECUTION);
		}

		// the MCP may just be returning the fully executed tool. Return that to the
		// playground. Otherwise execute the tool
		// TODO: this logic should be shifted such that the MCP FE app directly calls
		// AddPlaygroundToolExecution
		String toolExecutionResult = this.keyValue.get(this.keysToGet[3]);
		if (toolExecutionResult != null && !toolExecutionResult.trim().isEmpty()) {
			return new NounMetadata(toolExecutionResult, PixelDataType.CONST_STRING,
					PixelOperationType.MCP_TOOL_EXECUTION);
		}

		String engineId = resolveContextEngineId(this.keyValue.get(this.keysToGet[0].split(",")[0]));

		String toolName = this.keyValue.get(this.keysToGet[1]);
		String roomId = this.keyValue.get(ROOM_ID_KEY);
		if (roomId != null && !roomId.isBlank()) {
			Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
			if (MCPUtility.ROOM_MCP_ID.equals(engineId)) {
				this.insight.setRoomForInsight(room);
			}
			// The caller sends back the aliased name the model was given. The room
			// still holds the map that produced that alias, so undo it here rather
			// than leaving the resolver to match a possibly truncated name.
			toolName = room.resolveOriginalToolName(toolName);
		}

		// these are the params
		Map<String, Object> paramMap = getMap();

		return new NounMetadata(MCPUtility.executeTool(engineId, toolName, paramMap, this.insight),
				PixelDataType.MCP_TOOL_EXECUTION, PixelOperationType.MCP_TOOL_EXECUTION);
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
		return "Execute a tool defined in an engine or project/app. When actionId is provided, the call is a HITL decision: the result is written to the agent room and the agent run is resumed.";
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
		} else if (key.equals(RUN_ID_KEY)) {
			return "Agent run id. Optional; validated against the AGENT_RUN_ACTION row when provided.";
		} else if (key.equals(ROOM_ID_KEY)) {
			return "Agent room id. Optional; validated against the AGENT_RUN_ACTION row when provided.";
		} else if (key.equals(TOOL_CALL_ID_KEY)) {
			return "The tool call id from the agent's paused tool-call batch. Optional; validated against the AGENT_RUN_ACTION row when provided.";
		} else if (key.equals(PARENT_MESSAGE_ID_KEY)) {
			return "The assistant message id that contains the tool-call batch. Optional; validated against the AGENT_RUN_ACTION row when provided.";
		} else if (key.equals(ReactorKeysEnum.MCP_TOOL_STATUS.getKey())) {
			return "Tool execution status: success, error, or cancelled. Defaults to success (or error for passthrough results).";
		} else if (key.equals(ACTION_ID_KEY)) {
			return "The AGENT_RUN_ACTION id for this pending tool call. Used to record the user's decision.";
		} else if (key.equals(DECISION_KEY)) {
			return "The user's decision: approve, edit, reject, or respond.";
		}
		return super.getDescriptionForKey(key);
	}

}
