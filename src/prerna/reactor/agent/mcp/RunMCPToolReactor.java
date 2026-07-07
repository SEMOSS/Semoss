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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IMCP;
import prerna.engine.impl.MCPFactory;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunActionStore;
import prerna.reactor.agent.run.AgentRunEventBus;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunMCPToolReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(RunMCPToolReactor.class);
	private static final Gson GSON = new Gson();

	// Agent-context keys (optional). When all three are present, the tool result
	// is written to the room via addToolExecutionResult and the agent run is resumed.
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
		this.keyRequired = new int[] { 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// the MCP may just be returning the fully executed tool. Return that to the
		// playground. Otherwise execute the tool
		// TODO: this logic should be shifted such that the MCP FE app directly calls
		// AddPlaygroundToolExecution
		String toolExecutionResult = this.keyValue.get(this.keysToGet[3]);
		boolean hasPassthroughResult = toolExecutionResult != null && !toolExecutionResult.trim().isEmpty();

		// Agent-context params (optional). When runId + roomId + toolCallId are
		// present, this call originated from an agent run that was paused on an
		// SMSS_MCP_EXECUTION=ask tool. After getting the tool result (either by
		// executing or from the passthrough), we write it to the room via
		// addToolExecutionResult and resume the agent run.
		String runId = this.keyValue.get(RUN_ID_KEY);
		String agentRoomId = this.keyValue.get(ROOM_ID_KEY);
		String toolCallId = this.keyValue.get(TOOL_CALL_ID_KEY);
		String parentMessageId = this.keyValue.get(PARENT_MESSAGE_ID_KEY);
		String toolStatus = this.keyValue.get(ReactorKeysEnum.MCP_TOOL_STATUS.getKey());
		String actionId = this.keyValue.get(ACTION_ID_KEY);
		String decision = this.keyValue.get(DECISION_KEY);
		boolean hasAgentContext = runId != null && !runId.trim().isEmpty()
				&& agentRoomId != null && !agentRoomId.trim().isEmpty()
				&& toolCallId != null && !toolCallId.trim().isEmpty();
		String userId = this.insight != null ? this.insight.getUserId() : null;
		Map<String, Object> pendingAction = null;
		if (hasAgentContext) {
			pendingAction = loadAndValidatePendingAction(actionId, runId, agentRoomId, parentMessageId, toolCallId,
					userId);
		}

		if (hasPassthroughResult) {
			// Passthrough: the caller provided the result directly (reject/respond).
			// If agent context is present, write to room + resume before returning.
			if (hasAgentContext) {
				writeToRoomAndResume(runId, agentRoomId, toolCallId, parentMessageId, toolExecutionResult,
						toolStatus != null ? toolStatus : "error", actionId, decision,
						resolveToolParamsForDecision(pendingAction, getMap(), decision), pendingAction, userId);
			}
			return new NounMetadata(toolExecutionResult, PixelDataType.CONST_STRING,
					PixelOperationType.MCP_TOOL_EXECUTION);
		}

		String engineId = engineIdFromPendingAction(pendingAction);
		if (engineId == null) {
			engineId = this.keyValue.get(this.keysToGet[0].split(",")[0]);
		}
		if (engineId == null || engineId.isEmpty()) {
			engineId = insight.getContextProjectId();
			if (engineId == null || engineId.isEmpty()) {
				engineId = insight.getProjectId();
			}
		}
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the project id or set the app context");
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

		String toolName = null;
		if (pendingAction != null) {
			toolName = stringValue(pendingAction.get("toolName"));
		}
		if (toolName == null) {
			toolName = this.keyValue.get(this.keysToGet[1]);
		}
		if (toolName == null || (toolName = toolName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Tool name must be passed in to execute the mcp tool");
		}
		toolName = MCPUtility.removeEngineIdFromToolsMethodName(engine.getEngineId(), toolName);

		// these are the params
		Map<String, Object> paramMap = resolveToolParamsForDecision(pendingAction, getMap(), decision);

		IMCP mcp = MCPFactory.build(engine);
		NounMetadata result = new NounMetadata(mcp.callTool(toolName, paramMap, this.insight),
				PixelDataType.MCP_TOOL_EXECUTION, PixelOperationType.MCP_TOOL_EXECUTION);

		// Agent context: write the tool result to the room and resume the agent run.
		if (hasAgentContext) {
			String resultStr = result.getValue() != null ? result.getValue().toString() : "";
			String status = toolStatus != null ? toolStatus : "success";
			writeToRoomAndResume(runId, agentRoomId, toolCallId, parentMessageId, resultStr, status,
					actionId, decision, paramMap, pendingAction, userId);
		}

		return result;
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
		return "Execute a tool defined in an engine or project/app. When runId/roomId/toolCallId are provided, the result is also written to the agent room and the agent run is resumed.";
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
			return "Agent run id. When present along with roomId and toolCallId, the tool result is written to the agent room and the run is resumed.";
		} else if (key.equals(ROOM_ID_KEY)) {
			return "Agent room id. Used with runId and toolCallId to write the tool result back to the agent's conversation history.";
		} else if (key.equals(TOOL_CALL_ID_KEY)) {
			return "The tool call id from the agent's paused tool-call batch. Used to match the result to the pending tool call in the room.";
		} else if (key.equals(PARENT_MESSAGE_ID_KEY)) {
			return "The assistant message id that contains the tool-call batch. Used as the parent for the tool result message.";
		} else if (key.equals(ReactorKeysEnum.MCP_TOOL_STATUS.getKey())) {
			return "Tool execution status: success, error, or cancelled. Defaults to success (or error for passthrough results).";
		} else if (key.equals(ACTION_ID_KEY)) {
			return "The AGENT_RUN_ACTION id for this pending tool call. Used to record the user's decision.";
		} else if (key.equals(DECISION_KEY)) {
			return "The user's decision: approve, edit, reject, or respond.";
		}
		return super.getDescriptionForKey(key);
	}

	/**
	 * Write the tool result to the agent's room via
	 * {@code Room.addToolExecutionResult}. If all tool calls from the paused
	 * batch are now answered, this auto-calls the model. Then marks the agent
	 * run as SUBMITTED so the worker picks it up and resumes the harness loop.
	 */
	private void writeToRoomAndResume(String runId, String roomId, String toolCallId, String parentMessageId,
			String toolResult, String toolStatus, String actionId, String decision, Map<String, Object> toolParams,
			Map<String, Object> pendingAction, String userId) {
		try {
			Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
			if (room == null) {
				logger.warn("RunMCPToolReactor: cannot resume - room not found roomId={}", roomId);
				return;
			}

			// Resolve the model engine from the room or the agent run record.
			String modelId = room.getModelId();
			if (modelId == null || modelId.trim().isEmpty()) {
				// Fallback: load the run record to get the model id
				AgentRunStore runStore = new AgentRunStore();
				prerna.reactor.agent.run.AgentRunRecord record = runStore.getRun(runId, this.insight);
				if (record != null && record.getRequest() != null) {
					modelId = record.getRequest().getEngineIdFallback();
				}
			}
			IModelEngine modelEngine = null;
			if (modelId != null && !modelId.trim().isEmpty()) {
				modelEngine = Utility.getModel(modelId);
			}
			if (modelEngine == null) {
				logger.warn("RunMCPToolReactor: cannot resume - model engine not found for roomId={} modelId={}",
						roomId, modelId);
				return;
			}

			// Write the tool result to the room. addToolExecutionResult
			// auto-calls the model when all tool_call_ids from the parent
			// message are answered.
			Map<String, Object> paramMapForRoom = new HashMap<>();
			String roomToolName = stringValue(pendingAction.get("toolName"));
			room.addToolExecutionResult(toolCallId, roomToolName,
					toolResult, toolParams != null ? toolParams : new HashMap<>(),
					paramMapForRoom, parentMessageId, modelEngine, this.insight, toolStatus);

			// Record the decision in AGENT_RUN_ACTION.
			String actionStatus = resolveActionStatus(decision);
			AgentRunActionStore actionStore = new AgentRunActionStore();
			boolean marked = actionStore.markDecided(actionId, runId, userId, decision != null ? decision : "approve",
					"edit".equalsIgnoreCase(decision) ? toolParams : null, null, toolResult, actionStatus);
			if (!marked) {
				throw new IllegalStateException("Pending action was not updated for actionId=" + actionId);
			}

			// Transition the run from INPUT_REQUIRED to SUBMITTED once ALL pending
			// actions in the batch are decided. For N>1 tool calls, the run must
			// not re-queue until every tool has a result; addToolExecutionResult
			// already guards the model auto-call on the same condition.
			if (actionStore.allActionsDecided(runId)) {
				AgentRunStore runStore = new AgentRunStore();
				boolean resumed = runStore.markResumed(runId, runId);
				if (resumed) {
					AgentRunEventBus.get().publishStatus(runId, roomId, AgentRunStatus.SUBMITTED, false);
					AgentRuntimeManager.get().signalWorkerForResume(runId, this.insight);
					logger.info("RunMCPToolReactor: resumed agent runId={} roomId={} toolCallId={}",
							runId, roomId, toolCallId);
				} else {
					logger.warn("RunMCPToolReactor: could not resume runId={} - not in INPUT_REQUIRED state", runId);
				}
			} else {
				logger.info("RunMCPToolReactor: recorded decision for toolCallId={} but waiting for remaining actions runId={}",
						toolCallId, runId);
			}

			// Push room to cluster if needed
			if (ClusterUtil.IS_CLUSTER) {
				try {
					ClusterUtil.pushRoomAsync(roomId);
				} catch (Exception e) {
					logger.warn("RunMCPToolReactor: room push failed for roomId={}: {}", roomId, e.getMessage());
				}
			}
		} catch (Exception e) {
			logger.error("RunMCPToolReactor: failed to write to room and resume runId={}: {}", runId, e.getMessage(), e);
		}
	}

	private Map<String, Object> loadAndValidatePendingAction(String actionId, String runId, String roomId,
			String parentMessageId, String toolCallId, String userId) {
		if (actionId == null || actionId.trim().isEmpty()) {
			throw new IllegalArgumentException("actionId is required to resume an agent HITL tool call");
		}
		if (parentMessageId == null || parentMessageId.trim().isEmpty()) {
			throw new IllegalArgumentException("parentMessageId is required to resume an agent HITL tool call");
		}
		if (userId == null || userId.trim().isEmpty() || "-1".equals(userId)) {
			throw new SecurityException("Agent HITL resume requires an authenticated user");
		}
		AgentRunActionStore actionStore = new AgentRunActionStore();
		Map<String, Object> pendingAction = actionStore.getPendingAction(actionId.trim(), runId.trim(), userId.trim());
		if (pendingAction == null) {
			throw new SecurityException("No pending agent action found for actionId=" + actionId);
		}
		requireEquals("roomId", roomId, pendingAction.get("roomId"));
		requireEquals("parentMessageId", parentMessageId, pendingAction.get("parentMessageId"));
		requireEquals("toolCallId", toolCallId, pendingAction.get("toolCallId"));
		return pendingAction;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> resolveToolParamsForDecision(Map<String, Object> pendingAction,
			Map<String, Object> callerParams, String decision) {
		if (pendingAction == null || "edit".equalsIgnoreCase(decision)) {
			return callerParams;
		}
		Object storedArgs = pendingAction.get("toolArgs");
		if (storedArgs instanceof Map) {
			return (Map<String, Object>) storedArgs;
		}
		if (storedArgs instanceof String && !((String) storedArgs).trim().isEmpty()) {
			Object parsed = GSON.fromJson((String) storedArgs, Object.class);
			if (parsed instanceof Map) {
				return (Map<String, Object>) parsed;
			}
		}
		return callerParams;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> parseStoredMap(Object value) {
		if (value instanceof Map) {
			return (Map<String, Object>) value;
		}
		if (value instanceof String && !((String) value).trim().isEmpty()) {
			Object parsed = GSON.fromJson((String) value, Object.class);
			if (parsed instanceof Map) {
				return (Map<String, Object>) parsed;
			}
		}
		return null;
	}

	private String engineIdFromPendingAction(Map<String, Object> pendingAction) {
		if (pendingAction == null) {
			return null;
		}
		Map<String, Object> toolMeta = parseStoredMap(pendingAction.get("toolMeta"));
		if (toolMeta == null) {
			return null;
		}
		String engineId = stringValue(toolMeta.get(MCPUtility.SMSS_ENGINE_ID));
		if (engineId == null) {
			engineId = stringValue(toolMeta.get(MCPUtility.SMSS_PROJECT_ID));
		}
		return engineId;
	}

	private static void requireEquals(String field, String provided, Object stored) {
		String providedValue = stringValue(provided);
		String storedValue = stringValue(stored);
		if (providedValue == null || storedValue == null || !providedValue.equals(storedValue)) {
			throw new SecurityException("Invalid agent HITL " + field);
		}
	}

	private static String resolveActionStatus(String decision) {
		if (decision == null) {
			return "APPROVED";
		}
		switch (decision.toLowerCase()) {
		case "edit":
			return "EDITED";
		case "reject":
			return "REJECTED";
		case "respond":
			return "RESPONDED";
		default:
			return "APPROVED";
		}
	}

	private static String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String s = String.valueOf(value).trim();
		return s.isEmpty() ? null : s;
	}

}
