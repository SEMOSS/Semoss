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
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.ToolResultMessagePart;
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
	private static final String DECISION_APPROVE = "approve";
	private static final String DECISION_EDIT = "edit";
	private static final String DECISION_REJECT = "reject";
	private static final String DECISION_RESPOND = "respond";
	private static final String STATUS_PENDING = "PENDING";
	private static final String STATUS_EXECUTING = "EXECUTING";

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
		// Agent context is driven by actionId alone. The AGENT_RUN_ACTION row is
		// the source of truth for runId/roomId/toolCallId/parentMessageId, so the
		// caller only needs to send actionId (+ decision, + edited paramValues).
		boolean hasAgentContext = actionId != null && !actionId.trim().isEmpty();
		String userId = this.insight != null ? this.insight.getUserId() : null;
		Map<String, Object> pendingAction = null;
		String normalizedDecision = null;
		if (hasAgentContext) {
			pendingAction = loadAndValidateAction(actionId, userId);
			normalizedDecision = normalizeDecision(decision);
			// Derive the run context from the row, validating any caller-supplied
			// values against it defensively.
			runId = resolveFromRow("runId", runId, pendingAction);
			agentRoomId = resolveFromRow("roomId", agentRoomId, pendingAction);
			toolCallId = resolveFromRow("toolCallId", toolCallId, pendingAction);
			parentMessageId = resolveFromRow("parentMessageId", parentMessageId, pendingAction);
		}

		if (hasAgentContext && isDecidedAction(pendingAction)) {
			String storedResult = stringValue(pendingAction.get("result"));
			if (storedResult == null) {
				throw new IllegalStateException("Agent HITL action is decided but has no stored result actionId=" + actionId);
			}
			Map<String, Object> retryParams = resolveRetryToolParams(pendingAction);
			writeToRoomAndResume(runId, agentRoomId, toolCallId, parentMessageId, storedResult,
					toolStatus != null ? toolStatus : toolStatusForActionStatus(stringValue(pendingAction.get("status"))),
					actionId, normalizedDecision, retryParams, pendingAction, userId, false);
			return new NounMetadata(storedResult, PixelDataType.CONST_STRING, PixelOperationType.MCP_TOOL_EXECUTION);
		}

		if (hasAgentContext && !decisionExecutesTool(normalizedDecision)) {
			String manualResult = resolveManualDecisionResult(normalizedDecision, toolExecutionResult);
			writeToRoomAndResume(runId, agentRoomId, toolCallId, parentMessageId, manualResult,
					toolStatus != null ? toolStatus : toolStatusForDecision(normalizedDecision), actionId,
					normalizedDecision, resolveToolParamsForDecision(pendingAction, getMap()), pendingAction, userId,
					true);
			return new NounMetadata(manualResult, PixelDataType.CONST_STRING, PixelOperationType.MCP_TOOL_EXECUTION);
		}

		if (hasAgentContext && hasPassthroughResult) {
			throw new IllegalArgumentException("mcpToolResult is only valid for HITL decision=reject or decision=respond");
		}

		if (hasPassthroughResult) {
			// Passthrough for normal MCP playground/UI calls. HITL calls are handled
			// above so approve/edit cannot bypass actual tool execution.
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
		Map<String, Object> paramMap = resolveToolParamsForDecision(pendingAction, getMap());

		AgentRunActionStore actionStore = new AgentRunActionStore();
		if (hasAgentContext && !actionStore.claimForExecution(actionId, runId, userId)) {
			Map<String, Object> latestAction = actionStore.getActionById(actionId, userId);
			if (isDecidedAction(latestAction)) {
				String storedResult = stringValue(latestAction.get("result"));
				if (storedResult == null) {
					throw new IllegalStateException("Agent HITL action is decided but has no stored result actionId="
							+ actionId);
				}
				Map<String, Object> retryParams = resolveRetryToolParams(latestAction);
				writeToRoomAndResume(runId, agentRoomId, toolCallId, parentMessageId, storedResult,
						toolStatus != null ? toolStatus : toolStatusForActionStatus(stringValue(latestAction.get("status"))),
						actionId, normalizedDecision, retryParams, latestAction, userId, false);
				return new NounMetadata(storedResult, PixelDataType.CONST_STRING, PixelOperationType.MCP_TOOL_EXECUTION);
			}
			throw new IllegalStateException("Agent HITL action is already being handled actionId=" + actionId);
		}

		IMCP mcp = MCPFactory.build(engine);
		NounMetadata result;
		try {
			result = new NounMetadata(mcp.callTool(toolName, paramMap, this.insight),
					PixelDataType.MCP_TOOL_EXECUTION, PixelOperationType.MCP_TOOL_EXECUTION);
		} catch (RuntimeException e) {
			if (hasAgentContext) {
				actionStore.releaseExecutionClaim(actionId, runId, userId);
			}
			throw e;
		}

		// Agent context: write the tool result to the room and resume the agent run.
		if (hasAgentContext) {
			String resultStr = result.getValue() != null ? result.getValue().toString() : "";
			String status = toolStatus != null ? toolStatus : "success";
			writeToRoomAndResume(runId, agentRoomId, toolCallId, parentMessageId, resultStr, status,
					actionId, normalizedDecision, paramMap, pendingAction, userId, true);
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
	 * Write the tool result to the agent's room without calling the model. When all
	 * paused actions are answered, mark the run SUBMITTED so the worker owns the
	 * follow-up model call and resumes the harness loop.
	 */
	private void writeToRoomAndResume(String runId, String roomId, String toolCallId, String parentMessageId,
			String toolResult, String toolStatus, String actionId, String decision, Map<String, Object> toolParams,
			Map<String, Object> pendingAction, String userId, boolean markActionDecided) {
		if (pendingAction == null) {
			throw new IllegalArgumentException("pendingAction is required to resume an agent HITL tool call");
		}
		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		if (room == null) {
			throw new IllegalStateException("Cannot resume agent run because room was not found roomId=" + roomId);
		}

		// Resolve the model engine from the room or the agent run record.
		String modelId = room.getModelId();
		if (modelId == null || modelId.trim().isEmpty()) {
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
			throw new IllegalStateException("Cannot resume agent run because model engine was not found for roomId="
					+ roomId + " modelId=" + modelId);
		}

		String actionStatus = resolveActionStatus(decision);
		Map<String, Object> storedArgs = parseStoredMap(pendingAction.get("toolArgs"));
		boolean argsChanged = storedArgs == null ? (toolParams != null && !toolParams.isEmpty())
				: !storedArgs.equals(toolParams);
		Object editedArgs = (DECISION_EDIT.equalsIgnoreCase(decision) || argsChanged) ? toolParams : null;
		AgentRunActionStore actionStore = new AgentRunActionStore();
		if (markActionDecided) {
			boolean marked = actionStore.markDecided(actionId, runId, userId, editedArgs, toolResult, actionStatus);
			if (!marked) {
				throw new IllegalStateException("Pending action was not updated for actionId=" + actionId);
			}
		}

		Map<String, Object> paramMapForRoom = new HashMap<>();
		String roomToolName = stringValue(pendingAction.get("toolName"));
		if (!toolResultAlreadyInRoom(room, parentMessageId, toolCallId)) {
			room.addToolExecutionResultWithoutModel(toolCallId, roomToolName,
					toolResult, toolParams != null ? toolParams : paramMapForRoom,
					parentMessageId, modelEngine, this.insight, toolStatus);
		}

		// Transition the run from INPUT_REQUIRED to SUBMITTED once ALL pending
		// actions in the batch are decided. For N>1 tool calls, the run must
		// not re-queue until every tool has a result; the resumed harness performs
		// the model continuation once the batch is complete.
		if (actionStore.allActionsDecided(runId)) {
			AgentRunStore runStore = new AgentRunStore();
			boolean resumed = runStore.markResumed(runId, runId);
			if (!resumed) {
				prerna.reactor.agent.run.AgentRunRecord record = runStore.getRun(runId, this.insight);
				AgentRunStatus status = record != null ? record.getStatus() : null;
				if (status == AgentRunStatus.INPUT_REQUIRED) {
					throw new IllegalStateException("Agent run was not resumed because it is still INPUT_REQUIRED: "
							+ runId);
				}
				logger.info("RunMCPToolReactor: runId={} already resumed or terminal status={}", runId, status);
				return;
			}
			AgentRunEventBus.get().publishStatus(runId, roomId, AgentRunStatus.SUBMITTED, false);
			AgentRuntimeManager.get().signalWorkerForResume(runId, this.insight);
			logger.info("RunMCPToolReactor: resumed agent runId={} roomId={} toolCallId={}",
					runId, roomId, toolCallId);
		} else {
			logger.info("RunMCPToolReactor: recorded decision for toolCallId={} but waiting for remaining actions runId={}",
					toolCallId, runId);
		}

		if (ClusterUtil.IS_CLUSTER) {
			try {
				ClusterUtil.pushRoomAsync(roomId);
			} catch (Exception e) {
				logger.warn("RunMCPToolReactor: room push failed for roomId={}: {}", roomId, e.getMessage());
			}
		}
	}

	private Map<String, Object> loadAndValidateAction(String actionId, String userId) {
		if (actionId == null || actionId.trim().isEmpty()) {
			throw new IllegalArgumentException("actionId is required to resume an agent HITL tool call");
		}
		if (userId == null || userId.trim().isEmpty() || "-1".equals(userId)) {
			throw new SecurityException("Agent HITL resume requires an authenticated user");
		}
		AgentRunActionStore actionStore = new AgentRunActionStore();
		Map<String, Object> action = actionStore.getActionById(actionId.trim(), userId.trim());
		if (action == null) {
			throw new SecurityException("No agent action found for actionId=" + actionId);
		}
		String status = stringValue(action.get("status"));
		if (STATUS_EXECUTING.equals(status)) {
			throw new IllegalStateException("Agent HITL action is already being handled actionId=" + actionId);
		}
		if (!STATUS_PENDING.equals(status) && !isDecidedStatus(status)) {
			throw new IllegalStateException("Agent HITL action cannot be resumed from status=" + status);
		}
		return action;
	}

	/**
	 * Resolve a run-context field from the stored action row. If the caller
	 * supplied a value, it must match the row (defensive check); otherwise the
	 * stored value is used.
	 */
	private String resolveFromRow(String field, String provided, Map<String, Object> pendingAction) {
		String stored = stringValue(pendingAction.get(field));
		if (provided == null || provided.trim().isEmpty()) {
			return stored;
		}
		requireEquals(field, provided, stored);
		return stored;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> resolveToolParamsForDecision(Map<String, Object> pendingAction,
			Map<String, Object> callerParams) {
		// Prefer caller-supplied params: the UI may add args the model omitted
		// (e.g. user-selected file_pks) or edit the model's proposed args.
		if (callerParams != null && !callerParams.isEmpty()) {
			return callerParams;
		}
		if (pendingAction == null) {
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

	private Map<String, Object> resolveRetryToolParams(Map<String, Object> action) {
		Map<String, Object> editedArgs = parseStoredMap(action.get("editedArgs"));
		if (editedArgs != null) {
			return editedArgs;
		}
		return resolveToolParamsForDecision(action, null);
	}

	private boolean toolResultAlreadyInRoom(Room room, String parentMessageId, String toolCallId) {
		if (room == null || toolCallId == null || toolCallId.trim().isEmpty()) {
			return false;
		}
		List<AbstractMessage> messages = room.getMessages();
		if (messages == null || messages.isEmpty()) {
			return false;
		}
		for (int i = messages.size() - 1; i >= 0; --i) {
			AbstractMessage message = messages.get(i);
			if (message instanceof InputMessage && message.hasToolResultPart()) {
				if (parentMessageId != null && !parentMessageId.trim().isEmpty()
						&& !parentMessageId.equals(message.getParentMessageId())) {
					continue;
				}
				for (MessagePart part : message.getParts()) {
					if (part instanceof ToolResultMessagePart) {
						prerna.engine.impl.model.message.ToolResultPart result =
								((ToolResultMessagePart) part).getToolResult();
						if (result != null && toolCallId.equals(result.getToolCallId())) {
							return true;
						}
					}
				}
			}
			if (parentMessageId != null && parentMessageId.equals(message.getMessageId())) {
				break;
			}
		}
		return false;
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
		switch (normalizeDecision(decision)) {
		case DECISION_EDIT:
			return "EDITED";
		case DECISION_REJECT:
			return "REJECTED";
		case DECISION_RESPOND:
			return "RESPONDED";
		default:
			return "APPROVED";
		}
	}

	private static boolean isDecidedAction(Map<String, Object> action) {
		return action != null && isDecidedStatus(stringValue(action.get("status")));
	}

	private static boolean isDecidedStatus(String status) {
		return "APPROVED".equals(status) || "EDITED".equals(status) || "REJECTED".equals(status)
				|| "RESPONDED".equals(status);
	}

	private static String normalizeDecision(String decision) {
		String normalized = stringValue(decision);
		if (normalized == null) {
			return DECISION_APPROVE;
		}
		normalized = normalized.toLowerCase();
		if (DECISION_APPROVE.equals(normalized) || DECISION_EDIT.equals(normalized)
				|| DECISION_REJECT.equals(normalized) || DECISION_RESPOND.equals(normalized)) {
			return normalized;
		}
		throw new IllegalArgumentException("Unsupported HITL decision: " + decision);
	}

	private static boolean decisionExecutesTool(String decision) {
		String normalized = normalizeDecision(decision);
		return DECISION_APPROVE.equals(normalized) || DECISION_EDIT.equals(normalized);
	}

	private static String resolveManualDecisionResult(String decision, String toolExecutionResult) {
		String result = stringValue(toolExecutionResult);
		if (result != null) {
			return result;
		}
		String normalized = normalizeDecision(decision);
		if (DECISION_REJECT.equals(normalized)) {
			return "Tool call rejected by user.";
		}
		throw new IllegalArgumentException("mcpToolResult is required for HITL decision=" + normalized);
	}

	private static String toolStatusForDecision(String decision) {
		String normalized = normalizeDecision(decision);
		if (DECISION_REJECT.equals(normalized)) {
			return "cancelled";
		}
		return "success";
	}

	private static String toolStatusForActionStatus(String actionStatus) {
		if ("REJECTED".equals(actionStatus)) {
			return "cancelled";
		}
		return "success";
	}

	private static String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String s = String.valueOf(value).trim();
		return s.isEmpty() ? null : s;
	}

}
