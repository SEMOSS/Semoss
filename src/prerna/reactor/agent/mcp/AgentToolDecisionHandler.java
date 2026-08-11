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

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.ToolResultMessagePart;
import prerna.engine.impl.model.message.ToolResultPart;
import prerna.om.Insight;
import prerna.reactor.agent.run.AgentRunActionStore;
import prerna.reactor.agent.run.AgentRunRecord;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.reactor.agent.runtime.SemossAgentHarness;
import prerna.reactor.agent.stream.AgentRunStreamService;
import prerna.reactor.agent.stream.AgentStreamItems;
import prerna.util.Utility;

/**
 * Handles a HITL decision (approve/edit/reject/respond) on a paused agent tool
 * call. The AGENT_RUN_ACTION row is the source of truth for the run context;
 * the tool result is written to the room without calling the model and the run
 * is resumed once the whole batch is decided.
 */
public final class AgentToolDecisionHandler {

	private static final Logger logger = LogManager.getLogger(AgentToolDecisionHandler.class);
	private static final Gson GSON = new Gson();

	public static final String DECISION_APPROVE = "approve";
	public static final String DECISION_EDIT = "edit";
	public static final String DECISION_REJECT = "reject";
	public static final String DECISION_RESPOND = "respond";
	private static final String STATUS_PENDING = "PENDING";
	private static final String STATUS_EXECUTING = "EXECUTING";

	// optional caller-supplied context keys, validated against the action row
	public static final String CTX_RUN_ID = "runId";
	public static final String CTX_ROOM_ID = "roomId";
	public static final String CTX_TOOL_CALL_ID = "toolCallId";
	public static final String CTX_PARENT_MESSAGE_ID = "parentMessageId";

	private final Insight insight;

	public AgentToolDecisionHandler(Insight insight) {
		this.insight = insight;
	}

	/**
	 * Applies a decision to a pending action and returns the tool-result string.
	 * callerContext values are optional; when present they must match the row.
	 */
	public String handleDecision(String actionId, String decision, String passthroughResult, String toolStatus,
			Map<String, Object> callerParams, Map<String, String> callerContext) {
		String userId = this.insight != null ? this.insight.getUserId() : null;
		Map<String, Object> pendingAction = loadAndValidateAction(actionId, userId);
		String normalizedDecision = normalizeDecision(decision);

		// derive the run context from the row, validating caller-supplied values
		String runId = resolveFromRow(CTX_RUN_ID, contextValue(callerContext, CTX_RUN_ID), pendingAction);
		String roomId = resolveFromRow(CTX_ROOM_ID, contextValue(callerContext, CTX_ROOM_ID), pendingAction);
		String toolCallId = resolveFromRow(CTX_TOOL_CALL_ID, contextValue(callerContext, CTX_TOOL_CALL_ID),
				pendingAction);
		String parentMessageId = resolveFromRow(CTX_PARENT_MESSAGE_ID,
				contextValue(callerContext, CTX_PARENT_MESSAGE_ID), pendingAction);

		// idempotent replay: the action was already decided (retry/duplicate call)
		if (isDecidedAction(pendingAction)) {
			return replayDecidedAction(pendingAction, runId, roomId, toolCallId, parentMessageId, toolStatus, actionId,
					normalizedDecision, userId);
		}

		// reject/respond record a manual result without executing the tool
		if (!decisionExecutesTool(normalizedDecision)) {
			String manualResult = resolveManualDecisionResult(normalizedDecision, passthroughResult);
			writeToRoomAndResume(runId, roomId, toolCallId, parentMessageId, manualResult,
					toolStatus != null ? toolStatus : toolStatusForDecision(normalizedDecision), actionId,
					normalizedDecision, resolveToolParamsForDecision(pendingAction, callerParams), pendingAction,
					userId, true);
			publishDecisionToolItem(runId, toolCallId, stringValue(pendingAction.get("toolName")),
					resolveToolParamsForDecision(pendingAction, callerParams),
					DECISION_REJECT.equals(normalizedDecision) ? AgentStreamItems.TOOL_REJECTED
							: AgentStreamItems.TOOL_COMPLETED,
					manualResult, null);
			return manualResult;
		}

		// approve/edit must execute the real tool
		if (passthroughResult != null && !passthroughResult.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"mcpToolResult is only valid for HITL decision=reject or decision=respond");
		}

		String engineId = engineIdFromPendingAction(pendingAction);
		if (engineId == null && this.insight != null) {
			engineId = this.insight.getContextProjectId();
			if (engineId == null || engineId.isEmpty()) {
				engineId = this.insight.getProjectId();
			}
		}
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the project id or set the app context");
		}
		String toolName = stringValue(pendingAction.get("toolName"));
		Map<String, Object> paramMap = resolveToolParamsForDecision(pendingAction, callerParams);
		if (roomId != null && !roomId.isBlank()) {
			Room executionRoom = RoomUtils.getOrLoadRoom(roomId, this.insight);
			if (MCPUtility.ROOM_MCP_ID.equals(engineId)) {
				this.insight.setRoomForInsight(executionRoom);
			}
			// The stored action holds the aliased name the model produced; undo it
			// from the room's own map. See Room#resolveOriginalToolName.
			toolName = executionRoom.resolveOriginalToolName(toolName);
		}

		AgentRunActionStore actionStore = new AgentRunActionStore();
		if (!actionStore.claimForExecution(actionId, runId, userId)) {
			Map<String, Object> latestAction = actionStore.getActionById(actionId, userId);
			if (isDecidedAction(latestAction)) {
				// a concurrent handler already decided it; replay the stored result
				return replayDecidedAction(latestAction, runId, roomId, toolCallId, parentMessageId, toolStatus,
						actionId, normalizedDecision, userId);
			}
			throw new IllegalStateException("Agent HITL action is already being handled actionId=" + actionId);
		}

		ToolExecutionResult toolResult = MCPUtility.executeToolResult(engineId, toolName, paramMap, this.insight);
		String resultStr = toolResultContent(toolResult);
		String executedToolStatus = toolResult.getStatusValue();
		try {
			writeToRoomAndResume(runId, roomId, toolCallId, parentMessageId, resultStr,
					executedToolStatus, actionId, normalizedDecision, paramMap, pendingAction,
					userId, true);
		} catch (RuntimeException e) {
			// release the claim so a retry is not wedged on EXECUTING; the tool already
			// ran, so the retry replays via the decided/claim-race path if it was marked
			actionStore.releaseExecutionClaim(actionId, runId, userId);
			throw e;
		}
		publishDecisionToolItem(runId, toolCallId, toolName, paramMap,
				toolResult.isSuccess() ? AgentStreamItems.TOOL_COMPLETED : AgentStreamItems.TOOL_FAILED,
				toolResult.isSuccess() ? resultStr : null, toolResult.isSuccess() ? null : resultStr);
		return resultStr;
	}

	private static String toolResultContent(ToolExecutionResult result) {
		if (!result.isSuccess() && result.getError() != null && !result.getError().isBlank()) {
			return result.getError();
		}
		return result.getOutput() != null ? result.getOutput().toString() : "";
	}

	private static void publishDecisionToolItem(String runId, String toolCallId, String toolName,
			Map<String, Object> args, String status, String output, String error) {
		if (runId == null || runId.isBlank() || toolCallId == null || toolCallId.isBlank()) {
			return;
		}
		Map<String, Object> item = AgentStreamItems.toolItem(toolCallId, toolName, args, null, status);
		String boundedOutput = AgentStreamItems.truncate(output, AgentStreamItems.MAX_TOOL_OUTPUT_CHARS);
		if (boundedOutput != null && !boundedOutput.isBlank()) {
			item.put("output", boundedOutput);
		}
		String boundedError = AgentStreamItems.truncate(error, AgentStreamItems.MAX_TOOL_OUTPUT_CHARS);
		if (boundedError != null && !boundedError.isBlank()) {
			item.put("error", boundedError);
		}
		AgentRunStreamService.get().publishToolCompleted(runId, item);
	}

	/**
	 * Replays the stored result of an already-decided action without re-executing.
	 */
	private String replayDecidedAction(Map<String, Object> action, String runId, String roomId, String toolCallId,
			String parentMessageId, String toolStatus, String actionId, String normalizedDecision, String userId) {
		String storedResult = stringValue(action.get("result"));
		if (storedResult == null) {
			throw new IllegalStateException(
					"Agent HITL action is decided but has no stored result actionId=" + actionId);
		}
		Map<String, Object> retryParams = resolveRetryToolParams(action);
		String storedToolStatus = stringValue(action.get("toolStatus"));
		writeToRoomAndResume(runId, roomId, toolCallId, parentMessageId, storedResult,
				storedToolStatus != null ? storedToolStatus
						: (toolStatus != null ? toolStatus
								: toolStatusForActionStatus(stringValue(action.get("status")))),
				actionId, normalizedDecision, retryParams, action, userId, false);
		return storedResult;
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
			AgentRunRecord record = runStore.getRun(runId, this.insight);
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
			boolean marked = actionStore.markDecided(actionId, runId, userId, editedArgs, toolResult, actionStatus,
					toolStatus);
			if (!marked) {
				throw new IllegalStateException("Pending action was not updated for actionId=" + actionId);
			}
		}

		Map<String, Object> paramMapForRoom = new HashMap<>();
		String roomToolName = stringValue(pendingAction.get("toolName"));
		if (findToolResultMessage(room, parentMessageId, toolCallId) == null) {
			room.addToolExecutionResultWithoutModel(toolCallId, roomToolName, toolResult,
					toolParams != null ? toolParams : paramMapForRoom, parentMessageId, modelEngine, this.insight,
					toolStatus);
		}
		InputMessage toolResultMessage = findToolResultMessage(room, parentMessageId, toolCallId);
		if (toolResultMessage != null) {
			toolResultMessage.setOrnament(SemossAgentHarness.ORNAMENT_AGENT_RUN_ID, runId);
			toolResultMessage.setOrnament(SemossAgentHarness.ORNAMENT_AGENT_RUN_ROLE, "tool_result");
			RoomMessageStore.persist(room, userId);
		}

		// Transition the run from INPUT_REQUIRED to SUBMITTED once ALL pending
		// actions in the batch are decided. For N>1 tool calls, the run must
		// not re-queue until every tool has a result; the resumed harness performs
		// the model continuation once the batch is complete.
		if (actionStore.allActionsDecided(runId)) {
			AgentRunStore runStore = new AgentRunStore();
			boolean resumed = runStore.markResumed(runId, runId);
			if (!resumed) {
				AgentRunRecord record = runStore.getRun(runId, this.insight);
				AgentRunStatus status = record != null ? record.getStatus() : null;
				if (status == AgentRunStatus.INPUT_REQUIRED) {
					throw new IllegalStateException(
							"Agent run was not resumed because it is still INPUT_REQUIRED: " + runId);
				}
				logger.info("AgentToolDecisionHandler: runId={} already resumed or terminal status={}", runId, status);
				return;
			}
			AgentRuntimeManager.get().signalWorkerForResume(runId, this.insight);
			logger.info("AgentToolDecisionHandler: resumed agent runId={} roomId={} toolCallId={}", runId, roomId,
					toolCallId);
		} else {
			logger.info(
					"AgentToolDecisionHandler: recorded decision for toolCallId={} but waiting for remaining actions runId={}",
					toolCallId, runId);
		}

		if (ClusterUtil.IS_CLUSTER) {
			try {
				ClusterUtil.pushRoomAsync(roomId);
			} catch (Exception e) {
				logger.warn("AgentToolDecisionHandler: room push failed for roomId={}: {}", roomId, e.getMessage());
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

	private static String contextValue(Map<String, String> callerContext, String key) {
		return callerContext != null ? callerContext.get(key) : null;
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

	private InputMessage findToolResultMessage(Room room, String parentMessageId, String toolCallId) {
		if (room == null || toolCallId == null || toolCallId.trim().isEmpty()) {
			return null;
		}
		List<AbstractMessage> messages = room.getMessages();
		if (messages == null || messages.isEmpty()) {
			return null;
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
						ToolResultPart result = ((ToolResultMessagePart) part).getToolResult();
						if (result != null && toolCallId.equals(result.getToolCallId())) {
							return (InputMessage) message;
						}
					}
				}
			}
			if (parentMessageId != null && parentMessageId.equals(message.getMessageId())) {
				break;
			}
		}
		return null;
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
