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
package prerna.engine.impl.model;

import java.io.File;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.InternalMCP;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.AbstractWorkspaceReactor;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageIO;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessagePartType;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.message.ToolResultMessagePart;
import prerna.engine.impl.model.message.ToolResultPart;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.PlaygroundThemeUtils;
import prerna.util.Utility;

public class Room implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger classLogger = LogManager.getLogger(Room.class);

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private static final Pattern SYSTEM_PROMPT_VARIABLE_PATTERN = Pattern
			.compile("\\{\\{\\s*([A-Z][A-Z0-9_]*)\\s*((?:\\.|\\[)[^}]*)?\\s*\\}\\}");
	private static final Pattern SAFE_SINGLE_STATEMENT_PIXEL = Pattern
			.compile("^\\s*[A-Za-z_][A-Za-z0-9_]*\\s*\\(.*\\)\\s*;?\\s*$", Pattern.DOTALL);

	private static final List<String> TEXT_MODEL_PARAM_KEYS = List.of("temperature");

	/**
	 * Per-room in-memory mutex for message mutations. Kept transient because lock
	 * state is not part of persisted room state.
	 */
	private transient ReentrantLock messageLock = new ReentrantLock();

	private String room_id;
	private String userId;
	private String roomName;
	private String shareId;
	private boolean isActive;
	private Timestamp createdAt;
	private Timestamp updatedAt;
	private final List<AbstractMessage> messages = new ArrayList<>();
	private boolean pinned;
	private String projectId;
	// options contains the tools
	private String options; // Stays as string (as from DB)
	private transient Map<String, Object> optionsMap; // Not stored, just for use in code

	private String modelId;
	private String messagesJson;

	private String parentRoomId;

	private transient Insight insight;
	private String roomFolderPath;

	/**
	 * Per-call reverse lookup map: LLM-facing tool name to enriched tool entry
	 * (containing engine metadata and original untruncated function name).
	 * Populated by {@link #getAllToolsJsonForRoom(int)} and consumed by
	 * {@link #updateToolResponseMeta(ResponseMessage)}.
	 */
	private transient final Map<String, Map<String, Object>> toolLookupByLLMName = new HashMap<>();

	/**
	 * Creates an empty room instance. Primarily used for serialization frameworks
	 * and ad-hoc object construction.
	 */
	public Room() {
	}

	/**
	 * Returns the room-level message lock, lazily reinitializing after
	 * deserialization when transient fields are null.
	 */
	private ReentrantLock getMessageLock() {
		ReentrantLock lock = this.messageLock;
		if (lock != null) {
			return lock;
		}
		synchronized (this) {
			lock = this.messageLock;
			if (lock == null) {
				lock = new ReentrantLock();
				this.messageLock = lock;
			}
		}
		return lock;
	}

	/**
	 * Creates a room from persisted ROOM-table fields.
	 *
	 * @param room_id       room identifier
	 * @param userId        owning user identifier
	 * @param roomName      display name
	 * @param systemMessage ignored legacy parameter retained for constructor
	 *                      compatibility
	 * @param projectId     associated project identifier
	 * @param shareId       share identifier, if shared
	 * @param isActive      whether the room is active
	 * @param createdAt     creation timestamp
	 * @param updatedAt     last update timestamp
	 * @param messagesJson  persisted message history JSON
	 * @param pinned        whether the room is pinned
	 * @param options       room options JSON
	 * @param modelId       model/engine identifier associated to the room
	 * @param parentRoomId  parent room identifier for sub-conversations (nullable)
	 */
	public Room(String room_id, String userId, String roomName, String systemMessage, String projectId, String shareId,
			boolean isActive, Timestamp createdAt, Timestamp updatedAt, String messagesJson, boolean pinned,
			String options, String modelId, String parentRoomId) {
		this.room_id = room_id;
		this.userId = userId;
		this.roomName = roomName;
		this.projectId = projectId;
		this.shareId = shareId;
		this.isActive = isActive;
		this.createdAt = createdAt;
		this.updatedAt = updatedAt;
		this.pinned = pinned;
		this.options = options;
		this.modelId = modelId;
		this.parentRoomId = parentRoomId;
		this.messagesJson = messagesJson;
		this.roomFolderPath = roomFolderPath(this.room_id);

		parseMessages();

		// --------- Parse options on object creation -------------
		if (options != null && !options.trim().isEmpty()) {
			try {
				this.optionsMap = GSON.fromJson(options, new TypeToken<Map<String, Object>>() {
				}.getType());
			} catch (Exception e) {
				this.optionsMap = new HashMap<>();
			}
		} else {
			this.optionsMap = new HashMap<>();
		}
	}

	/**
	 * Re-parses {@link #messagesJson} into the in-memory {@link #messages} list.
	 */
	public void parseMessages() {
		setMessagesFromString(this.messagesJson);
	}

	/**
	 * Sends an input message to the provided model engine using default parent
	 * resolution and history persistence behavior.
	 *
	 * @param msg         input message to send
	 * @param modelEngine model engine used for inference
	 * @return assistant response message
	 */
	public ResponseMessage ask(InputMessage msg, IModelEngine modelEngine) {
		return ask(msg, modelEngine, null);
	}

	/**
	 * Sends an input message to the model engine while explicitly targeting a
	 * parent message in the conversation tree.
	 *
	 * @param msg             input message to send
	 * @param modelEngine     model engine used for inference
	 * @param parentMessageId explicit parent message id; when null/blank the latest
	 *                        message is used
	 * @return assistant response message
	 */
	public ResponseMessage ask(InputMessage msg, IModelEngine modelEngine, String parentMessageId) {
		Boolean appendToHistory = true;
		return ask(msg, modelEngine, parentMessageId, appendToHistory);
	}

	/**
	 * Core room ask flow.
	 * <p>
	 * Builds message payload, applies tool metadata, executes model inference, and
	 * optionally appends/persists both input and output in room history.
	 *
	 * @param msg             input message to send
	 * @param modelEngine     model engine used for inference
	 * @param parentMessageId explicit parent message id; when null/blank the latest
	 *                        message is used
	 * @param appendToHistory whether to append and persist messages to room history
	 * @return assistant response message
	 */
	public ResponseMessage ask(InputMessage msg, IModelEngine modelEngine, String parentMessageId,
			Boolean appendToHistory) {
		ReentrantLock lock = getMessageLock();
		lock.lock();
		try {
			Map<String, Object> kwArgMap = new HashMap<>(msg.getParamMap());

			// if it is full prompt, process that first.
			if (kwArgMap.containsKey(AbstractModelEngine.FULL_PROMPT)) {
				AskModelEngineResponse llmResponse = modelEngine.askRoom(msg.getInputPrompt(), this, msg, kwArgMap);
				applyInputUsageFromModelResponse(msg, llmResponse);
				return buildAssistantResponseFromModelResponse(llmResponse, modelEngine, msg);
			}

			// if a specific system message is sent to use, overwrite the existing in the db
			if (msg.getSystemPrompt() != null) {
				ModelInferenceLogsUtils.setRoomContext(this.insight.getInsightId(),
						this.insight.getUser().getPrimaryLoginToken().getId(), msg.getSystemPrompt());
			}

			// this will modify tools if name is too large
			appendToolsToParams(kwArgMap, modelEngine);

			applyTextModelParams(kwArgMap);

			boolean useHistory = true;
			Object useHistoryObj = kwArgMap.get("use_history");
			if (useHistoryObj instanceof Boolean) {
				useHistory = (Boolean) useHistoryObj;
				kwArgMap.remove("use_history");
			} else if (useHistoryObj != null && "false".equalsIgnoreCase(useHistoryObj.toString())) {
				useHistory = false;
				kwArgMap.remove("use_history");
			}

			// does the model have keep keep input output off or is use_history false? if so
			// then just ask the model and send the response back.
			if (!modelEngine.keepInputOutput() || !useHistory) {
				String singleMessageJson = MessageUtils.toJsonArrayWithImageData(Arrays.asList(msg));
				kwArgMap.put("message_json", singleMessageJson);

				AskModelEngineResponse llmResponse = modelEngine.askRoom(msg.getInputPrompt(), this, msg, kwArgMap);
				applyInputUsageFromModelResponse(msg, llmResponse);
				return buildAssistantResponseFromModelResponse(llmResponse, modelEngine, msg);
			}

			String userId = insight.getUser().getPrimaryLoginToken().getId();
			try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(this)) {
				RoomMessageStore.refreshFromLatestProjection(this, userId);
				// if we dont have to keep history. then wipe all previous messages.
				if (!modelEngine.keepsConversationHistory()) {
					messages.clear();
				}

				// drop orphan tool_use (cancel mid-tool, crash) before building the outbound
				// branch so providers do not reject the next payload
				RoomMessageStore.normalizeForProviderPayload(this);

				// Set model type and add message to history
				msg.setModel(modelEngine);

				// Set parentMessageId for this message
				// first check that messages is not empty. otherwise its the first message of
				// the thread and parent is null
				if (!messages.isEmpty()) {
					// if a parent message id is passed in, validate it exists and use it.
					if (parentMessageId != null && !parentMessageId.isEmpty()) {
						msg.setParentMessageId(parentMessageId);
					} else {
						// if no parent message id is passed in, use the last message as the parent.
						AbstractMessage lastMsg = messages.get(messages.size() - 1);
						msg.setParentMessageId(lastMsg.getMessageId());
					}
				} else {
					msg.setParentMessageId(null); // first message
				}

				ResponseMessage response = null;
				try {
					String messageJsonString = RoomMessageStore.messageHistoryWithNewMessage(this, msg);
					kwArgMap.put("message_json", messageJsonString);

					AskModelEngineResponse llmResponse = modelEngine.askRoom(msg.getInputPrompt(), this, msg, kwArgMap);
					applyInputUsageFromModelResponse(msg, llmResponse);
					response = buildAssistantResponseFromModelResponse(llmResponse, modelEngine, msg);
				} catch (Exception e) {
					classLogger.error("Error running new message in room", e);
					throw e;
				}
				// on success, add the message
				if (appendToHistory) {
					messages.add(msg);
					messages.add(response);
				}

				// Save the old (before) roomName for comparison
				String prevRoomName = this.roomName;

				// Try to infer/set roomName if missing
				if (prevRoomName == null || prevRoomName.trim().isEmpty()) {
					for (AbstractMessage m : this.messages) {
						if (m instanceof InputMessage) {
							InputMessage im = (InputMessage) m;
							String prompt = im.getInputUIPrompt();
							if (prompt != null && !prompt.trim().isEmpty()) {
								this.roomName = prompt.substring(0, Math.min(prompt.length(), 100));
								break;
							}
						}
					}
				}

				// Persist message history - room name was just updated
				if (appendToHistory) {
					if ((prevRoomName == null || prevRoomName.trim().isEmpty()) && this.roomName != null
							&& !this.roomName.trim().isEmpty()) {
						// Only update with room name if we just set it now!
						RoomMessageStore.persist(this, userId, this.roomName, modelEngine.getEngineId());
					} else {
						// Otherwise, regular update
						RoomMessageStore.persist(this, userId);
					}
				}
				return response;
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Adds a tool execution result to the active tool-call context and, when all
	 * pending tool calls are satisfied, invokes the model for the follow-up
	 * assistant response.
	 *
	 * @param toolCallId            tool call identifier being fulfilled
	 * @param toolName              tool name used for execution
	 * @param toolExecutionResponse serialized tool output
	 * @param toolParameterValues   parameters provided to the tool call
	 * @param paramValuesMap        model parameter map used when continuing the
	 *                              assistant turn
	 * @param parentMessageId       optional parent message id anchoring the branch
	 * @param modelEngine           model engine used for follow-up inference
	 * @param insight               insight context used for persistence
	 * @param toolStatus            execution status for the tool result
	 * @return model response when all tool calls are fulfilled; otherwise
	 *         {@code null}
	 * @throws IllegalStateException    if no compatible tool-call context is
	 *                                  present
	 * @throws IllegalArgumentException if {@code toolCallId} does not match the
	 *                                  current assistant tool-call payload
	 */
	public AskModelEngineResponse addToolExecutionResult(String toolCallId, String toolName,
			String toolExecutionResponse, Map<String, Object> toolParameterValues, Map<String, Object> paramValuesMap,
			String parentMessageId, IModelEngine modelEngine, Insight insight, String toolStatus) {
		ReentrantLock lock = getMessageLock();
		lock.lock();
		try {
			return addToolExecutionResultInternal(toolCallId, toolName, toolExecutionResponse, toolParameterValues,
					paramValuesMap, parentMessageId, modelEngine, insight, toolStatus, true);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Adds a tool execution result to the active tool-call context and persists it
	 * without invoking the model. Used by durable agent HITL resumes so the worker
	 * owns the follow-up model call.
	 */
	public void addToolExecutionResultWithoutModel(String toolCallId, String toolName, String toolExecutionResponse,
			Map<String, Object> toolParameterValues, String parentMessageId, IModelEngine modelEngine, Insight insight,
			String toolStatus) {
		ReentrantLock lock = getMessageLock();
		lock.lock();
		try {
			addToolExecutionResultInternal(toolCallId, toolName, toolExecutionResponse, toolParameterValues, null,
					parentMessageId, modelEngine, insight, toolStatus, false);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Continues from a completed tool-result input message. Returns {@code null}
	 * when the tool-call batch is still incomplete.
	 */
	public AskModelEngineResponse continueAfterToolExecutionResults(Map<String, Object> paramValuesMap,
			String parentMessageId, IModelEngine modelEngine, Insight insight) {
		ReentrantLock lock = getMessageLock();
		lock.lock();
		try {
			String userId = insight.getUser().getPrimaryLoginToken().getId();
			try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(this)) {
				RoomMessageStore.refreshFromLatestProjection(this, userId);
				if (messages.isEmpty()) {
					throw new IllegalStateException("No messages to match tool call context");
				}

				String lastMessageId = resolveToolContinuationMessageId(parentMessageId);
				ToolExecutionContext context = findToolExecutionContext(lastMessageId);
				InputMessage toolResultsMessage = findToolResultsMessage(context.toolResponse, context.toolResponseIdx);
				if (toolResultsMessage == null) {
					throw new IllegalStateException("No tool execution result message found to continue.");
				}
				if (!allToolCallsAnswered(context.toolResponse, context.toolResponseIdx, null)) {
					RoomMessageStore.persist(this, userId);
					return null;
				}
				return continueFromToolResultsMessage(context.toolResponseIdx, toolResultsMessage, paramValuesMap,
						modelEngine, userId, false);
			}
		} finally {
			lock.unlock();
		}
	}

	private AskModelEngineResponse addToolExecutionResultInternal(String toolCallId, String toolName,
			String toolExecutionResponse, Map<String, Object> toolParameterValues, Map<String, Object> paramValuesMap,
			String parentMessageId, IModelEngine modelEngine, Insight insight, String toolStatus,
			boolean continueWhenReady) {
		String userId = insight.getUser().getPrimaryLoginToken().getId();
		try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(this)) {
			RoomMessageStore.refreshFromLatestProjection(this, userId);
			if (messages.isEmpty()) {
				throw new IllegalStateException("No messages to match tool call context");
			}

			String lastMessageId = resolveToolContinuationMessageId(parentMessageId);
			ToolExecutionContext context = findToolExecutionContext(lastMessageId);
			validateToolCallId(context.toolResponse, toolCallId);

			if (hasToolCallBeenAnswered(toolCallId)) {
				classLogger.warn(
						"Skipping duplicate tool execution result for toolCallId={} (toolName={}) on parentMessageId={}",
						toolCallId, toolName, context.toolResponse.getMessageId());
				RoomMessageStore.persist(this, userId);
				return null;
			}

			InputMessage toolResultsMessage = findToolResultsMessage(context.toolResponse, context.toolResponseIdx);
			boolean isToolResultsInputMessage = false;
			if (toolResultsMessage == null) {
				isToolResultsInputMessage = true;
				toolResultsMessage = InputMessage
						.builder(this).withSystemPrompt(this.getSystemPromptForModel()).withToolResult(toolCallId,
								toolName, toolExecutionResponse, toolParameterValues, toolStatus, false)
						.withModelType(modelEngine.getModelType()).build();
				toolResultsMessage.setParentMessageId(context.toolResponse.getMessageId());
				toolResultsMessage.setModel(modelEngine);
				toolResultsMessage.setVisible(false);
			} else {
				toolResultsMessage.addPart(new ToolResultMessagePart(new ToolResultPart(toolCallId, toolName,
						toolExecutionResponse, toolParameterValues, toolStatus, false)));
				toolResultsMessage.normalizeForWrite();
			}

			if (isToolResultsInputMessage) {
				messages.add(toolResultsMessage);
			}
			if (!continueWhenReady
					|| !allToolCallsAnswered(context.toolResponse, context.toolResponseIdx, toolCallId)) {
				RoomMessageStore.persist(this, userId);
				return null;
			}
			return continueFromToolResultsMessage(context.toolResponseIdx, toolResultsMessage, paramValuesMap,
					modelEngine, userId, true);
		}
	}

	private String resolveToolContinuationMessageId(String parentMessageId) {
		if (parentMessageId != null && !parentMessageId.isEmpty()) {
			return parentMessageId;
		}
		AbstractMessage lastMsg = messages.get(messages.size() - 1);
		return lastMsg.getMessageId();
	}

	private ToolExecutionContext findToolExecutionContext(String lastMessageId) {
		ResponseMessage toolResponse = null;
		List<AbstractMessage> branchMessages = MessageUtils.getMessageBranchFromParent(messages, lastMessageId);
		for (int i = branchMessages.size() - 1; i >= 0; --i) {
			AbstractMessage m = branchMessages.get(i);
			if (m instanceof ResponseMessage) {
				if (((ResponseMessage) m).hasToolResponses() || m.hasToolCallPart()) {
					toolResponse = (ResponseMessage) m;
					break;
				} else if (m.hasTextPart()) {
					break;
				}
			} else if (m instanceof InputMessage) {
				if (m.hasToolResultPart()) {
					continue;
				}
				break;
			}
		}
		if (toolResponse == null) {
			throw new IllegalStateException(
					"No previous assistant tool_calls (RESPONSE_TOOL) message found immediately before tool execution(s).");
		}

		int toolResponseIdx = -1;
		for (int i = messages.size() - 1; i >= 0; --i) {
			AbstractMessage m = messages.get(i);
			if (m != null && toolResponse.getMessageId().equals(m.getMessageId())) {
				toolResponseIdx = i;
				break;
			}
		}
		if (toolResponseIdx < 0) {
			throw new IllegalStateException("Unable to locate tool response message in room history.");
		}
		return new ToolExecutionContext(toolResponse, toolResponseIdx);
	}

	private static void validateToolCallId(ResponseMessage toolResponse, String toolCallId) {
		for (Map<String, Object> toolCall : toolResponse.getToolResponses()) {
			String thisId = String.valueOf(toolCall.get("id"));
			if (toolCallId.equals(thisId)) {
				return;
			}
		}
		throw new IllegalArgumentException("No matching tool_call_id in last assistant tool_calls response.");
	}

	private InputMessage findToolResultsMessage(ResponseMessage toolResponse, int toolResponseIdx) {
		for (int i = messages.size() - 1; i > toolResponseIdx; --i) {
			AbstractMessage m = messages.get(i);
			if (m instanceof ResponseMessage) {
				break;
			}
			if (m instanceof InputMessage && m.hasToolResultPart()
					&& toolResponse.getMessageId().equals(m.getParentMessageId())) {
				return (InputMessage) m;
			}
		}
		return null;
	}

	public boolean hasToolCallBeenAnswered(String toolCallId) {
		ReentrantLock lock = getMessageLock();
		lock.lock();
		try {
			for (AbstractMessage m : messages) {
				if (!(m instanceof InputMessage) || !m.hasToolResultPart()) {
					continue;
				}
				for (MessagePart p : m.getParts()) {
					if (p instanceof ToolResultMessagePart) {
						ToolResultPart tr = ((ToolResultMessagePart) p).getToolResult();
						if (tr != null && toolCallId.equals(tr.getToolCallId())) {
							return true;
						}
					}
				}
				if (toolCallId.equals(((InputMessage) m).getToolCallId())) {
					return true;
				}
			}
			return false;
		} finally {
			lock.unlock();
		}
	}

	private boolean allToolCallsAnswered(ResponseMessage toolResponse, int toolResponseIdx, String newToolCallId) {
		Set<String> allIds = new HashSet<>();
		for (Map<String, Object> tc : toolResponse.getToolResponses()) {
			allIds.add(String.valueOf(tc.get("id")));
		}
		if (allIds.isEmpty()) {
			return false;
		}

		Set<String> answeredIds = new HashSet<>();
		if (newToolCallId != null) {
			answeredIds.add(newToolCallId);
		}
		for (int i = toolResponseIdx + 1; i < messages.size(); ++i) {
			AbstractMessage m = messages.get(i);
			if (m instanceof InputMessage && m.hasToolResultPart()) {
				for (MessagePart p : m.getParts()) {
					if (p instanceof ToolResultMessagePart) {
						ToolResultPart tr = ((ToolResultMessagePart) p).getToolResult();
						if (tr != null && tr.getToolCallId() != null) {
							answeredIds.add(tr.getToolCallId());
						}
					}
				}
				String legacyId = ((InputMessage) m).getToolCallId();
				if (legacyId != null) {
					answeredIds.add(legacyId);
				}
			}
		}
		return answeredIds.containsAll(allIds);
	}

	private AskModelEngineResponse continueFromToolResultsMessage(int toolResponseIdx, InputMessage toolResultsMessage,
			Map<String, Object> paramValuesMap, IModelEngine modelEngine, String userId,
			boolean removeToolResultsOnFailure) {
		String messageJsonString = RoomMessageStore.currentMessageHistory(this);
		if (paramValuesMap == null) {
			paramValuesMap = new HashMap<>();
		}
		paramValuesMap.put("message_json", messageJsonString);
		appendToolsToParams(paramValuesMap, modelEngine);

		StringBuilder toolResultsForLogging = new StringBuilder();
		for (MessagePart part : toolResultsMessage.getParts()) {
			if (part instanceof ToolResultMessagePart) {
				ToolResultPart tr = ((ToolResultMessagePart) part).getToolResult();
				if (tr != null && tr.getOutput() != null) {
					if (toolResultsForLogging.length() > 0) {
						toolResultsForLogging.append("\n");
					}
					toolResultsForLogging.append(tr.getOutput());
				}
			}
		}

		AskModelEngineResponse llmResponse = null;
		ResponseMessage nextAssistant = null;
		try {
			llmResponse = modelEngine.askRoom(toolResultsForLogging.toString(), this, toolResultsMessage,
					paramValuesMap);
			applyInputUsageFromModelResponse(toolResultsMessage, llmResponse);
			nextAssistant = buildAssistantResponseFromModelResponse(llmResponse, modelEngine, toolResultsMessage);
		} catch (Exception e) {
			if (removeToolResultsOnFailure && !messages.isEmpty()) {
				messages.removeLast();
			}
			classLogger.error("Error adding the tool result message and getting a model response. Error: {}",
					e.getMessage(), e);
			throw e;
		}
		messages.add(nextAssistant);

		String transactionId = nextAssistant.getTransactionId();
		if (transactionId == null || transactionId.isEmpty()) {
			transactionId = GUID.v7().toUUID().toString();
			nextAssistant.setTransactionId(transactionId);
		}

		for (int i = toolResponseIdx + 1; i < messages.size(); ++i) {
			AbstractMessage m = messages.get(i);
			if (m == nextAssistant) {
				break;
			}
			if (m == toolResultsMessage) {
				m.setTransactionId(transactionId);
			}
		}

		RoomMessageStore.persist(this, userId);
		return llmResponse;
	}

	private static final class ToolExecutionContext {
		private final ResponseMessage toolResponse;
		private final int toolResponseIdx;

		private ToolExecutionContext(ResponseMessage toolResponse, int toolResponseIdx) {
			this.toolResponse = toolResponse;
			this.toolResponseIdx = toolResponseIdx;
		}
	}

	/**
	 * Applies prompt-side usage metadata from model output to the originating input
	 * message.
	 *
	 * @param inputMessage input message that triggered the model call
	 * @param llmResponse  model response containing prompt token count and message
	 *                     id
	 */
	private void applyInputUsageFromModelResponse(InputMessage inputMessage, AskModelEngineResponse llmResponse) {
		if (inputMessage == null || llmResponse == null) {
			return;
		}
		inputMessage.setTransactionId(llmResponse.getMessageId());
		inputMessage.setTokensInMessage(llmResponse.getNumberOfTokensInPrompt());
	}

	/**
	 * Builds and finalizes an assistant response message from a model response with
	 * consistent side effects.
	 * <p>
	 * Applies response id/transaction, model/room/parent linkage, response token
	 * count, and persists media parts to the room folder.
	 *
	 * @param llmResponse   model response
	 * @param modelEngine   model engine used for inference
	 * @param parentMessage parent message to link as this response's parent
	 * @return finalized assistant response message
	 */
	private ResponseMessage buildAssistantResponseFromModelResponse(AskModelEngineResponse llmResponse,
			IModelEngine modelEngine, AbstractMessage parentMessage) {
		ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(llmResponse).build();
		String llmMessageId = llmResponse.getMessageId();
		if (llmMessageId != null && !llmMessageId.isEmpty()) {
			response.setMessageId(llmMessageId);
			response.setTransactionId(llmMessageId);
		} else if (response.getTransactionId() == null || response.getTransactionId().isEmpty()) {
			response.setTransactionId(response.getMessageId());
		}
		response.setModel(modelEngine);
		response.setRoom(this);
		response.setParentMessageId(parentMessage == null ? null : parentMessage.getMessageId());
		response.setTokensInMessage(llmResponse.getNumberOfTokensInResponse());
		RoomUtils.persistMediaPartsToRoomFolder(response, this);
		return response;
	}

	/**
	 * Indicates whether this room can accept a new user input message at the
	 * current point in conversation state.
	 *
	 * @return {@code true} when the last message is a terminal assistant response
	 *         (text/media), {@code false} when tool-call continuation is required
	 * @throws IllegalStateException if the room has no messages
	 */
	public boolean roomCanAcceptNewInputMessage() {
		if (messages.isEmpty()) {
			throw new IllegalStateException("No messages to match tool call context");
		}

		// get the last messsage
		AbstractMessage lastMessage = messages.get(messages.size() - 1);
		if (lastMessage.hasParts()) {
			if (lastMessage.getIo() == MessageIO.OUTPUT) {
				MessagePart lastMessagePart = lastMessage.getParts().getLast();
				if (lastMessagePart.getType() != MessagePartType.TOOL_CALL) {
					return true;
				}
			}
		} else {
			if (lastMessage.getMessageType() == MessageType.RESPONSE_MEDIA
					|| lastMessage.getMessageType() == MessageType.RESPONSE_TEXT) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Appends text gen model specific parameters from room options into the
	 * provided model.
	 * 
	 * @param kwArgMap
	 */
	private void applyTextModelParams(Map<String, Object> kwArgMap) {
		Map<String, Object> options = getOptionsMap();
		if (options == null || options.isEmpty()) {
			return;
		}

		for (String key : TEXT_MODEL_PARAM_KEYS) {
			Object val = options.get(key);
			if (val != null) {
				kwArgMap.putIfAbsent(key, val);
			}
		}
	}

	/**
	 * Appends room-level tools into the model invocation parameter map.
	 *
	 * @param params      mutable model parameter map
	 * @param modelEngine model engine used to determine max tool name length
	 */
	private void appendToolsToParams(Map<String, Object> params, IModelEngine modelEngine) {
		int maxLength = MCPUtility.getMaxToolNameLength(modelEngine);
		List<Map<String, Object>> newTools = getAllToolsJsonForRoom(maxLength);
		Object existing = params.get("tools");
		if (existing instanceof List<?>) {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> toolsList = (List<Map<String, Object>>) existing;
			toolsList.addAll(newTools);
		} else if (existing == null) {
			params.put("tools", new ArrayList<>(newTools));
		} else {
			params.put("tools", new ArrayList<>(newTools));
		}
	}

	/**
	 * Returns all tools for this room using the default max tool name length. Also
	 * populates {@link #toolLookupByLLMName} for reverse-lookup in
	 * {@link #updateToolResponseMeta(ResponseMessage)}.
	 *
	 * @return list of tool definition maps ready to pass to the LLM
	 */
	public List<Map<String, Object>> getAllToolsJsonForRoom() {
		return getAllToolsJsonForRoom(Integer.MAX_VALUE);
	}

	/**
	 * Returns all tools for this room, applying provider-specific name-length
	 * limits. Also clears and rebuilds {@link #toolLookupByLLMName} so that
	 * {@link #updateToolResponseMeta(ResponseMessage)} can resolve LLM-facing names
	 * back to their original engine IDs and function names.
	 *
	 * <p>
	 * <b>In-process LLM path.</b> This is the resolution used by the {@code semoss}
	 * harness (and the playground COT reactors). It reads
	 * {@code room.options.mcp[]} plus the {@code WORKSPACE_RESOURCE} rows for
	 * {@code room.options.workspace.workspace_id}, resolves each engine through
	 * {@link prerna.reactor.agent.mcp.MCPUtility#getAggregatedTools}, and returns
	 * full tool definition maps for the LLM call.
	 *
	 * <p>
	 * External-CLI harnesses ({@code claude_code}, {@code github_copilot_py}) take
	 * a sibling path: {@code AgentConfig.getMcps()}, populated by
	 * {@code AgentConfigLoader} from the same two sources, but returning engine
	 * refs ({@code id}/{@code name}) rather than resolved tool defs - the external
	 * CLI does its own MCP handshake to discover tools.
	 *
	 * <p>
	 * Both paths honor {@code room.options.workspace.workspace_id}, so the
	 * {@code workspaceId} arg on {@code RunAgent} (applied via
	 * {@code AgentRunner}'s workspace overlay) yields the same MCP set in either
	 * harness style.
	 *
	 * @param maxLength maximum allowed tool name length (use
	 *                  {@link MCPUtility#DEFAULT_MAX_TOOL_NAME_LENGTH} as default)
	 * @return list of tool definition maps ready to pass to the LLM
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> getAllToolsJsonForRoom(int maxLength) {
		toolLookupByLLMName.clear();
		List<Map<String, Object>> aggregated = new ArrayList<>();
		Map<String, Object> o = getOptionsMap();

		// make sure the same toolbox is not accidentally added more than once
		Set<String> ensureUnique = new HashSet<>();

		// The room's own toolbox comes from disk, with no options.mcp entry needed.
		// Done first so ensureUnique skips a room whose options also list the room id.
		if (InternalMCP.hasDefinitions(getRoomFolderPath())) {
			ensureUnique.add(MCPUtility.ROOM_MCP_ID);
			try {
				aggregated.addAll(getToolJson(MCPUtility.ROOM_MCP_ID, maxLength));
			} catch (Exception e) {
				classLogger.error("Unable to add the room's own MCP tools", e);
			}
		}

		if (o.containsKey("mcp")) {
			try {
				List<Map<String, Object>> mapMapList = (List<Map<String, Object>>) o.get("mcp");
				for (Map<String, Object> mcpMap : mapMapList) {
					try {
						if (mcpMap.containsKey("id")) {
							String id = (String) mcpMap.get("id");
							if (!ensureUnique.contains(id)) {
								aggregated.addAll(getToolJson(id, maxLength));
								ensureUnique.add(id);
							}
						} else {
							// id is the only field that is read; type and name are display only
							throw new IllegalArgumentException("Tool map must contain an id");
						}
					} catch (Exception e) {
						classLogger.error("Unable to add tool map from room mcp", e);
					}
				}
			} catch (ClassCastException e) {
				classLogger.error("Malformed 'mcp' value in the options map", e);
			}
		}

		if (o.containsKey("workspace")) {
			try {
				Map<String, Object> workspace = (Map<String, Object>) o.get("workspace");
				if (workspace != null && workspace.containsKey("workspace_id")) {
					String workspaceId = (String) workspace.get("workspace_id");

					// legacy: workspace_resource rows
					try {
						List<Map<String, Object>> tools = ModelInferenceLogsUtils.getWorkspaceResourcesIgnoringType(
								workspaceId, Arrays.asList(AbstractWorkspaceReactor.PROMPT_RESOURCE_TYPE,
										AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE));
						for (Map<String, Object> tool : tools) {
							String toolId = (String) tool.get("resource_id");
							if (!ensureUnique.contains(toolId)) {
								aggregated.addAll(getToolJson(toolId, maxLength));
								ensureUnique.add(toolId);
							}
						}
					} catch (Exception e) {
						classLogger.error("Unable to add tool map from workspace mcp", e);
					}

					// new: CONFIG_JSON.mcps (additive, deduped against legacy by id)
					try {
						JSONObject cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
						JSONArray arr = cfg != null ? cfg.optJSONArray("mcps") : null;
						if (arr != null) {
							for (int i = 0; i < arr.length(); i++) {
								JSONObject mcp = arr.optJSONObject(i);
								if (mcp == null) {
									continue;
								}
								String toolId = mcp.optString("id", null);
								if (toolId != null && !toolId.isEmpty() && !ensureUnique.contains(toolId)) {
									aggregated.addAll(getToolJson(toolId, maxLength));
									ensureUnique.add(toolId);
								}
							}
						}
					} catch (Exception e) {
						classLogger.warn("CONFIG_JSON.mcps read failed for workspaceId={}: {}", workspaceId,
								e.getMessage());
					}
				}
			} catch (ClassCastException e) {
				classLogger.error("Malformed 'workspace' value in the options map", e);
			}
		}

		return aggregated;
	}

	/**
	 * Returns the tool definitions for a single engine, applying the given name
	 * length limit and populating {@link #toolLookupByLLMName} so that
	 * {@link #updateToolResponseMeta(ResponseMessage)} can reverse-resolve
	 * LLM-facing names.
	 *
	 * @param engineId  the engine/project UUID
	 * @param maxLength maximum allowed tool name length
	 * @return list of non-disabled tool definition maps
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getToolJson(String engineId, int maxLength) {
		// room level MCPs
		if (MCPUtility.ROOM_MCP_ID.equals(engineId)) {
			InternalMCP roomMcp = InternalMCP.genFromRoomFolder(this.getRoomFolderPath());
			JSONObject toolMap = roomMcp.getMCPTools();
			if (toolMap == null) {
				return new ArrayList<>();
			}
			Map<String, Object> engineMeta = toolMap.has("_meta") ? toolMap.getJSONObject("_meta").toMap()
					: new HashMap<>();
			Map<Integer, String> originalNames = new HashMap<>();
			if (toolMap.has("tools")) {
				JSONArray toolsBefore = toolMap.getJSONArray("tools");
				for (int i = 0; i < toolsBefore.length(); i++) {
					JSONObject toolBefore = toolsBefore.optJSONObject(i);
					if (toolBefore != null && toolBefore.has("name")) {
						originalNames.put(i, toolBefore.getString("name"));
					}
				}
			}
			JSONObject updatedToolMap = MCPUtility.appendEngineIdToToolsMethodName(MCPUtility.ROOM_MCP_ID, toolMap,
					maxLength);
			if (updatedToolMap == null || !updatedToolMap.has("tools")) {
				return new ArrayList<>();
			}
			JSONArray arr = updatedToolMap.getJSONArray("tools");
			List<Map<String, Object>> result = new ArrayList<>();
			for (int i = 0; i < arr.length(); i++) {
				JSONObject toolObj = arr.optJSONObject(i);
				if (toolObj == null) {
					continue;
				}
				JSONObject meta = toolObj.optJSONObject("_meta");
				Object executionValue = meta != null ? meta.opt(MCPUtility.SMSS_MCP_EXECUTION) : null;
				if (!MCPExecution.DISABLED.getValue().equals(executionValue)) {
					Map<String, Object> entry = toolObj.toMap();
					result.add(entry);
					String llmName = toolObj.getString("name");
					Map<String, Object> lookupMeta = new HashMap<>(engineMeta);
					Object rawMeta = entry.get("_meta");
					if (rawMeta instanceof Map) {
						lookupMeta.putAll((Map<String, Object>) rawMeta);
					}
					lookupMeta.put(MCPUtility.SMSS_ENGINE_ID, MCPUtility.ROOM_MCP_ID);
					lookupMeta.put(MCPUtility.SMSS_ORIGINAL_TOOL_NAME, originalNames.get(i));

					Map<String, Object> lookupEntry = new HashMap<>();
					if (entry.containsKey("title")) {
						lookupEntry.put("title", entry.get("title"));
					}
					if (entry.containsKey("description")) {
						lookupEntry.put("description", entry.get("description"));
					}
					lookupEntry.put("_meta", lookupMeta);
					toolLookupByLLMName.put(llmName, lookupEntry);
				}
			}
			return result;
		}

		// normal engine/project mcp
		IEngine engine = null;
		try {
			engine = Utility.getEngine(engineId);
		} catch (Exception ex) {
			// ignore
			engine = Utility.getProject(engineId);
		}
		if (engine == null) {
			throw new IllegalArgumentException(
					"Invalid MCP toolbox " + engineId + ". Please remove the toolbox from your room.");
		}

		JSONObject toolMap = MCPUtility.getAggregatedTools(engine);

		// Record original tool names (before prefix is added) so we can store them in
		// the reverse-lookup map later
		Map<Integer, String> originalNames = new HashMap<>();
		if (toolMap != null && toolMap.has("tools")) {
			JSONArray toolsBefore = toolMap.getJSONArray("tools");
			for (int i = 0; i < toolsBefore.length(); i++) {
				JSONObject t = toolsBefore.optJSONObject(i);
				if (t != null && t.has("name")) {
					originalNames.put(i, t.getString("name"));
				}
			}
		}

		// Capture engine-level metadata for the lookup entries
		Map<String, Object> engineMeta = new HashMap<>();
		if (toolMap != null && toolMap.has("_meta")) {
			engineMeta = toolMap.getJSONObject("_meta").toMap();
		}

		JSONObject updatedToolMap = MCPUtility.appendEngineIdToToolsMethodName(engineId, toolMap, maxLength);
		if (updatedToolMap != null && updatedToolMap.has("tools")) {
			JSONArray arr = updatedToolMap.getJSONArray("tools");
			List<Map<String, Object>> result = new ArrayList<>();
			for (int i = 0; i < arr.length(); i++) {
				JSONObject toolObj = arr.optJSONObject(i);
				if (toolObj == null) {
					continue;
				}

				JSONObject meta = toolObj.optJSONObject("_meta");
				Object executionValue = meta != null ? meta.opt(MCPUtility.SMSS_MCP_EXECUTION) : null;

				if (!MCPExecution.DISABLED.getValue().equals(executionValue)) {
					Map<String, Object> toolMapEntry = toolObj.toMap();
					result.add(toolMapEntry);

					// Build a minimal lookup entry (only what updateToolResponseMeta reads).
					// Engine-level fields go in first; tool-level meta overrides on top.
					String llmFacingName = toolObj.getString("name");
					Map<String, Object> lookupMeta = new HashMap<>(engineMeta);
					Object rawToolMeta = toolMapEntry.get("_meta");
					if (rawToolMeta instanceof Map) {
						lookupMeta.putAll((Map<String, Object>) rawToolMeta);
					}
					// the user might already have an indirection between the tool name and function
					// name so if this key already exists keep using that
					if (lookupMeta.containsKey(MCPUtility.SMSS_FUNCTION_NAME)) {
						lookupMeta.put(MCPUtility.SMSS_FUNCTION_NAME, originalNames.get(i));
					}
					lookupMeta.put(MCPUtility.SMSS_ORIGINAL_TOOL_NAME, originalNames.get(i));

					Map<String, Object> lookupEntry = new HashMap<>();
					if (toolMapEntry.containsKey("title")) {
						lookupEntry.put("title", toolMapEntry.get("title"));
					}
					if (toolMapEntry.containsKey("description")) {
						lookupEntry.put("description", toolMapEntry.get("description"));
					}
					lookupEntry.put("_meta", lookupMeta);
					toolLookupByLLMName.put(llmFacingName, lookupEntry);
				}
			}
			return result;
		}

		// Fallback: always return an empty list if nothing found
		return Collections.emptyList();
	}

	/**
	 * Updates the tool response with engine and tool metadata, using the per-call
	 * reverse-lookup map ({@link #toolLookupByLLMName}) that was populated during
	 * the most recent call to {@link #getAllToolsJsonForRoom(int)}. Falls back to
	 * UUID-regex parsing for legacy full-UUID-prefix names.
	 *
	 * @param response the response message to enrich
	 */
	public void updateToolResponseMeta(ResponseMessage response) {
		MCPUtility.updateToolResponseWithProjectMeta(response, null, getToolLookupByLLMName());
	}

	/**
	 * Returns the current per-call reverse-lookup map (LLM-facing name -> enriched
	 * tool entry). The map is rebuilt each time
	 * {@link #getAllToolsJsonForRoom(int)} is called.
	 *
	 * @return unmodifiable view of the lookup map
	 */
	public Map<String, Map<String, Object>> getToolLookupByLLMName() {
		return Collections.unmodifiableMap(toolLookupByLLMName);
	}

	/**
	 * Undoes the name aliasing applied when the tools were shown to the model.
	 *
	 * <p>
	 * {@link MCPUtility#appendEngineIdToToolsMethodName(String, JSONObject, int)}
	 * both prefixes a tool name with its engine id and, for providers that cap tool
	 * name length, truncates it. Stripping the prefix reverses only the first half;
	 * the truncation is lossy. This resolves the real name from the same map that
	 * recorded it, so callers get an exact name back instead of pattern matching a
	 * shortened one.
	 *
	 * <p>
	 * The map is populated by {@link #getAllToolsJsonForRoom(int)} and lives on the
	 * cached Room, which outlives the HTTP insight that built it, so it is normally
	 * still warm on the follow-up request that executes the tool. When it is not,
	 * the name is returned unchanged and the caller falls back to matching.
	 *
	 * @param llmFacingName the name as the model called it
	 * @return the original tool name, or the input unchanged when unknown
	 */
	@SuppressWarnings("unchecked")
	public String resolveOriginalToolName(String llmFacingName) {
		if (llmFacingName == null || llmFacingName.isBlank()) {
			return llmFacingName;
		}
		Map<String, Object> entry = toolLookupByLLMName.get(llmFacingName);
		if (entry == null) {
			return llmFacingName;
		}
		Object rawMeta = entry.get("_meta");
		if (rawMeta instanceof Map) {
			Object original = ((Map<String, Object>) rawMeta).get(MCPUtility.SMSS_ORIGINAL_TOOL_NAME);
			if (original instanceof String && !((String) original).isBlank()) {
				return (String) original;
			}
		}
		return llmFacingName;
	}

	/**
	 * Checks whether the specified message id belongs to an assistant-authored
	 * output message in this room.
	 *
	 * @param messageId message id to validate
	 * @return {@code true} when a matching assistant output message exists
	 */
	public boolean isMessageAuthor(String messageId) {
		return getMessages().parallelStream().anyMatch(m -> m.getMessageId().equals(messageId)
				&& m instanceof prerna.engine.impl.model.message.ResponseMessage);
	}

	// --- System Prompt Handling ----

	/**
	 * Resolves the user-authored system prompt - the room/workspace layer, before
	 * the enterprise template wrap or {@code {{VAR}}} expansion. Precedence:
	 * <ol>
	 * <li>{@code options.instructions}</li>
	 * <li>{@code workspace.system_prompt} (looked up via
	 * {@code options.workspace.workspace_id})</li>
	 * </ol>
	 *
	 * <p>
	 * Use this when you need the raw user prompt as a composable layer (e.g., a
	 * harness combining it with built-in agent instructions). Use
	 * {@link #getSystemPromptForModel()} for the final prompt sent to the model.
	 *
	 * @return authored prompt, or {@code null} if neither layer is set
	 * @throws IllegalArgumentException when workspace lookup fails access or
	 *                                  active-state checks
	 */
	public String getRoomOrWorkspaceSystemPrompt() {
		String opts = getOptions();
		JsonObject optionsObj = null;
		if (opts != null && !opts.trim().isEmpty()) {
			try {
				optionsObj = JsonParser.parseString(opts).getAsJsonObject();
			} catch (Exception ignore) {
			}
		}
		if (optionsObj == null) {
			return null;
		}

		// 1. options.instructions
		JsonElement instructionsElem = optionsObj.get("instructions");
		if (instructionsElem != null && instructionsElem.isJsonPrimitive()) {
			String fromInstructions = StringUtils.trimToNull(instructionsElem.getAsString());
			if (fromInstructions != null) {
				return fromInstructions;
			}
		}

		// 2. workspace.system_prompt (by workspace_id in options)
		JsonElement workspaceElem = optionsObj.get("workspace");
		String workspaceId = null;
		if (workspaceElem != null) {
			if (workspaceElem.isJsonPrimitive()) {
				workspaceId = workspaceElem.getAsString();
			} else if (workspaceElem.isJsonObject()) {
				JsonElement idElem = workspaceElem.getAsJsonObject().get("workspace_id");
				if (idElem != null && idElem.isJsonPrimitive()) {
					workspaceId = idElem.getAsString();
				}
			}
		}
		if (workspaceId == null) {
			return null;
		}
		Map<String, Object> workspace = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (workspace == null) {
			return null;
		}
		User user = this.insight.getUser();
		if (!SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}
		Object isActive = workspace.get("is_active");
		if (Boolean.FALSE.equals(isActive)) {
			throw new IllegalArgumentException("Workspace is disabled by the owner");
		}
		return StringUtils.trimToNull((String) workspace.get("system_prompt"));
	}

	/**
	 * Returns the final system prompt this room should send to the model. Starts
	 * with the room/workspace-authored prompt, then applies any outer wrapper
	 * required by the room's runtime surface. Today only SYSTEM__PLAYGROUND has an
	 * outer wrapper: the active playground global system prompt from the admin
	 * theme.
	 *
	 * @return resolved system prompt, or {@code null} when no prompt is configured
	 * @throws IllegalArgumentException when workspace prompt resolution fails
	 *                                  access or active-state checks
	 */
	public String getSystemPromptForModel() {
		return applySystemPromptOuterWrapper(getRoomOrWorkspaceSystemPrompt());
	}

	/**
	 * Applies the outermost system prompt wrapper, when this room's runtime surface
	 * defines one.
	 *
	 * @param systemPrompt already-composed caller prompt
	 * @return prompt after project wrapper, or the original prompt when no wrapper
	 *         applies
	 */
	public String applySystemPromptOuterWrapper(String systemPrompt) {
		if (!usesPlaygroundSystemPromptWrapper()) {
			return systemPrompt;
		}
		String enterpriseTemplate = getEnterpriseSystemPromptTemplateFromActiveTheme();
		String merged = applyEnterpriseSystemPromptTemplate(enterpriseTemplate, systemPrompt);
		return expandSystemPromptVariables(merged);
	}

	/**
	 * Returns whether this room should use the playground/admin-theme global system
	 * prompt wrapper.
	 */
	public boolean usesPlaygroundSystemPromptWrapper() {
		return PlaygroundUtils.PLAYGROUND_PROJECT_ID.equals(this.projectId);
	}

	/**
	 * Merges an enterprise-wide system prompt template with the room-level prompt.
	 *
	 * @param enterpriseTemplate    enterprise template (may include
	 *                              {@code {{SYSTEM_PROMPT}}})
	 * @param effectiveSystemPrompt room/workspace prompt to merge into template
	 * @return merged prompt string, or {@code null} when no non-blank content
	 *         remains
	 */
	private static String applyEnterpriseSystemPromptTemplate(String enterpriseTemplate, String effectiveSystemPrompt) {
		enterpriseTemplate = StringUtils.trimToNull(enterpriseTemplate);
		effectiveSystemPrompt = StringUtils.trimToNull(effectiveSystemPrompt);
		if (enterpriseTemplate == null) {
			return effectiveSystemPrompt;
		}

		String base = effectiveSystemPrompt == null ? "" : effectiveSystemPrompt;

		boolean hasPlaceholder = enterpriseTemplate.contains("{{SYSTEM_PROMPT}}");
		String merged = enterpriseTemplate.replace("{{SYSTEM_PROMPT}}", base);

		if (!hasPlaceholder && !base.isEmpty()) {
			merged = StringUtils.trimToEmpty(enterpriseTemplate) + "\n\n" + base;
		}

		return StringUtils.trimToNull(merged);
	}

	/**
	 * Expands configured system-prompt variables in the given prompt string.
	 *
	 * @param systemPrompt prompt containing optional {@code {{VAR}}} placeholders
	 * @return prompt with resolved variable substitutions, or {@code null} when
	 *         input is blank
	 */
	private String expandSystemPromptVariables(String systemPrompt) {
		systemPrompt = StringUtils.trimToNull(systemPrompt);
		if (systemPrompt == null) {
			return null;
		}

		Matcher matcher = SYSTEM_PROMPT_VARIABLE_PATTERN.matcher(systemPrompt);
		if (!matcher.find()) {
			return systemPrompt;
		}

		Map<String, Object> cache = new HashMap<>();
		Map<String, String> configuredVars = PlaygroundThemeUtils.getPlaygroundSystemPromptVars();
		matcher.reset();

		StringBuffer out = new StringBuffer();
		while (matcher.find()) {
			String varName = matcher.group(1);
			String path = matcher.group(2);
			if (path != null && path.startsWith(".")) {
				path = path.substring(1);
			}

			String replacement = resolveSystemPromptVariableToString(varName, path, configuredVars, cache);
			if (replacement == null) {
				replacement = matcher.group(0);
			}
			matcher.appendReplacement(out, Matcher.quoteReplacement(replacement));
		}
		matcher.appendTail(out);
		return StringUtils.trimToNull(out.toString());
	}

	/**
	 * Resolves a configured prompt variable (and optional object path) to its
	 * string representation.
	 *
	 * @param varName        configured variable name
	 * @param path           optional dotted/indexed path off the root variable
	 *                       value
	 * @param configuredVars configured variable map (variable name to pixel
	 *                       expression)
	 * @param cache          per-resolution cache to avoid duplicate pixel execution
	 * @return resolved string value, or {@code null} if unresolved
	 */
	private String resolveSystemPromptVariableToString(String varName, String path, Map<String, String> configuredVars,
			Map<String, Object> cache) {
		Object rootVal = cache.containsKey(varName) ? cache.get(varName)
				: computeSystemPromptVariable(varName, configuredVars);
		cache.put(varName, rootVal);
		Object val = rootVal;
		if (val != null && path != null) {
			val = resolvePath(val, path);
		}
		return stringifyPromptValue(val);
	}

	/**
	 * Computes a configured system-prompt variable by executing its configured meta
	 * pixel.
	 *
	 * @param varName        configured variable name
	 * @param configuredVars configured variable map
	 * @return resolved variable value, or {@code null} if missing/unresolvable
	 */
	private Object computeSystemPromptVariable(String varName, Map<String, String> configuredVars) {
		if (varName == null) {
			return null;
		}

		if (configuredVars != null) {
			String pixel = StringUtils.trimToNull(configuredVars.get(varName));
			if (pixel != null) {
				return runMetaPixelForValue(pixel);
			}
		}

		return null;
	}

	/**
	 * Executes a single-statement META pixel and returns the value from the final
	 * noun result.
	 *
	 * @param pixel raw pixel expression
	 * @return extracted result value, or {@code null} on validation/exec failure
	 */
	private Object runMetaPixelForValue(String pixel) {
		if (this.insight == null) {
			return null;
		}
		pixel = StringUtils.trimToNull(pixel);
		if (pixel == null) {
			return null;
		}
		pixel = normalizeAndValidateSingleStatementPixel(pixel);
		if (pixel == null) {
			return null;
		}
		try {
			PixelRunner runner = this.insight.runPixel("META|" + pixel);
			List<NounMetadata> results = runner == null ? null : runner.getResults();
			if (results == null || results.isEmpty()) {
				return null;
			}
			NounMetadata last = results.get(results.size() - 1);
			return last == null ? null : last.getValue();
		} catch (Exception e) {
			classLogger.debug("Failed to execute META pixel while resolving system prompt variable. Pixel={}", pixel,
					e);
			return null;
		}
	}

	/**
	 * Normalizes and validates that the supplied pixel is a safe single statement.
	 *
	 * @param pixel raw pixel expression
	 * @return normalized pixel ending with {@code ;}, or {@code null} if invalid
	 */
	private static String normalizeAndValidateSingleStatementPixel(String pixel) {
		pixel = StringUtils.trimToNull(pixel);
		if (pixel == null) {
			return null;
		}
		if (!pixel.endsWith(";")) {
			pixel = pixel + ";";
		}
		int firstSemi = pixel.indexOf(';');
		if (firstSemi != pixel.length() - 1) {
			return null;
		}
		if (!SAFE_SINGLE_STATEMENT_PIXEL.matcher(pixel).matches()) {
			return null;
		}
		return pixel;
	}

	/**
	 * Resolves a dotted/indexed path (for example {@code a.b[0].c}) from the
	 * provided root object.
	 *
	 * @param root root value to traverse
	 * @param path dotted/indexed path
	 * @return resolved object, or {@code null} when traversal fails
	 */
	private static Object resolvePath(Object root, String path) {
		if (root == null || path == null || path.isBlank()) {
			return root;
		}
		Object cur = root;
		int i = 0;
		while (i < path.length()) {
			char ch = path.charAt(i);
			if (ch == '.') {
				i++;
				continue;
			}
			if (ch == '[') {
				int close = path.indexOf(']', i + 1);
				if (close < 0) {
					return null;
				}
				String idxStr = path.substring(i + 1, close).trim();
				int idx;
				try {
					idx = Integer.parseInt(idxStr);
				} catch (Exception e) {
					return null;
				}
				cur = resolveIndex(cur, idx);
				i = close + 1;
				continue;
			}

			int start = i;
			while (i < path.length()) {
				char c = path.charAt(i);
				if (c == '.' || c == '[') {
					break;
				}
				i++;
			}
			String key = path.substring(start, i);
			cur = resolveKey(cur, key);
		}
		return cur;
	}

	/**
	 * Resolves a map/JSON-object key on the current traversal value.
	 *
	 * @param cur current traversal object
	 * @param key key name to resolve
	 * @return resolved value, or {@code null} when key/object is unsupported
	 */
	private static Object resolveKey(Object cur, String key) {
		if (cur == null) {
			return null;
		}
		if (cur instanceof Map<?, ?>) {
			@SuppressWarnings("unchecked")
			Map<String, Object> m = (Map<String, Object>) cur;
			return m.get(key);
		}
		if (cur instanceof JsonObject) {
			JsonElement e = ((JsonObject) cur).get(key);
			if (e == null || e.isJsonNull()) {
				return null;
			}
			if (e.isJsonPrimitive()) {
				return e.getAsString();
			}
			return e;
		}
		if (cur instanceof JsonElement) {
			JsonElement e = (JsonElement) cur;
			if (e.isJsonObject()) {
				return resolveKey(e.getAsJsonObject(), key);
			}
		}
		return null;
	}

	/**
	 * Resolves a list/array index on the current traversal value.
	 *
	 * @param cur current traversal object
	 * @param idx zero-based index
	 * @return resolved value, or {@code null} when index/object is unsupported
	 */
	private static Object resolveIndex(Object cur, int idx) {
		if (cur == null || idx < 0) {
			return null;
		}
		if (cur instanceof List<?>) {
			List<?> l = (List<?>) cur;
			return idx < l.size() ? l.get(idx) : null;
		}
		if (cur instanceof JsonElement) {
			JsonElement e = (JsonElement) cur;
			if (e.isJsonArray()) {
				if (idx >= e.getAsJsonArray().size()) {
					return null;
				}
				JsonElement out = e.getAsJsonArray().get(idx);
				if (out == null || out.isJsonNull()) {
					return null;
				}
				if (out.isJsonPrimitive()) {
					return out.getAsString();
				}
				return out;
			}
		}
		return null;
	}

	/**
	 * Converts a resolved prompt variable value into a prompt-safe string.
	 *
	 * @param val value to stringify
	 * @return stringified value, or {@code null} for null/blank string values
	 */
	private static String stringifyPromptValue(Object val) {
		if (val == null) {
			return null;
		}
		if (val instanceof String) {
			return StringUtils.trimToNull((String) val);
		}
		if (val instanceof Number || val instanceof Boolean) {
			return String.valueOf(val);
		}
		if (val instanceof JsonElement) {
			return ((JsonElement) val).isJsonPrimitive() ? ((JsonElement) val).getAsString() : GSON.toJson(val);
		}
		if (val instanceof Map<?, ?> || val instanceof List<?>) {
			return GSON.toJson(val);
		}
		return StringUtils.trimToNull(String.valueOf(val));
	}

	/**
	 * Reads the enterprise/global system prompt template from the active theme.
	 *
	 * @return enterprise prompt template, or {@code null} if unavailable
	 */
	private static String getEnterpriseSystemPromptTemplateFromActiveTheme() {
		try {
			return PlaygroundThemeUtils.getPlaygroundGlobalSystemPrompt();
		} catch (Exception ignore) {
		}
		return null;
	}

	// ---- Getters and Setters ----

	/**
	 * Returns the room identifier.
	 *
	 * @return room id
	 */
	public String getId() {
		return room_id;
	}

	/**
	 * Sets the room identifier.
	 *
	 * @param id room id
	 */
	public void setId(String id) {
		this.room_id = id;
	}

	/**
	 * Returns the owning user id.
	 *
	 * @return user id
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * Sets the owning user id.
	 *
	 * @param userId user id
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}

	/**
	 * Returns the room display name.
	 *
	 * @return room name
	 */
	public String getRoomName() {
		return roomName;
	}

	/**
	 * Sets the room display name.
	 *
	 * @param roomName room name
	 */
	public void setRoomName(String roomName) {
		this.roomName = roomName;
	}

	/**
	 * Returns the room share id.
	 *
	 * @return share id
	 */
	public String getShareId() {
		return shareId;
	}

	/**
	 * Sets the room share id.
	 *
	 * @param shareId share id
	 */
	public void setShareId(String shareId) {
		this.shareId = shareId;
	}

	/**
	 * Indicates whether the room is active.
	 *
	 * @return {@code true} if active
	 */
	public boolean isActive() {
		return isActive;
	}

	/**
	 * Sets whether the room is active.
	 *
	 * @param isActive active flag
	 */
	public void isActive(boolean isActive) {
		this.isActive = isActive;
	}

	/**
	 * Returns the room creation timestamp.
	 *
	 * @return creation timestamp
	 */
	public Timestamp getCreatedAt() {
		return createdAt;
	}

	/**
	 * Sets the room creation timestamp.
	 *
	 * @param createdAt creation timestamp
	 */
	public void setCreatedAt(Timestamp createdAt) {
		this.createdAt = createdAt;
	}

	/**
	 * Returns the room last-updated timestamp.
	 *
	 * @return last-updated timestamp
	 */
	public Timestamp getUpdatedAt() {
		return updatedAt;
	}

	/**
	 * Sets the room last-updated timestamp.
	 *
	 * @param updatedAt last-updated timestamp
	 */
	public void setUpdatedAt(Timestamp updatedAt) {
		this.updatedAt = updatedAt;
	}

	/**
	 * Indicates whether the room is pinned.
	 *
	 * @return {@code true} if pinned
	 */
	public boolean isPinned() {
		return pinned;
	}

	/**
	 * Sets whether the room is pinned.
	 *
	 * @param pinned pinned flag
	 */
	public void setPinned(boolean pinned) {
		this.pinned = pinned;
	}

	/**
	 * Returns raw room options JSON.
	 *
	 * @return options JSON
	 */
	public String getOptions() {
		return options;
	}

	/**
	 * Sets raw room options JSON.
	 *
	 * @param options options JSON
	 */
	public void setOptions(String options) {
		this.options = options;
	}

	/**
	 * Returns parsed room options, lazily deserializing from {@link #options} when
	 * needed.
	 *
	 * @return mutable options map (never {@code null})
	 */
	public Map<String, Object> getOptionsMap() {
		if (optionsMap == null) {
			if (options != null && !options.trim().isEmpty()) {
				try {
					optionsMap = GSON.fromJson(options, new TypeToken<Map<String, Object>>() {
					}.getType());
				} catch (Exception e) {
					optionsMap = new HashMap<>();
				}
			} else {
				optionsMap = new HashMap<>();
			}
		}
		return optionsMap;
	}

	/**
	 * Sets room options from a map and updates the serialized options JSON.
	 *
	 * @param map options map
	 */
	public void setOptionsMap(Map<String, Object> map) {
		this.optionsMap = map;
		this.options = map == null ? null : GSON.toJson(map);
	}

	/**
	 * Returns the room model id.
	 *
	 * @return model id
	 */
	public String getModelId() {
		return modelId;
	}

	/**
	 * Sets the room model id.
	 *
	 * @param modelId model id
	 */
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}

	public String getParentRoomId() {
		return parentRoomId;
	}

	public void setParentRoomId(String parentRoomId) {
		this.parentRoomId = parentRoomId;
	}

	/**
	 * Returns the room folder path used for media persistence.
	 *
	 * @return room folder path
	 */
	public String getRoomFolderPath() {
		return roomFolderPath;
	}

	/**
	 * Resolves a room's folder from its id, for callers that need the path without
	 * loading the room.
	 *
	 * @param roomId the room id
	 * @return the room folder path
	 */
	public static String roomFolderPath(String roomId) {
		return Utility.getBaseFolder() + File.separator + "room" + File.separator + roomId;
	}

	// Core message accessors
	/**
	 * Returns the in-memory room message list.
	 *
	 * @return mutable message list
	 */
	public List<AbstractMessage> getMessages() {
		return this.messages;
	}

	/**
	 * Replaces the in-memory room message list.
	 *
	 * @param messagesList message list to set (null clears messages)
	 */
	public void setMessages(List<AbstractMessage> messagesList) {
		this.messages.clear();
		if (messagesList != null) {
			this.messages.addAll(messagesList);
		}
	}

	/**
	 * Serializes current room messages to DB-safe JSON and updates cached
	 * {@link #messagesJson}.
	 *
	 * @return serialized message JSON
	 */
	public String getMessagesAsString() {
		this.messagesJson = MessageUtils.toJsonArray(messages);
		return this.messagesJson;
	}

	/**
	 * Serializes current room messages to JSON including inline image data for
	 * model execution payloads.
	 *
	 * @return serialized message JSON with image data
	 */
	public String getMessagesWithImageDataAsString() {
		return MessageUtils.toJsonArrayWithImageData(messages);
	}

	/**
	 * Deserializes persisted message JSON into the in-memory message list.
	 *
	 * @param messagesJson persisted message JSON from the ROOM table
	 */
	public void setMessagesFromString(String messagesJson) {
		// Pull room folder - Room folder is at BASE_FOLDER/roomid
		ClusterUtil.pullRoom(this.room_id);
		if (messagesJson == null || messagesJson.trim().isEmpty()) {
			this.setMessages(new ArrayList<>());
			return;
		}
		List<AbstractMessage> loaded = RoomMessageStore.loadFromPersistedJson(this, messagesJson);

		this.setMessages(loaded != null ? loaded : new ArrayList<>());
	}

	/**
	 * Returns the insight context currently attached to this room.
	 *
	 * @return insight context
	 */
	public Insight getInsight() {
		return insight;
	}

	/**
	 * Sets the insight context for this room.
	 *
	 * @param insight insight context
	 */
	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	/**
	 * Returns the cached serialized message JSON value.
	 *
	 * @return serialized messages JSON
	 */
	public String getMessageJson() {
		return this.messagesJson;
	}

	/**
	 * Sets the cached serialized message JSON value directly.
	 * <p>
	 * Prefer mutating {@link #messages} and using {@link #getMessagesAsString()}
	 * unless synchronizing with externally produced JSON.
	 *
	 * @param messagesJson serialized messages JSON
	 */
	public void setMessagesJson(String messagesJson) {
		this.messagesJson = messagesJson;
	}

	/**
	 * Returns the associated project id.
	 *
	 * @return project id
	 */
	public String getProjectId() {
		return projectId;
	}

	/**
	 * Sets the associated project id.
	 *
	 * @param projectId project id
	 */
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

}
