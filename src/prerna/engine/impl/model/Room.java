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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
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
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.PlaygroundThemeUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class Room {

	private static final Logger classLogger = LogManager.getLogger(Room.class);

	protected static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private static final Pattern SYSTEM_PROMPT_VARIABLE_PATTERN = Pattern
			.compile("\\{\\{\\s*([A-Z][A-Z0-9_]*)\\s*((?:\\.|\\[)[^}]*)?\\s*\\}\\}");
	private static final Pattern SAFE_SINGLE_STATEMENT_PIXEL = Pattern
			.compile("^\\s*[A-Za-z_][A-Za-z0-9_]*\\s*\\(.*\\)\\s*;?\\s*$", Pattern.DOTALL);

	static final String SEARCH_TOOLS_NAME = "search_tools";
	static final int DEFERRED_SEARCH_MAX_ROUNDS = 3;
	static final int DEFERRED_SEARCH_RESULTS_LIMIT = 5;

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
	private transient Map<String, Map<String, Object>> toolCatalog; // lazy-loaded in deferred mode
	private transient Set<String> discoveredToolNames = new LinkedHashSet<>(); // grows as LLM searches

	private String modelId;
	private String messagesJson;

	private Insight insight;
	private String roomFolderPath;

	public Room() {
	}

	// Use this constructor if you want to load from JSON (as from DB)
	public Room(String room_id, String userId, String roomName, String systemMessage, String projectId, String shareId,
			boolean isActive, Timestamp createdAt, Timestamp updatedAt, String messagesJson, boolean pinned,
			String options, String modelId) {
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
		this.messagesJson = messagesJson;
		this.roomFolderPath = Utility.getBaseFolder() + File.separator + "room" + File.separator + this.room_id;

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

	public void parseMessages() {
		setMessagesFromString(this.messagesJson);
	}

	/**
	 * 
	 * @param msg
	 * @param systemMessage
	 * @param modelEngine
	 * @return
	 */
	public ResponseMessage ask(InputMessage msg, IModelEngine modelEngine) {
		return ask(msg, modelEngine, null);
	}

	public ResponseMessage ask(InputMessage msg, IModelEngine modelEngine, String parentMessageId) {
		Boolean appendToHistory = true;
		return ask(msg, modelEngine, parentMessageId, appendToHistory);
	}

	/**
	 * 
	 * @param msg
	 * @param systemMessage
	 * @param modelEngine
	 * @param parentMessageId
	 * @return
	 */
	public synchronized ResponseMessage ask(InputMessage msg, IModelEngine modelEngine, String parentMessageId,
			Boolean appendToHistory) {

		Map<String, Object> kwArgMap = new HashMap<>(msg.getParamMap());

		// if it is full prompt, process that first.
		if (kwArgMap.containsKey(AbstractModelEngine.FULL_PROMPT)) {
			AskModelEngineResponse llmResponse = modelEngine.askRoom(msg.getInputPrompt(), this, msg, kwArgMap);
			ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(llmResponse).build();
			response.setRoom(this);
			MessageUtils.persistMediaPartsToRoomFolder(response, this);
			return response;
		}

		// if a specific system message is sent to use, overwrite the existing in the db
		if (msg.getSystemPrompt() != null) {
			ModelInferenceLogsUtils.setRoomContext(this.insight.getInsightId(),
					this.insight.getUser().getPrimaryLoginToken().getId(), msg.getSystemPrompt());
		}

		// Reset deferred-tool state at the start of each new user turn.
		// toolCatalog is rebuilt fresh (matching the non-deferred path that always calls
		// getAllToolsJsonForRoom() per request) so MCP changes are picked up immediately.
		// discoveredToolNames is cleared so the model always starts with only search_tools.
		if (isDeferredToolLoadingEnabled()) {
			toolCatalog = null;
			discoveredToolNames.clear();
		}
		appendToolsToParams(kwArgMap);

		// Determine useHistory: default true unless "use_history" is Boolean.FALSE or
		// string "false"
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
			ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(llmResponse).build();

			// set transaction id for both pieces
			msg.setTransactionId(llmResponse.getMessageId());
			msg.setTokensInMessage(llmResponse.getNumberOfTokensInPrompt());
			response.setTransactionId(llmResponse.getMessageId());

			response.setModel(modelEngine);
			response.setRoom(this);
			response.setParentMessageId(msg.getMessageId());
			response.setTokensInMessage(llmResponse.getNumberOfTokensInResponse());
			MessageUtils.persistMediaPartsToRoomFolder(response, this);
			return response;
		}

		// if we dont have to keep history. then wipe all previous messages.
		if (!modelEngine.keepsConversationHistory()) {
			messages.clear();
		}

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

		List<AbstractMessage> deferredMessages = new ArrayList<>();
		ResponseMessage response = null;
		try {
			String messageJsonString = MessageUtils.getMessageHistoryWithNewMessage(this.messages, msg);
			kwArgMap.put("message_json", messageJsonString);

			AskModelEngineResponse llmResponse = modelEngine.askRoom(msg.getInputPrompt(), this, msg, kwArgMap);

			// If deferred tool loading is active and the LLM called search_tools, resolve it server-side
			if (isDeferredToolLoadingEnabled()
					&& llmResponse.getMessageType().equals(AskModelEngineResponse.TOOL)
					&& isAllSearchToolsCalls((AskToolModelEngineResponse) llmResponse)) {
				llmResponse = runSearchToolsLoop(llmResponse, msg, modelEngine, kwArgMap, deferredMessages);
			}

			response = ResponseMessage.Builder.fromAskModelEngineResponse(llmResponse).build();
			response.setMessageId(llmResponse.getMessageId());

			// set transaction id for both pieces
			msg.setTransactionId(llmResponse.getMessageId());
			msg.setTokensInMessage(llmResponse.getNumberOfTokensInPrompt());
			response.setTransactionId(llmResponse.getMessageId());

			// Create the assistant's response message and add to history
			response.setModel(modelEngine);
			response.setRoom(this);
			response.setParentMessageId(msg.getMessageId());
			response.setTokensInMessage(llmResponse.getNumberOfTokensInResponse());
			MessageUtils.persistMediaPartsToRoomFolder(response, this);
		} catch (Exception e) {
			classLogger.error("Error running new message in room", e);
			throw e;
		}
		// on success, add the message
		if (appendToHistory) {
			messages.add(msg);
			messages.addAll(deferredMessages); // intermediate search_tools exchanges (may be empty)
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
				ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id,
						insight.getUser().getPrimaryLoginToken().getId(), getMessagesAsString(), this.roomName,
						modelEngine.getEngineId());
			} else {
				// Otherwise, regular update
				ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id,
						insight.getUser().getPrimaryLoginToken().getId(), getMessagesAsString());
			}
		}
		return response;
	}

	/**
	 * 
	 * @param toolCallId
	 * @param toolName
	 * @param toolExecutionResponse
	 * @param toolParameterValues
	 * @param parentMessageId
	 * @param modelEngine
	 * @param insight
	 * @return
	 */
	public synchronized AskModelEngineResponse addToolExecutionResult(String toolCallId, String toolName,
			String toolExecutionResponse, Map<String, Object> toolParameterValues, Map<String, Object> paramValuesMap,
			String parentMessageId, IModelEngine modelEngine, Insight insight, String toolStatus) {
		if (messages.isEmpty()) {
			throw new IllegalStateException("No messages to match tool call context");
		}

		String lastMessageId = null;
		if (parentMessageId != null && !parentMessageId.isEmpty()) {
			lastMessageId = parentMessageId;
		} else {
			// if no parent message id is passed in, use the last message as the parent.
			AbstractMessage lastMsg = messages.get(messages.size() - 1);
			lastMessageId = lastMsg.getMessageId();
		}

		// 1. Find the last RESPONSE_TOOL message (assistant tool_calls)
		ResponseMessage toolResponse = null;
		List<AbstractMessage> branchMessages = MessageUtils.getMessageBranchFromParent(messages, lastMessageId);
		for (int i = branchMessages.size() - 1; i >= 0; --i) {
			AbstractMessage m = branchMessages.get(i);
			// Stop if a user or assistant non-tool-response appears
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

		// 2. Confirm tool_call_id is present in that RESPONSE_TOOL message's tool_calls
		Map<String, Object> matchingToolCall = null;
		for (Map<String, Object> toolCall : toolResponse.getToolResponses()) {
			String thisId = String.valueOf(toolCall.get("id"));
			if (toolCallId.equals(thisId)) {
				matchingToolCall = toolCall;
				break;
			}
		}
		if (matchingToolCall == null) {
			throw new IllegalArgumentException("No matching tool_call_id in last assistant tool_calls response.");
		}

		// 3. Add tool execution result as a part to a single pending input message
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

		boolean isToolResultsInputMessage = false;
		InputMessage toolResultsMessage = null;
		for (int i = messages.size() - 1; i > toolResponseIdx; --i) {
			AbstractMessage m = messages.get(i);
			if (m instanceof ResponseMessage) {
				break;
			}
			if (m instanceof InputMessage && m.hasToolResultPart()
					&& toolResponse.getMessageId().equals(m.getParentMessageId())) {
				toolResultsMessage = (InputMessage) m;
				break;
			}
		}

		if (toolResultsMessage == null) {
			isToolResultsInputMessage = true;
			toolResultsMessage = InputMessage.builder(this).withSystemPrompt(this.getEffectiveSystemPrompt())
					.withToolResult(toolCallId, toolName, toolExecutionResponse, toolParameterValues, toolStatus)
					.withModelType(modelEngine.getModelType()).build();
			toolResultsMessage.setParentMessageId(toolResponse.getMessageId());
			toolResultsMessage.setModel(modelEngine);
			toolResultsMessage.setVisibile(false);
		} else {
			toolResultsMessage.addPart(new ToolResultMessagePart(
					new ToolResultPart(toolCallId, toolName, toolExecutionResponse, toolParameterValues, toolStatus)));
			toolResultsMessage.normalizeForWrite();
		}

		// 4. After the last RESPONSE_TOOL, gather all tool execution messages for that
		// context
		Set<String> allIds = new HashSet<>();
		for (Map<String, Object> tc : toolResponse.getToolResponses()) {
			allIds.add(String.valueOf(tc.get("id")));
		}

		Set<String> answeredIds = new HashSet<>();
		// add this new tool call id
		answeredIds.add(toolCallId);
		// scan forward from toolResponse idx+1 to the end
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
				// legacy fallback
				if (m instanceof InputMessage) {
					String legacyId = ((InputMessage) m).getToolCallId();
					if (legacyId != null) {
						answeredIds.add(legacyId);
					}
				}
			}
		}

		if (isToolResultsInputMessage) {
			// add to the messages array if it is new
			messages.add(toolResultsMessage);
		}
		// 5. If all tool_call_ids fulfilled, trigger next model.ask
		// otherwise, we add the message and save the result
		if (!answeredIds.containsAll(allIds) || allIds.size() == 0) {
			// persist the new message (or just the part) into the room
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id, insight.getUser().getPrimaryLoginToken().getId(),
					getMessagesAsString());
		} else {
			// we have to add the tool results into the message history
			// so that we can track for multiple tools
			// so we are calling getting the current message history as the messages array
			// already has the result
			// as opposed to the normal
			// getMessageHistoryWithNewMessage(List<AbstractMessage> messages,
			// AbstractMessage newMessage) method
			String messageJsonString = MessageUtils.getCurrentMessageHistory(this.messages);
			if (paramValuesMap == null) {
				paramValuesMap = new HashMap<>();
			}
			paramValuesMap.put("message_json", messageJsonString);
			appendToolsToParams(paramValuesMap);

			AskModelEngineResponse llmResponse = null;
			ResponseMessage nextAssistant = null;
			try {
				llmResponse = modelEngine.askRoom("", this, toolResultsMessage, paramValuesMap);
				// If deferred tool loading is active and the LLM called search_tools, resolve it server-side
				if (isDeferredToolLoadingEnabled()
						&& llmResponse != null
						&& llmResponse.getMessageType().equals(AskModelEngineResponse.TOOL)
						&& isAllSearchToolsCalls((AskToolModelEngineResponse) llmResponse)) {
					List<AbstractMessage> deferredMessages = new ArrayList<>();
					llmResponse = runSearchToolsLoop(llmResponse, toolResultsMessage, modelEngine, paramValuesMap, deferredMessages);
					messages.addAll(deferredMessages);
				}
				nextAssistant = createResponseMessage(llmResponse);
				nextAssistant.setParentMessageId(toolResultsMessage.getMessageId());
				nextAssistant.setModel(modelEngine);
				nextAssistant.setTokensInMessage(llmResponse.getNumberOfTokensInResponse());
			} catch (Exception e) {
				// remove the last tool since it failed
				toolResultsMessage.getParts().removeLast();
				classLogger.error("Error adding tool result and getting model response", e);
				throw e;
			}
			// we have already added to the messages above
			// we dont need to add again, only the response
//			messages.add(toolResultsMessage);
			messages.add(nextAssistant);

			// --------- BEGIN TRANSACTION ID PROPAGATION ---------
			// Step 1: retrieve or create transactionId from nextAssistant
			String transactionId = nextAssistant.getTransactionId();
			if (transactionId == null || transactionId.isEmpty()) {
				transactionId = GUID.v7().toUUID().toString();
				nextAssistant.setTransactionId(transactionId);
			}

			// Find all INPUT_TOOL_EXECs after toolResponse up through nextAssistant
			// (exclusive)
			for (int i = toolResponseIdx + 1; i < messages.size(); ++i) {
				AbstractMessage m = messages.get(i);
				if (m == nextAssistant) {
					break; // Stop at nextAssistant (exclusive)
				}
				if (m == toolResultsMessage) {
					m.setTransactionId(transactionId);
				}
			}
			// --------- END TRANSACTION ID PROPAGATION ---------

			ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id, insight.getUser().getPrimaryLoginToken().getId(),
					getMessagesAsString());

			return llmResponse;
		}
		// Not all tool_calls fulfilled yet
		return null;
	}

	/**
	 * 
	 * @param parentMessageId
	 * @return
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

	private void appendToolsToParams(Map<String, Object> params) {
		if (!isDeferredToolLoadingEnabled()) {
			// Existing behavior: send all tools eagerly
			List<Map<String, Object>> newTools = getAllToolsJsonForRoom();
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
			return;
		}
		// Deferred mode: lazy-load catalog, send search_tools + previously discovered tools only
		if (toolCatalog == null) {
			toolCatalog = buildToolCatalog();
		}
		List<Map<String, Object>> toolsToSend = new ArrayList<>();
		toolsToSend.add(buildSearchToolDefinition());
		for (String name : discoveredToolNames) {
			Map<String, Object> def = toolCatalog.get(name);
			if (def != null) {
				toolsToSend.add(def);
			}
		}
		params.put("tools", toolsToSend);
	}

	private boolean isDeferredToolLoadingEnabled() {
		Map<String, Object> o = getOptionsMap();
		Object flag = o.get("deferred_tool_loading");
		if (flag instanceof Boolean) {
			return (Boolean) flag;
		} else if (flag instanceof String) {
			return "true".equalsIgnoreCase((String) flag);
		}
		return false;
	}

	private Map<String, Map<String, Object>> buildToolCatalog() {
		List<Map<String, Object>> allTools = getAllToolsJsonForRoom();
		Map<String, Map<String, Object>> catalog = new LinkedHashMap<>();
		for (Map<String, Object> tool : allTools) {
			Object nameObj = tool.get("name");
			if (nameObj instanceof String) {
				catalog.put((String) nameObj, tool);
			}
		}
		return catalog;
	}

	private static Map<String, Object> buildSearchToolDefinition() {
		Map<String, Object> tool = new LinkedHashMap<>();
		tool.put("name", SEARCH_TOOLS_NAME);
		tool.put("description",
				"Search for available tools by name or purpose. Returns matching tool definitions "
				+ "that you can then call. Use this to discover what tools exist before calling them.");
		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		Map<String, Object> props = new LinkedHashMap<>();
		Map<String, Object> queryProp = new LinkedHashMap<>();
		queryProp.put("type", "string");
		queryProp.put("description", "Keywords or description of the capability you need");
		props.put("query", queryProp);
		schema.put("properties", props);
		schema.put("required", List.of("query"));
		tool.put("inputSchema", schema);
		return tool;
	}

	/**
	 * If the LLM called search_tools (deferred loading mode), executes the search server-side,
	 * accumulates intermediate messages into {@code accumulator}, and re-calls the LLM — looping
	 * until it stops calling search_tools or max rounds are reached.
	 * <p>
	 * Intermediate messages are placed in {@code accumulator} so the caller can commit them to
	 * {@code this.messages} only on success.
	 */
	private AskModelEngineResponse runSearchToolsLoop(AskModelEngineResponse llmResponse,
			AbstractMessage parentMsg, IModelEngine modelEngine, Map<String, Object> params,
			List<AbstractMessage> accumulator) {

		// In ask(), the triggering user message is not yet in this.messages — it is
		// committed only on success. Capture it now so we can include it in each
		// rebuilt message_json, otherwise the model loses the user's original intent.
		final AbstractMessage originalMsg = parentMsg;

		int rounds = 0;
		while (rounds < DEFERRED_SEARCH_MAX_ROUNDS
				&& llmResponse.getMessageType().equals(AskModelEngineResponse.TOOL)
				&& isAllSearchToolsCalls((AskToolModelEngineResponse) llmResponse)) {
			rounds++;
			AskToolModelEngineResponse toolResp = (AskToolModelEngineResponse) llmResponse;

			// 1. Record the assistant's search_tools call
			ResponseMessage assistantMsg = ResponseMessage.toolResponses(toolResp.getToolResponse());
			assistantMsg.setParentMessageId(parentMsg.getMessageId());
			assistantMsg.setModel(modelEngine);
			accumulator.add(assistantMsg);

			// 2. Execute each search_tools call and build a combined tool-result message
			InputMessage toolResultMsg = null;
			for (Map<String, Object> tc : toolResp.getToolResponse()) {
				String callId = (String) tc.get("id");
				if (!SEARCH_TOOLS_NAME.equals(tc.get("name"))) {
					continue;
				}
				@SuppressWarnings("unchecked")
				Map<String, Object> args = (Map<String, Object>) tc.get("arguments");
				String query = (args != null && args.get("query") instanceof String)
						? (String) args.get("query") : "";

				List<Map<String, Object>> found = MCPUtility.searchToolCatalog(toolCatalog, query,
						DEFERRED_SEARCH_RESULTS_LIMIT);
				for (Map<String, Object> t : found) {
					if (t.get("name") instanceof String) {
						discoveredToolNames.add((String) t.get("name"));
					}
				}
				String resultJson = GSON.toJson(found);

				if (toolResultMsg == null) {
					toolResultMsg = InputMessage.builder(this).withSystemPrompt(this.getEffectiveSystemPrompt())
							.withToolResult(callId, SEARCH_TOOLS_NAME, resultJson, args, "success")
							.withModelType(modelEngine.getModelType()).build();
					toolResultMsg.setParentMessageId(assistantMsg.getMessageId());
					toolResultMsg.setModel(modelEngine);
					toolResultMsg.setVisibile(false);
				} else {
					toolResultMsg.addPart(new ToolResultMessagePart(
							new ToolResultPart(callId, SEARCH_TOOLS_NAME, resultJson, args, "success")));
				}
			}
			if (toolResultMsg == null) {
				break;
			}
			accumulator.add(toolResultMsg);

			// 3. Rebuild message_json: committed history + original triggering message
			//    (if not yet committed, as is the case in ask()) + accumulated exchanges
			List<AbstractMessage> allMessages = new ArrayList<>(this.messages);
			boolean originalMsgCommitted = this.messages.stream().anyMatch(m -> m == originalMsg);
			if (!originalMsgCommitted) {
				allMessages.add(originalMsg);
			}
			allMessages.addAll(accumulator);
			params.put("message_json", MessageUtils.toJsonArrayWithImageData(allMessages));

			// 4. Refresh tool list (newly discovered tools are now included)
			appendToolsToParams(params);

			// 5. Re-call LLM
			llmResponse = modelEngine.askRoom("", this, toolResultMsg, params);
			parentMsg = toolResultMsg;
		}
		return llmResponse;
	}

	/** Returns true only if every tool call in the response is a search_tools call. */
	private static boolean isAllSearchToolsCalls(AskToolModelEngineResponse toolResp) {
		List<Map<String, Object>> calls = toolResp.getToolResponse();
		if (calls == null || calls.isEmpty()) {
			return false;
		}
		return calls.stream().allMatch(tc -> SEARCH_TOOLS_NAME.equals(tc.get("name")));
	}

	/**
	 * 
	 * @param
	 * @return List<Map<String, Object>> for a single app mcp
	 * 
	 */
	public List<Map<String, Object>> getAllToolsJsonForRoom() {
		List<Map<String, Object>> aggregated = new ArrayList<>();
		Map<String, Object> o = getOptionsMap();

		// make sure the same toolbox is not accidentally added more than once
		Set<String> ensureUnique = new HashSet<>();

		if (o.containsKey("mcp")) {
			try {
				List<Map<String, Object>> mapMapList = (List<Map<String, Object>>) o.get("mcp");
				for (Map<String, Object> mcpMap : mapMapList) {
					if (mcpMap.containsKey("id")) {
						String id = (String) mcpMap.get("id");
						if (!ensureUnique.contains(id)) {
							aggregated.addAll(getToolJson(id));
							ensureUnique.add(id);
						}
					} else {
						throw new IllegalArgumentException("Tool map must contain both type and id");
					}
				}
			} catch (Exception e) {
				classLogger.error("Unable to add tool map from room mcp", e);
			}
		}

		if (o.containsKey("workspace")) {
			try {
				Map<String, Object> workspace = (Map<String, Object>) o.get("workspace");
				if (workspace != null && workspace.containsKey("workspace_id")) {
					String workspaceId = (String) workspace.get("workspace_id");
					List<Map<String, Object>> tools = ModelInferenceLogsUtils.getWorkspaceResourcesByType(workspaceId,
							null);
					for (Map<String, Object> tool : tools) {
						String toolId = (String) tool.get("resource_id");
						if (!ensureUnique.contains(toolId)) {
							aggregated.addAll(getToolJson(toolId));
							ensureUnique.add(toolId);
						}
					}
				}
			} catch (Exception e) {
				classLogger.error("Unable to add tool map from workspace mcp", e);
			}
		}

		return aggregated;
	}

	/**
	 * 
	 * @param String app id
	 * @return List<Map<String, Object>> for a single app mcp
	 */
	private List<Map<String, Object>> getToolJson(String engineId) {
		IEngine engine = null;
		try {
			engine = Utility.getEngine(engineId);
		} catch (Exception ex) {
			// ignore
		}
		if (engine == null) {
			engine = Utility.getProject(engineId);
		}
		JSONObject toolMap = MCPUtility.getAggregatedTools(engine);
		JSONObject updatedToolMap = MCPUtility.appendEngineIdToToolsMethodName(engineId, toolMap);
		if (updatedToolMap != null && updatedToolMap.has("tools")) {
			JSONArray arr = updatedToolMap.getJSONArray("tools");
			List<Map<String, Object>> result = new ArrayList<>();
			for (int i = 0; i < arr.length(); i++) {
				JSONObject toolObj = arr.optJSONObject(i);
				if (toolObj == null) {
					continue; // no tool so skip
				}

				JSONObject meta = toolObj.optJSONObject("_meta");
				Object executionValue = meta != null ? meta.opt("SMSS_MCP_EXECUTION") : null;

				if (!MCPExecution.DISABLED.getValue().equals(executionValue)) {
					result.add(toolObj.toMap());
				}

			}
			return result;
		}

		// Fallback: always return an empty list if nothing found
		return Collections.emptyList();
	}

	/**
	 * 
	 * @param llmResponse
	 * @return
	 */
	private ResponseMessage createResponseMessage(AskModelEngineResponse llmResponse) {
		if (llmResponse.getMessageType().equals(AskModelEngineResponse.CHAT)) {
			return ResponseMessage.text(llmResponse.getStringResponse());
		} else if (llmResponse.getMessageType().equals(AskModelEngineResponse.TOOL)) {
			AskToolModelEngineResponse toolResponse = (AskToolModelEngineResponse) llmResponse;
			return ResponseMessage.toolResponses(toolResponse.getToolResponse());
		}
		// TODO: handle image, tool calls, etc.
		return ResponseMessage.text("null");
	}

	public boolean isMessageAuthor(String messageId) {
		return getMessages().parallelStream()
				.anyMatch(m -> m.getMessageId().equals(messageId)
						&& m instanceof prerna.engine.impl.model.message.ResponseMessage
						&& (m.hasTextPart() || m.hasToolCallPart()));
	}

	// --- System Prompt Handling ----

	/**
	 * Returns the effective system prompt by checking options.instructions, then
	 * workspace.system_prompt, then optionally applying an enterprise-level
	 * template from the active admin theme.
	 * 
	 * @param
	 * @return String the system prompt or null if none is defined
	 */
	public String getEffectiveSystemPrompt() {
		// 1. Try options.instructions
		String opts = getOptions();
		JsonObject optionsObj = null;
		if (opts != null && !opts.trim().isEmpty()) {
			try {
				optionsObj = JsonParser.parseString(opts).getAsJsonObject();
			} catch (Exception ignore) {
			}
		}
		String systemPrompt = null;
		if (optionsObj != null) {
			JsonElement instructionsElem = optionsObj.get("instructions");
			if (instructionsElem != null && instructionsElem.isJsonPrimitive()) {
				systemPrompt = StringUtils.trimToNull(instructionsElem.getAsString());
			}
		}

		// 2. Try workspace.system_prompt (by workspace_id in options)
		if (systemPrompt == null && optionsObj != null) {
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
			if (workspaceId != null) {
				Map<String, Object> workspace = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
				if (workspace != null) {
					User user = this.insight.getUser();
					if (!SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
						throw new IllegalArgumentException("Workspace " + workspaceId
								+ " does not exist or user does not have access to the workspace");
					}
					// Check active or other validation if needed
					Object isActive = workspace.get("is_active");
					if (Boolean.FALSE.equals(isActive)) {
						throw new IllegalArgumentException("Workspace is disabled by the owner");
					}
					systemPrompt = StringUtils.trimToNull((String) workspace.get("system_prompt"));
				}
			}
		}
		String enterpriseTemplate = getEnterpriseSystemPromptTemplateFromActiveTheme();
		String merged = applyEnterpriseSystemPromptTemplate(enterpriseTemplate, systemPrompt);
		return expandSystemPromptVariables(merged);
	}

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
			classLogger.debug(Constants.STACKTRACE, e);
			return null;
		}
	}

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

	private static String getEnterpriseSystemPromptTemplateFromActiveTheme() {
		try {
			return PlaygroundThemeUtils.getPlaygroundGlobalSystemPrompt();
		} catch (Exception ignore) {
		}
		return null;
	}

	// ---- Getters and Setters ----

	public String getId() {
		return room_id;
	}

	public void setId(String id) {
		this.room_id = id;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getRoomName() {
		return roomName;
	}

	public void setRoomName(String roomName) {
		this.roomName = roomName;
	}

	public String getShareId() {
		return shareId;
	}

	public void setShareId(String shareId) {
		this.shareId = shareId;
	}

	public boolean isActive() {
		return isActive;
	}

	public void isActive(boolean isActive) {
		this.isActive = isActive;
	}

	public Timestamp getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Timestamp createdAt) {
		this.createdAt = createdAt;
	}

	public Timestamp getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Timestamp updatedAt) {
		this.updatedAt = updatedAt;
	}

	public boolean isPinned() {
		return pinned;
	}

	public void setPinned(boolean pinned) {
		this.pinned = pinned;
	}

	public String getOptions() {
		return options;
	}

	public void setOptions(String options) {
		this.options = options;
	}

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

	public void setOptionsMap(Map<String, Object> map) {
		this.optionsMap = map;
		this.options = map == null ? null : GSON.toJson(map);
	}

	public String getModelId() {
		return modelId;
	}

	public void setModelId(String modelId) {
		this.modelId = modelId;
	}

	public String getRoomFolderPath() {
		return roomFolderPath;
	}

	// Core message accessors
	public List<AbstractMessage> getMessages() {
		return this.messages;
	}

	public void setMessages(List<AbstractMessage> messagesList) {
		this.messages.clear();
		if (messagesList != null) {
			this.messages.addAll(messagesList);
		}
	}

	// Serializes the message history to a JSON array for DB storage
	public String getMessagesAsString() {
		return MessageUtils.toJsonArray(messages);
	}

	// Serializes the message history to a JSON array for python exection
	public String getMessagesWithImageDataAsString() {
		return MessageUtils.toJsonArrayWithImageData(messages);
	}

	// Deserialize from a JSON string (DB column) and populate the list
	public void setMessagesFromString(String messagesJson) {
		// Pull room folder - Room folder is at BASE_FOLDER/roomid
		ClusterUtil.pullRoom(this.room_id);
		if (messagesJson == null || messagesJson.trim().isEmpty()) {
			this.setMessages(new ArrayList<>());
			return;
		}
		List<AbstractMessage> loaded = MessageUtils.fromJsonArray(messagesJson, this);

		this.setMessages(loaded != null ? loaded : new ArrayList<>());
	}

	public Insight getInsight() {
		return insight;
	}

	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	public String getMessageJson() {
		return this.messagesJson;
	}

	// this should rarely be used. Really only if a Message object was created and
	// then jsonified
	public void setMessagesJson(String messagesJson) {
		this.messagesJson = messagesJson;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

}
