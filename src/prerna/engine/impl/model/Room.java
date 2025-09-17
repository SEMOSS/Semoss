package prerna.engine.impl.model;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Constants;
import prerna.util.Utility;

public class Room {

	private static final Logger classLogger = LogManager.getLogger(Room.class);

	private String room_id;
	private String userId;
	private String roomName;
	private String shareId;
	private boolean isActive;
	private Timestamp createdAt;
	private Timestamp updatedAt;
	private final List<AbstractMessage> messages = new ArrayList<>();
	private boolean pinned;
	private String options; // Stays as string (as from DB)
	private transient Map<String, Object> optionsMap; // Not stored, just for use in code

	private String modelId;
	private String messagesJson;

	private Insight insight;
	private String systemMessage;
	private String roomFolderPath;

	public Room() {
	}

	// Use this constructor if you want to load from JSON (as from DB)
	public Room(String room_id, String userId, String roomName, String systemMessage, String shareId, boolean isActive,
			Timestamp createdAt, Timestamp updatedAt, String messagesJson, boolean pinned, String options,
			String modelId) {
		this.room_id = room_id;
		this.userId = userId;
		this.roomName = roomName;
		this.systemMessage = systemMessage;
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
				this.optionsMap = new Gson().fromJson(options, new TypeToken<Map<String, Object>>() {
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
	public ResponseMessage ask(InputMessage msg, String systemMessage, IModelEngine modelEngine) {
		return ask(msg, systemMessage, modelEngine, null);
	}

	/**
	 * 
	 * @param msg
	 * @param systemMessage
	 * @param modelEngine
	 * @param parentMessageId
	 * @return
	 */
	public ResponseMessage ask(InputMessage msg, String systemMessage, IModelEngine modelEngine,
			String parentMessageId) {
		// if a specific system message is sent to use, overwrite the existing in the db
		if (systemMessage != null) {
			this.systemMessage = systemMessage;
			ModelInferenceLogsUtils.setRoomContext(this.insight.getInsightId(),
					this.insight.getUser().getPrimaryLoginToken().getId(), systemMessage);
		}
		AbstractModelEngine abstractModel = (AbstractModelEngine) modelEngine;

		Map<String, Object> kwArgMap = new HashMap<>(msg.getParamMap());
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
		if (!abstractModel.keepInputOutput || !useHistory) {
			String singleMessageJson = MessageUtils.toJsonArrayWithImageData(Arrays.asList(msg));
			kwArgMap.put("message_json", singleMessageJson);

			AskModelEngineResponse llmResponse = modelEngine.askRoom(msg.getInputPrompt(), this.getSystemMessage(),
					this, msg, kwArgMap);
			ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(llmResponse).build();
			
			// set transaction id for both pieces
			msg.setTransactionId(llmResponse.getMessageId());
			msg.setTokensInMessage(llmResponse.getNumberOfTokensInPrompt());
			response.setTransactionId(llmResponse.getMessageId());

			
			
			response.setModel(modelEngine);
			response.setParentMessageId(msg.getMessageId());
			response.setTokensInMessage(llmResponse.getNumberOfTokensInResponse());
			return response;
		}

		// if we dont have to keep history. then wipe all previous messages.
		if (!abstractModel.keepConversationHistory) {
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

		ResponseMessage response = null;
		try {
			// add the message
			// note that the message must be sent in the message_json string
			messages.add(msg);

			String messageJsonString = MessageUtils.getMessageHistoryFromMessageId(this.messages, msg.getMessageId());
			kwArgMap.put("message_json", messageJsonString);

			AskModelEngineResponse llmResponse = modelEngine.askRoom(msg.getInputPrompt(), this.getSystemMessage(),
					this, msg, kwArgMap);
			response = ResponseMessage.Builder.fromAskModelEngineResponse(llmResponse).build();
			response.setMessageId(llmResponse.getMessageId());

			// set transaction id for both pieces
			msg.setTransactionId(llmResponse.getMessageId());
			msg.setTokensInMessage(llmResponse.getNumberOfTokensInPrompt());
			response.setTransactionId(llmResponse.getMessageId());

			// Create the assistant's response message and add to history
			response.setModel(modelEngine);
			response.setParentMessageId(msg.getMessageId());
			response.setTokensInMessage(llmResponse.getNumberOfTokensInResponse());
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			// removing the last message from the message list
			// because otherwise the chat history is all sorts of wonky
			messages.removeLast();
			throw e;
		}
		// the response was successful
		// so we can now add the response to the list of messages
		messages.add(response);

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
		if ((prevRoomName == null || prevRoomName.trim().isEmpty()) && this.roomName != null
				&& !this.roomName.trim().isEmpty()) {
			// Only update with room name if we just set it now!
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id, insight.getUser().getPrimaryLoginToken().getId(),
					getMessagesAsString(), this.roomName, modelEngine.getEngineId());
		} else {
			// Otherwise, regular update
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id, insight.getUser().getPrimaryLoginToken().getId(),
					getMessagesAsString());
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
	public AskModelEngineResponse addToolExecutionResult(String toolCallId, String toolName,
			String toolExecutionResponse, Map<String, Object> toolParameterValues, String parentMessageId,
			IModelEngine modelEngine, Insight insight) {
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
		int lastToolRespIdx = -1;
		ResponseMessage toolResponse = null;
		List<AbstractMessage> branchMessages = MessageUtils.getMessageBranch(messages, lastMessageId);
		for (int i = branchMessages.size() - 1; i >= 0; --i) {
			AbstractMessage m = branchMessages.get(i);
			// Stop if a user or assistant non-tool-response appears
			if (m instanceof ResponseMessage) {
				MessageType t = ((ResponseMessage) m).getMessageType();
				if (t == MessageType.RESPONSE_TOOL && ((ResponseMessage) m).hasToolResponses()) {
					lastToolRespIdx = i;
					toolResponse = (ResponseMessage) m;
					break;
				} else if (t == MessageType.RESPONSE_TEXT) {
					break;
				}
			} else if (m instanceof InputMessage) {
				if (m.getMessageType() == MessageType.INPUT_TOOL_EXEC) {
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

	    // CHAIN tool executions: parent is previous INPUT_TOOL_EXEC if exists after toolResponse, else toolResponse
	    String actualParentId = toolResponse.getMessageId();
	    int startSearchIdx = lastToolRespIdx + 1;
	    // Scan for last tool exec message for chaining
	    for (int i = messages.size() - 1; i >= startSearchIdx; --i) {
	        AbstractMessage m = messages.get(i);
	        if (m.getMessageType() == MessageType.INPUT_TOOL_EXEC) {
	            actualParentId = m.getMessageId();
	            break;
	        }
	    }
	    
		// 3. Add tool execution message
		AbstractMessage toolExecution = InputMessage.toolExecution(this, toolCallId, toolName, toolExecutionResponse,
				toolParameterValues);
	    toolExecution.setParentMessageId(actualParentId);
		toolExecution.setModel(modelEngine);
		messages.add(toolExecution);

		// 4. After the last RESPONSE_TOOL, gather all tool execution messages for that
		// context
		Set<String> allIds = new HashSet<>();
		for (Map<String, Object> tc : toolResponse.getToolResponses()) {
			allIds.add(String.valueOf(tc.get("id")));
		}

		Set<String> answeredIds = new HashSet<>();
		// scan forward from toolResponse idx+1 to the end
		for (int i = lastToolRespIdx + 1; i < messages.size(); ++i) {
			AbstractMessage m = messages.get(i);
			if (m.getMessageType() == MessageType.INPUT_TOOL_EXEC) {
				InputMessage inputMessage = (InputMessage) m;
				toolCallId = inputMessage.getToolCallId();
				if (toolCallId != null) {
					answeredIds.add(toolCallId);
				}
			}
		}

		if (insight != null) {
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id, insight.getUser().getPrimaryLoginToken().getId(),
					getMessagesAsString());
		}

		// 5. If all tool_call_ids fulfilled, trigger next model.ask
		if (answeredIds.containsAll(allIds) && allIds.size() > 0) {
			String messageJsonString = getMessagesWithImageDataAsString();
			Map<String, Object> params = new HashMap<>();
			params.put("message_json", messageJsonString);
			appendToolsToParams(params);
			AskModelEngineResponse llmResponse = modelEngine.askRoom("", null, this, toolExecution, params);
      
			ResponseMessage nextAssistant = createResponseMessage(llmResponse);
			nextAssistant.setParentMessageId(toolExecution.getMessageId());
			nextAssistant.setModel(modelEngine);
			messages.add(nextAssistant);
			
		    // --------- BEGIN TRANSACTION ID PROPAGATION ---------
		    // Step 1: retrieve or create transactionId from nextAssistant
		    String transactionId = nextAssistant.getTransactionId();
		    if (transactionId == null || transactionId.isEmpty()) {
		        transactionId = java.util.UUID.randomUUID().toString();
		        nextAssistant.setTransactionId(transactionId);
		    }

		    // Find all INPUT_TOOL_EXECs after toolResponse up through nextAssistant (exclusive)
		    for (int i = lastToolRespIdx + 1; i < messages.size(); ++i) {
		        AbstractMessage m = messages.get(i);
		        if (m == nextAssistant)
		            break; // Stop at nextAssistant (exclusive)
		        if (m.getMessageType() == MessageType.INPUT_TOOL_EXEC) {
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

	private void appendToolsToParams(Map<String, Object> params) {
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
	}

	/**
	 * 
	 * @param
	 * @return List<Map<String, Object>> for a single app mcp
	 * 
	 */
	public List<Map<String, Object>> getAllToolsJsonForRoom() {
		List<Map<String, Object>> aggregated = new ArrayList<>();
		Object mcpToolIDsObj = getOptionsMap().get(ReactorKeysEnum.MCP_TOOL_ID.getKey());
		if (mcpToolIDsObj instanceof List<?>) {
			List<?> mcpToolIDs = (List<?>) mcpToolIDsObj;
			for (Object appIdObj : mcpToolIDs) {
				if (appIdObj != null) {
					String appId = appIdObj.toString();
					aggregated.addAll(getToolJson(appId));
				}
			}
		}
		return aggregated;
	}

	/**
	 * 
	 * @param String app id
	 * @return List<Map<String, Object>> for a single app mcp
	 */
	private List<Map<String, Object>> getToolJson(String appId) {
		IProject project = Utility.getProject(appId);
		JSONObject toolMap = MCPUtility.getAggregatedTools(project);
		JSONObject updatedToolMap = MCPUtility.appendProjectIdToTooslMethodName(appId, toolMap);
		if (updatedToolMap != null && updatedToolMap.has("tools")) {
			JSONArray arr = updatedToolMap.getJSONArray("tools");
			List<Map<String, Object>> result = new ArrayList<>();
			for (int i = 0; i < arr.length(); i++) {
				JSONObject toolObj = arr.getJSONObject(i);
				Map<String, Object> map = toolObj.toMap();
				result.add(map);
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
					optionsMap = new Gson().fromJson(options, new TypeToken<Map<String, Object>>() {
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
		this.options = map == null ? null : new Gson().toJson(map);
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

	public String getSystemMessage() {
		return this.systemMessage;
	}

	public void setSystemMessage(String systemMessage) {
		this.systemMessage = systemMessage;
	}

	public String getMessageJson() {
		return this.messagesJson;
	}

	// this should rarely be used. Really only if a Message object was created and
	// then jsonified
	public void setMessagesJson(String messagesJson) {
		this.messagesJson = messagesJson;
	}

}
