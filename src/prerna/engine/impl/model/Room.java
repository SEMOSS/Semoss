package prerna.engine.impl.model;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import prerna.util.Utility;

public class Room {
	private String room_id;
	private String userId;
	private String roomName;
	private String shareId;
	private boolean isActive;
	private Timestamp createdAt;
	private Timestamp updatedAt;
	private final List<AbstractMessage> messages = new ArrayList<>();
	private boolean pinned;
	private String options;
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
	}

	public void parseMessages() {
		setMessagesFromString(this.messagesJson);
	}

	public ResponseMessage ask(InputMessage msg, String systemMessage, IModelEngine modelEngine) {

		// if a specific system message is sent to use, overwrite the existing in the
		// db.
		if (systemMessage != null) {
			this.systemMessage = systemMessage;
			ModelInferenceLogsUtils.setRoomContext(this.insight.getInsightId(),
					this.insight.getUser().getPrimaryLoginToken().getId(), systemMessage);
		}
		// Set model type and add message to history
		msg.setModel(modelEngine);
		// Set parentMessageId for this message
		if (!messages.isEmpty()) {
			AbstractMessage lastMsg = messages.get(messages.size() - 1);
			msg.setParentMessageId(lastMsg.getMessageId());
		} else {
			msg.setParentMessageId(null); // first message
		}

		messages.add(msg);

		String messageJsonString;
		if (Boolean.TRUE == msg.getParamMap().getOrDefault("use_history", Boolean.TRUE)) {
			messageJsonString = getMessagesWithImageDataAsString();
		} else {
			messageJsonString = MessageUtils.toJsonArrayWithImageData(Arrays.asList(msg));
		}

		Map<String, Object> kwArgMap = new HashMap<>();
		kwArgMap.putAll(msg.getParamMap());
		kwArgMap.put("message_json", messageJsonString);

		AskModelEngineResponse llmResponse = modelEngine.askRoom(msg.getInputPrompt(), this.getSystemMessage(), this,
				kwArgMap);
		ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(llmResponse).build();

		// set transaction id for both pieces
		msg.setTransactionId(llmResponse.getMessageId());
		msg.setTokensInMessage(llmResponse.getNumberOfTokensInPrompt());
		response.setTransactionId(llmResponse.getMessageId());

		// Create the assistant's response message and add to history
		response.setModel(modelEngine);
		response.setParentMessageId(msg.getMessageId());
		response.setTokensInMessage(llmResponse.getNumberOfTokensInResponse());
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

	public AskModelEngineResponse addToolExecutionResult(String toolCallId, String tool_name,
			String tool_execution_response, IModelEngine modelEngine, Insight insight) {
		if (messages.isEmpty())
			throw new IllegalStateException("No messages to match tool call context");

		// 1. Find the last RESPONSE_TOOL message (assistant tool_calls)
		int lastToolRespIdx = -1;
		ResponseMessage toolResponse = null;
		for (int i = messages.size() - 1; i >= 0; --i) {
			AbstractMessage m = messages.get(i);
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

		// 3. Add tool execution message
		AbstractMessage toolExecution = InputMessage.toolExecution(this, toolCallId, tool_name,
				tool_execution_response);
		toolExecution.setParentMessageId(toolResponse.getMessageId());
		toolExecution.setModel(modelEngine);
		messages.add(toolExecution);

		// 4. After the last RESPONSE_TOOL, gather all tool execution messages for that
		// context
		Set<String> allIds = new HashSet<>();
		for (Map<String, Object> tc : toolResponse.getToolResponses())
			allIds.add(String.valueOf(tc.get("id")));

		Set<String> answeredIds = new HashSet<>();
		// scan forward from toolResponse idx+1 to the end
		for (int i = lastToolRespIdx + 1; i < messages.size(); ++i) {
			AbstractMessage m = messages.get(i);
			if (m.getMessageType() == MessageType.INPUT_TOOL_EXEC) {
				InputMessage inputMessage = (InputMessage) m;
				toolCallId = inputMessage.getToolCallId();
				if (toolCallId != null)
					answeredIds.add(toolCallId);
			}
		}

		if (insight != null) {
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id, insight.getUser().getPrimaryLoginToken().getId(),
					getMessagesAsString());
		}

		// 5. If all tool_call_ids fulfilled, trigger next model.ask
		if (answeredIds.containsAll(allIds) && allIds.size() > 0) {
			// Prepare full prompt (map all formatted messages)
			List<Object> fullPrompt = new ArrayList<>();
//			for (AbstractMessage m : messages) {
//				fullPrompt.add(m.getFormattedMessage());
//			}
			Map<String, Object> params = new HashMap<>();
			params.put("full_prompt", fullPrompt);
			AskModelEngineResponse llmResponse = modelEngine.ask(null, null, insight, params);
			ResponseMessage nextAssistant = createResponseMessage(llmResponse);
			nextAssistant.setParentMessageId(toolExecution.getMessageId());
			nextAssistant.setModel(modelEngine);
//			nextAssistant.getFormattedMessage();
			messages.add(nextAssistant);

			ModelInferenceLogsUtils.llm2_updateRoomMessages(room_id, insight.getUser().getPrimaryLoginToken().getId(),
					getMessagesAsString());

			return llmResponse;
		}
		// Not all tool_calls fulfilled yet
		return null;
	}

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
		if (messagesList != null)
			this.messages.addAll(messagesList);
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