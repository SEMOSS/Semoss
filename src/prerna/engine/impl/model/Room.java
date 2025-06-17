package prerna.engine.impl.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import playground.utils.PlaygroundUtils;
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

public class Room {
    private String room_id;
    private String userId;
    private String title;
    private String shareId;
    private boolean archived;
    private Timestamp createdAt;
    private Timestamp updatedAt;
    private final List<AbstractMessage> messages = new ArrayList<>();
    private boolean pinned;
    private String options;
    private String modelId;
    private String messagesJson;
    private Insight insight;
    private String systemMessage;
    public Room() {}

    // Use this constructor if you want to load from JSON (as from DB)
    public Room(String room_id, String userId, String title, String shareId, boolean archived, 
                Timestamp createdAt, Timestamp updatedAt, String messagesJson, boolean pinned, 
                String options, String modelId) {
        this.room_id = room_id;
        this.userId = userId;
        this.title = title;
        this.shareId = shareId;
        this.archived = archived;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.pinned = pinned;
        this.options = options;
        this.modelId = modelId;
        this.messagesJson = messagesJson;
        parseMessages();
    }

    public void parseMessages() {
        // Ensuring the insight is not null so we pull that room folder to the insight first
        if(insight != null) {
            setMessagesFromString(this.messagesJson, insight);
        }
    }

    public AskModelEngineResponse ask(InputMessage msg, Insight insight, IModelEngine modelEngine) {
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

        // Prepare full prompt (map all formatted messages)
        List<Object> fullPrompt = new ArrayList<>();
        for (AbstractMessage m : messages) {
            fullPrompt.add(m.getFormattedMessage());
        }
        
         msg.getParamMap().put("full_prompt", fullPrompt);
         msg.getParamMap().put("tools", msg.getToolCalls());

        AskModelEngineResponse llmResponse = modelEngine.ask(null, this.getSystemMessage(), insight, msg.getParamMap());

        // Create the assistant's response message and add to history
        ResponseMessage response = createResponseMessage(llmResponse);
        response.setModel(modelEngine);
        response.setTransactionId(msg.getTransactionId());
        response.getFormattedMessage(); //set the formatted message
        response.setParentMessageId(msg.getMessageId());
        messages.add(response);
        
        // Persist the message history
        ModelInferenceLogsUtils.llm2_updateRoomMessages(
                room_id, 
                insight.getUser().getPrimaryLoginToken().getId(),
                getMessagesAsString()
        );

        // Debug
        System.out.println(getMessagesAsString());
        return llmResponse;
    }
    
    public AskModelEngineResponse addToolExecutionResult(
            String toolCallId, String tool_name, String tool_execution_response,
            IModelEngine modelEngine, Insight insight)
    {
        if (messages.isEmpty())
            throw new IllegalStateException("No messages to match tool call context");

        // 1. Find the last RESPONSE_TOOL message (assistant tool_calls)
        int lastToolRespIdx = -1;
        ResponseMessage toolResponse = null;
        for (int i = messages.size() - 1; i >= 0; --i) {
            AbstractMessage m = messages.get(i);
            // Stop if a user or assistant non-tool-response appears
            if (m instanceof ResponseMessage) {
                MessageType t = ((ResponseMessage)m).getMessageType();
                if (t == MessageType.RESPONSE_TOOL && ((ResponseMessage)m).hasToolResponses()) {
                    lastToolRespIdx = i;
                    toolResponse = (ResponseMessage) m;
                    break;
                } else if (t == MessageType.RESPONSE_TEXT) {
                    break;
                }
            } else if (m instanceof InputMessage) {
            	if(m.getMessageType() == MessageType.INPUT_TOOL_EXEC) {
            		continue;
            	}
                break;
            }
        }
        if (toolResponse == null) {
            throw new IllegalStateException("No previous assistant tool_calls (RESPONSE_TOOL) message found immediately before tool execution(s).");
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
        AbstractMessage toolExecution = InputMessage.toolExecution(toolCallId, tool_name, tool_execution_response);
        toolExecution.setParentMessageId(toolResponse.getMessageId());
        toolExecution.setModel(modelEngine);
        messages.add(toolExecution);

        // 4. After the last RESPONSE_TOOL, gather all tool execution messages for that context
        Set<String> allIds = new HashSet<>();
        for (Map<String, Object> tc : toolResponse.getToolResponses())
            allIds.add(String.valueOf(tc.get("id")));

        Set<String> answeredIds = new HashSet<>();
        // scan forward from toolResponse idx+1 to the end
        for (int i = lastToolRespIdx + 1; i < messages.size(); ++i) {
            AbstractMessage m = messages.get(i);
            if (m.getMessageType() == MessageType.INPUT_TOOL_EXEC ) {
                // role == "tool"
                Map<String,Object> f = m.getFormattedMessage();
                Object r = (f != null) ? f.get("tool_call_id") : null;
                if (r != null)
                    answeredIds.add(String.valueOf(r));
            }
        }

        if(insight != null) {
            ModelInferenceLogsUtils.llm2_updateRoomMessages(
                    room_id, 
                    insight.getUser().getPrimaryLoginToken().getId(),
                    getMessagesAsString()
            );
        }

        // 5. If all tool_call_ids fulfilled, trigger next model.ask
        if (answeredIds.containsAll(allIds) && allIds.size() > 0) {
            // Prepare full prompt (map all formatted messages)
            List<Object> fullPrompt = new ArrayList<>();
            for (AbstractMessage m : messages) {
                fullPrompt.add(m.getFormattedMessage());
            }
            Map<String,Object> params = new HashMap<>();
            params.put("full_prompt", fullPrompt);
            AskModelEngineResponse llmResponse = modelEngine.ask(null, null, insight, params);
            ResponseMessage nextAssistant = createResponseMessage(llmResponse);
            nextAssistant.setParentMessageId(toolExecution.getMessageId());
            nextAssistant.setModel(modelEngine);
            nextAssistant.getFormattedMessage();
            messages.add(nextAssistant);

            ModelInferenceLogsUtils.llm2_updateRoomMessages(
                    room_id, 
                    insight.getUser().getPrimaryLoginToken().getId(),
                    getMessagesAsString()
            );

            return llmResponse;
        }
        // Not all tool_calls fulfilled yet
        return null;
    }
    
    private ResponseMessage createResponseMessage(AskModelEngineResponse llmResponse) {
        if (llmResponse.getMessageType().equals(AskModelEngineResponse.CHAT)) {
            return ResponseMessage.text(llmResponse.getStringResponse());
        } else if (llmResponse.getMessageType().equals(AskModelEngineResponse.TOOL)){
        	AskToolModelEngineResponse toolResponse = (AskToolModelEngineResponse)llmResponse;
        	return ResponseMessage.toolResponses(toolResponse.getToolResponse());
        }
        // TODO: handle image, tool calls, etc.
        return ResponseMessage.text("null");
    }

    // ---- Getters and Setters ----

    public String getId()                  { return room_id; }
    public void setId(String id)           { this.room_id = id; }
    public String getUserId()              { return userId; }
    public void setUserId(String userId)   { this.userId = userId; }
    public String getTitle()               { return title; }
    public void setTitle(String title)     { this.title = title; }
    public String getShareId()             { return shareId; }
    public void setShareId(String shareId) { this.shareId = shareId; }
    public boolean isArchived()            { return archived; }
    public void setArchived(boolean archived)     { this.archived = archived; }
    public Timestamp getCreatedAt()        { return createdAt; }
    public void setCreatedAt(Timestamp createdAt) { this.createdAt = createdAt; }
    public Timestamp getUpdatedAt()        { return updatedAt; }
    public void setUpdatedAt(Timestamp updatedAt) { this.updatedAt = updatedAt; }
    public boolean isPinned()              { return pinned; }
    public void setPinned(boolean pinned)  { this.pinned = pinned; }
    public String getOptions()             { return options; }
    public void setOptions(String options) { this.options = options; }
    public String getModelId()             { return modelId; }
    public void setModelId(String modelId) { this.modelId = modelId; }

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

    // Deserialize from a JSON string (DB column) and populate the list
    public void setMessagesFromString(String messagesJson, Insight insight) {
        // Pull room folder to insight
        // Room folder is at bucket/userid/roomid
        String userId = this.insight.getUser().getPrimaryLoginToken().getId();
        String remotePath = userId + "/" + this.room_id;
        String insightFolder = this.insight.getInsightFolder();
        PlaygroundUtils.getInstance().pullFile(remotePath, insightFolder + "/" + remotePath);
        
        if (messagesJson == null || messagesJson.trim().isEmpty()) {
            this.setMessages(new ArrayList<>());
            return;
        }
        
        List<AbstractMessage> loaded = MessageUtils.fromJsonArray(messagesJson, insight);
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
}