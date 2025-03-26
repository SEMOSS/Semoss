package prerna.engine.impl.model.responses;

import java.util.List;
import java.util.Map;
import org.json.JSONObject;

public abstract class AskModelEngineResponse<T> extends AbstractModelEngineResponse<T> {

    private static final long serialVersionUID = 1L;

    public static final String MESSAGE_ID = "messageId";
    public static final String ROOM_ID = "roomId";
    public static final String MESSAGE_TYPE = "messageType";
    public static final String CHAT = "CHAT";
    public static final String TOOL = "TOOL";

    private String messageId;
    private String roomId;
    protected String messageType = CHAT;
    
    public AskModelEngineResponse(T response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getMessageId() {
        return this.messageId;
    }

    public void setRoomId(String roomId) {
        this.roomId = roomId;
    }

    public String getRoomId() {
        return this.roomId;
    }
    
    public String getMessageType() {
    	return this.messageType;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> responseMap = super.toMap();
        responseMap.put(MESSAGE_ID, this.messageId);
        responseMap.put(ROOM_ID, this.roomId);
        responseMap.put(MESSAGE_TYPE, this.messageType);
        return responseMap;
    }

 // Factory method to create the appropriate response type
    @SuppressWarnings("unchecked")
    public static AskModelEngineResponse<?> fromMap(Object responseObject) {
        Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
        Object response = modelResponse.get(RESPONSE);

        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));

        // Set default messageType
        String messageType = CHAT;

        // Check if MESSAGE_TYPE is present and valid
        Object messageTypeObject = modelResponse.get(MESSAGE_TYPE);
        if (messageTypeObject != null) {
            if (messageTypeObject instanceof String) {
                messageType = (String) messageTypeObject;
            } else {
                throw new IllegalArgumentException("MESSAGE_TYPE is not a String");
            }
        }

        // Adjust logic based on messageType
        if (TOOL.equals(messageType)) {
            if (response instanceof List) {
                List<?> responseList = (List<?>) response;
                // Handle one tool object to process
                if (!responseList.isEmpty() && responseList.get(0) instanceof Map) {
                    return new AskToolModelEngineResponse((Map<String, Object>) responseList.get(0), tokensInPrompt, tokensInResponse);
                } else {
                    throw new IllegalArgumentException("Tool list is empty or not valid");
                }
            } else {
                throw new IllegalArgumentException("Expected a List response for Tool messageType");
            }
        } else if (CHAT.equals(messageType)) {
            if (response instanceof String) {
                return new AskStringModelEngineResponse((String) response, tokensInPrompt, tokensInResponse);
            } else {
                throw new IllegalArgumentException("Expected a String response for Chat messageType");
            }
        } else {
            throw new IllegalArgumentException("Unsupported message type: " + messageType);
        }
    }

    
	@SuppressWarnings("unchecked")
	public static AskModelEngineResponse fromObject(Object responseObject) {
		Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
		return fromMap(modelResponse);
    }

	public abstract String getStringResponse();
}