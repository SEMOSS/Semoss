package prerna.engine.impl.model.responses;

import java.util.List;
import java.util.Map;

public abstract class AskModelEngineResponse<T> extends AbstractModelEngineResponse<T> {

    private static final long serialVersionUID = 1L;

    public static final String MESSAGE_ID = "messageId";
    public static final String ROOM_ID = "roomId";

    private String messageId;
    private String roomId;

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

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> responseMap = super.toMap();
        responseMap.put(MESSAGE_ID, this.messageId);
        responseMap.put(ROOM_ID, this.roomId);
        return responseMap;
    }

    // Factory method to create the appropriate response type
    @SuppressWarnings("unchecked")
    public static AskModelEngineResponse<?> fromMap(Object responseObject) {
        Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
        Object response = modelResponse.get(RESPONSE);

        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));

        if (response instanceof List) {
            List<?> responseList = (List<?>) response;
            //TODO here i am only handling one tool object to process
            if (!responseList.isEmpty() && responseList.get(0) instanceof Map) {
                return new AskToolModelEngineResponse((Map<String, Object>) responseList.get(0), tokensInPrompt, tokensInResponse);
            } else {
                throw new IllegalArgumentException("Unsupported response type of type list");
            }
        } else if (response instanceof String) {
            return new AskStringModelEngineResponse((String) response, tokensInPrompt, tokensInResponse);
        } else {
            throw new IllegalArgumentException("Unsupported response type");
        }
    }
    
	@SuppressWarnings("unchecked")
	public static AskModelEngineResponse fromObject(Object responseObject) {
		Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
		return fromMap(modelResponse);
    }

	public abstract String getStringResponse();
}