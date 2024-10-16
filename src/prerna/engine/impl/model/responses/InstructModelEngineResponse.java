package prerna.engine.impl.model.responses;

import java.util.Map;
import java.util.List;

public class InstructModelEngineResponse extends AbstractModelEngineResponse<String[]> {


    private static final long serialVersionUID = 1L;
    
	public static final String MESSAGE_ID = "messageId";
	public static final String ROOM_ID = "roomId";

	private String messageId;
	private String roomId;

    public InstructModelEngineResponse(String[] response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
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

    public static InstructModelEngineResponse fromMap(Map<String, Object> modelResponse) {
        Object responseObject = modelResponse.get(RESPONSE);
        String[] responseArray = null;

        if (responseObject instanceof String[]) {
            responseArray = (String[]) responseObject;
        } else if (responseObject instanceof java.util.List) {
            java.util.List<?> responseList = (java.util.List<?>) responseObject;
            responseArray = responseList.toArray(new String[0]);
        } else if (responseObject instanceof String) {
            responseArray = new String[] { (String) responseObject };
        } else {
            throw new IllegalArgumentException("Invalid response type: " + responseObject.getClass());
        }

        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));

        return new InstructModelEngineResponse(responseArray, tokensInPrompt, tokensInResponse);
    }

    @SuppressWarnings("unchecked")
    public static InstructModelEngineResponse fromObject(Object responseObject) {
        if (responseObject instanceof Map) {
            Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
            return fromMap(modelResponse);
        } else {
            throw new IllegalArgumentException("Expected a Map<String, Object> but got: " + responseObject.getClass());
        }
    }
}