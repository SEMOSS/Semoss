package prerna.engine.impl.model.responses;

import java.util.Map;

public class AskModelEngineResponse extends AbstractModelEngineResponse<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static final String MESSAGE_ID = "messageId";
	public static final String ROOM_ID = "roomId";

	private String messageId;
	private String roomId;
	
	public AskModelEngineResponse(String response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
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
	public Map<String, Object> toMap(){
    	Map<String, Object> responseMap = super.toMap();

    	responseMap.put(MESSAGE_ID, this.messageId);
    	responseMap.put(ROOM_ID, this.roomId);
  
    	return responseMap;
    }
	
	public static AskModelEngineResponse fromMap(Map<String, Object> modelResponse) {
        String responseObject = modelResponse.get(RESPONSE) + "";
        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));
        
        return new AskModelEngineResponse(responseObject, tokensInPrompt, tokensInResponse);
    }
	
	@SuppressWarnings("unchecked")
	public static AskModelEngineResponse fromObject(Object responseObject) {
		
		Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
		return fromMap(modelResponse);
    }
}
