package prerna.engine.impl.model.responses;

import java.util.Map;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NerModelEngineResponse extends AbstractModelEngineResponse<List<Map<String, Object>>> {
	private static final Logger classLogger = LogManager.getLogger(NerModelEngineResponse.class);
	
	private static final long serialVersionUID = 1L;
	
	public NerModelEngineResponse(List<Map<String, Object>> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
		super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
	}

	
	public static final String MESSAGE_ID = "messageId";
	public static final String ROOM_ID = "roomId";

	private String messageId;
	private String roomId;
	
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
	
	public static NerModelEngineResponse fromMap(Map<String, Object> modelResponse) {
        Object responseObject = modelResponse.get(RESPONSE);
        List<Map<String, Object>> responseList = null;

        if (responseObject instanceof List) {
            responseList = (List<Map<String, Object>>) responseObject;
        } else {
            throw new IllegalArgumentException("Invalid response type: " + responseObject.getClass());
        }

        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));
        
        return new NerModelEngineResponse(responseList, tokensInPrompt, tokensInResponse);
	}
	
	@SuppressWarnings("unchecked")
	public static NerModelEngineResponse fromObject(Object responseObject) {
		if (responseObject instanceof Map) {
			Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
			return fromMap(modelResponse);
		} else {
			classLogger.error("responseObject : {}", responseObject);
			throw new IllegalArgumentException("Expected a Map<String, Object> but got: " + responseObject.getClass());
		}
	}
}
