package prerna.engine.impl.model.responses;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NerModelEngineResponse extends AbstractModelEngineResponse<Map<String, Object>> {
    private static final Logger classLogger = LogManager.getLogger(NerModelEngineResponse.class);
    private static final long serialVersionUID = 1L;
    
    public static final String MESSAGE_ID = "messageId";
	public static final String ROOM_ID = "roomId";

    private String messageId;
    private String roomId;

    public NerModelEngineResponse(Map<String, Object> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
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


    @SuppressWarnings("unchecked")
    public static NerModelEngineResponse fromMap(Map<String, Object> modelResponse) {
        if (!(modelResponse instanceof Map)) {
            throw new IllegalArgumentException("Invalid response type: " + modelResponse.getClass());
        }
        
        Map<String, Object> responseMap = (Map<String, Object>) modelResponse;
        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));

        NerModelEngineResponse response = new NerModelEngineResponse(responseMap, tokensInPrompt, tokensInResponse);

        return response;
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
