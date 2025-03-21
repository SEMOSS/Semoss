package prerna.engine.impl.model.responses;

import java.util.Map;
import org.json.JSONObject;

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
	
	/**
	 * Creates an AskModelEngineResponse from a JSONObject returned by the KServe adapter
	 * 
	 * @param jsonResponse The JSONObject from makeModelRequest
	 * @return AskModelEngineResponse constructed from the JSONObject
	 */
	public static AskModelEngineResponse fromJson(JSONObject jsonResponse) {
	    if (jsonResponse == null) {
	        return null;
	    }
	    
	    String responseText;
	    if (jsonResponse.has("output")) {
	        Object outputObj = jsonResponse.get("output");
	        if (outputObj instanceof JSONObject || outputObj instanceof org.json.JSONArray) {
	            responseText = outputObj.toString();
	        } else {
	            responseText = jsonResponse.getString("output");
	        }
	    } else {
	        responseText = "";
	    }
	    
	    Integer promptTokens = 0;
	    Integer responseTokens = 0;
	    
	    if (jsonResponse.has("input_tokens")) {
	        promptTokens = jsonResponse.getInt("input_tokens");
	    }
	    
	    if (jsonResponse.has("output_tokens")) {
	        responseTokens = jsonResponse.getInt("output_tokens");
	    }
	    
	    AskModelEngineResponse response = new AskModelEngineResponse(responseText, promptTokens, responseTokens);
	    
	    return response;
	}
}
