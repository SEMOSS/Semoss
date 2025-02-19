package prerna.engine.impl.model.responses;

import java.util.Map;

import org.json.JSONObject;


public class AskToolModelEngineResponse extends AskModelEngineResponse<Map<String, Object>> {

    private static final long serialVersionUID = 1L;

    public AskToolModelEngineResponse(Map<String, Object> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
    	super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    	this.messageType=TOOL;
    }

    // Additional methods specific to tool responses can be added here

    public static AskToolModelEngineResponse fromMap(Map<String, Object> modelResponse) {
        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));
        return new AskToolModelEngineResponse(modelResponse, tokensInPrompt, tokensInResponse);
    }

	@Override
	public String getStringResponse() {	
		JSONObject jsonObject = new JSONObject(this.getResponse());
		return jsonObject.toString();
	}
    
 
}