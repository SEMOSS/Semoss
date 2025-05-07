package prerna.engine.impl.model.responses;

import org.json.JSONObject;

public class AskStringModelEngineResponse extends AskModelEngineResponse<String> {

    private static final long serialVersionUID = 1L;

    public AskStringModelEngineResponse(String response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

	@Override
	public String getStringResponse() {
		// TODO Auto-generated method stub
		return this.getResponse();
	}	
	
	/**
	 * Creates an AskModelEngineResponse from a JSONObject returned by the KServe adapter
	 * 
	 * @param jsonResponse The JSONObject from makeModelRequest
	 * @return AskModelEngineResponse constructed from the JSONObject
	 */
	public static AskStringModelEngineResponse fromJson(JSONObject jsonResponse) {
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
	    
	    AskStringModelEngineResponse response = new AskStringModelEngineResponse(responseText, promptTokens, responseTokens);
	    
	    return response;
	}

    // Additional methods specific to string responses can be added here
}