package prerna.engine.impl.model.responses;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractModelEngineResponse<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5721069401496221919L;
	
	public static final String RESPONSE = "response";
	public static final String NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt";
	public static final String NUMBER_OF_TOKENS_IN_RESPONSE = "numberOfTokensInResponse";
	
	private T response;
	private Integer numberOfTokensInPrompt;
	private Integer numberOfTokensInResponse;
	
    public AbstractModelEngineResponse(T response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        this.response = response;
        this.numberOfTokensInPrompt = numberOfTokensInPrompt;
        this.numberOfTokensInResponse = numberOfTokensInResponse;
    }

    public T getResponse() {
        return response;
    }

    public void setResponse(T response) {
        this.response = response;
    }

    public Integer getNumberOfTokensInPrompt() {
        return numberOfTokensInPrompt;
    }

    public void setNumberOfTokensInPrompt(Integer numberOfTokensInPrompt) {
        this.numberOfTokensInPrompt = numberOfTokensInPrompt;
    }

    public Integer getNumberOfTokensInResponse() {
        return numberOfTokensInResponse;
    }

    public void setNumberOfTokensInResponse(Integer numberOfTokensInResponse) {
        this.numberOfTokensInResponse = numberOfTokensInResponse;
    }
    
    public Map<String, Object> toMap(){
    	Map<String, Object> responseMap = new HashMap<>();
    	responseMap.put(RESPONSE, this.response);
    	responseMap.put(NUMBER_OF_TOKENS_IN_PROMPT, this.numberOfTokensInPrompt);
    	responseMap.put(NUMBER_OF_TOKENS_IN_RESPONSE, this.numberOfTokensInResponse);
    	
    	return responseMap;
    }
    
    protected static Integer getTokens(Object numTokens) {
		if (numTokens instanceof Integer) {
			return (Integer) numTokens;
		} else if (numTokens instanceof Long) {
			return ((Long) numTokens).intValue();
		} else if (numTokens instanceof Double) {
			return ((Double) numTokens).intValue();
		} else if (numTokens instanceof String){
			return Integer.valueOf((String) numTokens);
		} else {
			return null;
		}
	}
}
