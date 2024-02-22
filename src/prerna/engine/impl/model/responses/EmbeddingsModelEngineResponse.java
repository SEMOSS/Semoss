package prerna.engine.impl.model.responses;

import java.util.List;
import java.util.Map;

public class EmbeddingsModelEngineResponse extends AbstractModelEngineResponse<List<List<Double>>> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4408956306133085964L;

	public EmbeddingsModelEngineResponse(List<List<Double>> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

	
	@SuppressWarnings("unchecked")
	public static EmbeddingsModelEngineResponse fromMap(Map<String, Object> modelResponse) {
        // Assuming RESPONSE, NUMBER_OF_TOKENS_IN_PROMPT, and NUMBER_OF_TOKENS_IN_RESPONSE are constants defined elsewhere
		List<List<Double>> responseObject = (List<List<Double>>) modelResponse.get(RESPONSE);
        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));
        
        return new EmbeddingsModelEngineResponse(responseObject, tokensInPrompt, tokensInResponse);
    }
	
	@SuppressWarnings("unchecked")
	public static EmbeddingsModelEngineResponse fromObject(Object responseObject) {
		
		Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
		return fromMap(modelResponse);
    }
}
