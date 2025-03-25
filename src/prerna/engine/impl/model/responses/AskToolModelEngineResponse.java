package prerna.engine.impl.model.responses;

import java.util.Map;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AskToolModelEngineResponse extends AskModelEngineResponse<Map<String, Object>> {

    private static final long serialVersionUID = 1L;
    private static final String ID_KEY = "id";
    private static final String NAME_KEY = "name";
    private static final String ARGUMENTS_KEY = "arguments";
    private static final String NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt";
    private static final String NUMBER_OF_TOKENS_IN_RESPONSE = "numberOfTokensInResponse";

    private String id;
    private String name;
    private Map<String, Object> arguments;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public AskToolModelEngineResponse(Map<String, Object> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
        
        if (response.containsKey(ID_KEY) && response.get(ID_KEY) instanceof String) {
            this.id = (String) response.get(ID_KEY);
        } else {
            // Handle error or set default value
            this.id = null;
        }

        if (response.containsKey(NAME_KEY) && response.get(NAME_KEY) instanceof String) {
            this.name = (String) response.get(NAME_KEY);
        } else {
            // Handle error or set default value
            this.name = null;
        }

        if (response.containsKey(ARGUMENTS_KEY) && response.get(ARGUMENTS_KEY) instanceof String) {
            String argumentsJson = (String) response.get(ARGUMENTS_KEY);
            try {
                this.arguments = objectMapper.readValue(argumentsJson, Map.class);
            } catch (Exception e) {
                // Handle parsing error
                this.arguments = null;
            }
        } else {
            this.arguments = null;
        }
        
        this.messageType = TOOL;
    }

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
    
    public String getToolCallId() {
        return this.id;
    }
    
    public String getToolCallArgumentsAsString() {
        if (this.arguments != null) {
            try {
                return objectMapper.writeValueAsString(this.arguments);
            } catch (JsonProcessingException e) {
                // Handle the exception, maybe log it or return a default value
                e.printStackTrace();
                return "{}"; // or null, depending on your needs
            }
        }
        return "{}"; // or null, depending on your needs
    }
    
    public String getToolCallName() {
        return this.name;
    }
}