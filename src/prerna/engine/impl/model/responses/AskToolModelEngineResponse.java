package prerna.engine.impl.model.responses;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.util.Constants;

public class AskToolModelEngineResponse extends AskModelEngineResponse<List<Map<String, Object>>> {

	private static final Logger classLogger = LogManager.getLogger(AskToolModelEngineResponse.class);

    private static final long serialVersionUID = 1L;
    private static final String ID_KEY = "id";
    private static final String NAME_KEY = "name";
    private static final String ARGUMENTS_KEY = "arguments";

    private List<ToolResponse> tools;

    /**
     * 
     * @param response
     * @param numberOfTokensInPrompt
     * @param numberOfTokensInResponse
     */
    public AskToolModelEngineResponse(List<Map<String, Object>> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
        
        this.tools = new ArrayList<>();
        for(Map<String, Object> toolResponse : response) {
        	String id = null;
        	String name = null;
        	Map<String, Object> arguments = null;
        	
        	if (toolResponse.containsKey(ID_KEY) && toolResponse.get(ID_KEY) instanceof String) {
                id = (String) toolResponse.get(ID_KEY);
            }
        	
        	if (toolResponse.containsKey(NAME_KEY) && toolResponse.get(NAME_KEY) instanceof String) {
                name = (String) toolResponse.get(NAME_KEY);
            }
        	
        	// TODO: why is this a string?
        	// TODO: why is this a string?
        	// TODO: why is this a string?
        	// TODO: why is this a string?
        	if (toolResponse.containsKey(ARGUMENTS_KEY) && toolResponse.get(ARGUMENTS_KEY) instanceof String) {
                String argumentsJson = (String) toolResponse.get(ARGUMENTS_KEY);
                try {
                	arguments = new GsonBuilder().disableHtmlEscaping().create().fromJson(argumentsJson, Map.class);
                } catch (Exception e) {
                	classLogger.error(Constants.STACKTRACE, e);
                }
            }
        	
        	ToolResponse tool = new ToolResponse(id, name, arguments);
        	this.tools.add(tool);
        }

        this.messageType = TOOL;
    }
    
    @Deprecated
    public String getToolCallId() {
        return this.tools.get(0).getId();
    }
    
    @Deprecated
    public String getToolCallArgumentsAsString() {
    	Map<String, Object> arguments = this.tools.get(0).getArguments();
    	if(arguments == null) {
    		return "{}";
    	}
    	return new Gson().toJson(arguments);
    }

    @Deprecated
    public String getToolCallName() {
    	return this.tools.get(0).getName();
    }
    
    @Override
    public String getStringResponse() {    
        JSONObject jsonObject = new JSONObject(this.getResponse());
        return jsonObject.toString();
    }
    
    /**
     * 
     * @return
     */
    public List<ToolResponse> getTools() {
		return tools;
	}
    
    /**
     * 
     */
    class ToolResponse {
    	
    	private String id;
        private String name;
        private Map<String, Object> arguments;
        
        public ToolResponse(String id, String name, Map<String, Object> arguments) {
        	this.id= id;
        	this.name = name;
        	this.arguments = arguments;
        }

		public String getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Map<String, Object> getArguments() {
			return arguments;
		}

//		public void setId(String id) {
//			this.id = id;
//		}
//		
//		public void setName(String name) {
//			this.name = name;
//		}
//		
//		public void setArguments(Map<String, Object> arguments) {
//			this.arguments = arguments;
//		}
    }
}