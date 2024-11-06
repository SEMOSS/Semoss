package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.RemoteModelStateEnum;
import prerna.engine.impl.model.responses.NerModelEngineResponse;
import prerna.om.Insight;
import prerna.engine.api.ModelTypeEnum;

public class NEREngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);
	
	public NerModelEngineResponse predict(String text, List<String> entities, List<String> maskEntities, Insight insight, Map<String, Object> parameters) {
	    JSONObject payload = new JSONObject();
	    payload.put("text", text);
	    payload.put("entities", entities);
	    payload.put("mask_entities", maskEntities);
	    payload.put("model", this.model);
	    
	    JSONObject response = null;
	    try {
	        response = this.makeModelRequest(payload);
	        
	        if (response != null) {
	            classLogger.info("Response from model: {}", response.toString());
	            
	            // Converting JSONObject to Map<String, Object>
	            Map<String, Object> responseMap = new HashMap<>();
	            for (String key : response.keySet()) {
	                responseMap.put(key, response.get(key));
	            }
	           
	            NerModelEngineResponse nerResponse = new NerModelEngineResponse(
	                responseMap,
	                0, // numberOfTokensInPrompt (will see if I can get this from the response eventually)
	                0  // numberOfTokensInResponse (will see if I can get this from the response eventually)
	            );
	            
	            return nerResponse;
	            
	        } else {
	            classLogger.error("Null response from model request");
		        // error response
	            Map<String, Object> errorMap = new HashMap<>();
	            errorMap.put("status", "error");
	            errorMap.put("message", "Null response from model request");
	            
	            return new NerModelEngineResponse(errorMap, 0, 0);
	        }
	    } catch (Exception e) {
	        classLogger.error("Error making model request", e);
	        // error response
	        Map<String, Object> errorMap = new HashMap<>();
	        errorMap.put("status", "error");
	        errorMap.put("message", e.getMessage());
	        
	        return new NerModelEngineResponse(errorMap, 0, 0);
	    }
	}
	
	// Check if the model is active and if not attempt to start the model
	public String getModelStatus() {
		try {
		RemoteModelStateEnum currentState = getCurrentModelState();
        classLogger.info("Current state for engineId {} is: {}", this.engineId, currentState);
        // Turn it into a string
		String modelState = currentState.name();
        return modelState;
		} catch (Exception e) {
			classLogger.error("Error getting model state", e);
			return "ERROR";
		}
		
	}
	
	
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.NER;
	}

}
