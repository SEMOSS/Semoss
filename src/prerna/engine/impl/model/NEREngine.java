package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.responses.NerModelEngineResponse;
import prerna.om.Insight;
import prerna.engine.api.ModelTypeEnum;

/**
 * Named Entity Recognition models
 */
public class NEREngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);
	
	public NerModelEngineResponse predict(String text, List<String> entities, List<String> maskEntities, Insight insight, Map<String, Object> parameters) {
	    JSONObject payload = new JSONObject();
	    payload.put("text", text);
	    payload.put("labels", entities);
	    
	    if (maskEntities != null && !maskEntities.isEmpty()) {
	        payload.put("mask_entities", maskEntities);
	    }
	    
	    payload.put("model", this.model);
	    
	    classLogger.debug("NER predict payload: {}", payload.toString(2));
	    
	    try {
	        JSONObject response = this.makeModelRequest(payload);
	        
	        if (response == null) {
	            Map<String, Object> errorMap = new HashMap<>();
	            errorMap.put("status", "error");
	            errorMap.put("message", "Null response from model");
	            return new NerModelEngineResponse(errorMap, 0, 0);
	        }
	        
	        if (response.has("status") && "error".equals(response.getString("status"))) {
	            Map<String, Object> errorMap = new HashMap<>();
	            errorMap.put("status", "error");
	            errorMap.put("message", response.optString("message", "Unknown error"));
	            errorMap.put("code", response.optInt("code", 0));
	            return new NerModelEngineResponse(errorMap, 0, 0);
	        }
	        
	        NerModelEngineResponse formattedResponse = NerModelEngineResponse.fromJson(response);
	        return formattedResponse;
	    } catch (Exception e) {
	        classLogger.error("Error making model request", e);
	        Map<String, Object> errorMap = new HashMap<>();
	        errorMap.put("status", "error");
	        errorMap.put("message", e.getMessage());

	        return new NerModelEngineResponse(errorMap, 0, 0);
	    }
	}
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.NER;
	}
}