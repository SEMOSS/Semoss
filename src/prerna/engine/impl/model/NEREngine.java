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
 * This is a class representing a RemoteClientServer deployed instance of a Named Entity Recognition model.
 * See https://github.com/SEMOSS/remote-client-server for RemoteClientServer implementation.
 */
public class NEREngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);
	
    public NerModelEngineResponse predict(String text, List<String> entities, List<String> maskEntities, Insight insight, Map<String, Object> parameters) {
        JSONObject payload = new JSONObject();
        payload.put("text", text);
        payload.put("entities", entities);
        payload.put("mask_entities", maskEntities);
        payload.put("model", this.model);
        
        try {
            JSONObject response = this.makeModelRequest(payload);
            
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
