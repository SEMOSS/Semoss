package prerna.engine.impl.model;

import java.util.Map;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;

public class KServeVisionEngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);
	
	@Override
	public AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight, Map<String, Object> hyperParameters) {
		classLogger.debug("Handling KServeVision Request..");
		
		JSONObject payload = new JSONObject();
		
		// Vision models require images in the payload..
	    if (hyperParameters != null && hyperParameters.containsKey("image_url")) {
	        String imageUrl = (String) hyperParameters.get("image_url");
	        payload.put("image", imageUrl);
	    } else {
	        classLogger.warn("No image_url found in hyperParameters");
	        AskModelEngineResponse response = new AskModelEngineResponse("Please provide an image parameter.", 0, 0);
	        return response;
	    }
		
		payload.put("text", question);
		
		classLogger.debug("KServeVision askCall payload: {}", payload.toString(2));
		
		try {
            JSONObject modelResponse = makeModelRequest(payload);
            if (modelResponse != null) {
                return AskModelEngineResponse.fromJson(modelResponse);
            } else {
                classLogger.error("Received null response from model");
                return new AskModelEngineResponse("Error processing image.", 0, 0);
            }
		} catch (Exception e) {
            classLogger.error("Error making model request", e);
            return new AskModelEngineResponse("An error occurred while processing the request: " + e.getMessage(), 0, 10);
        }
	}
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.KSERVE_VISION;
	}

}
