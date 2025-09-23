package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.responses.AskImageModelEngineResponse;
import prerna.om.Insight;
import prerna.engine.api.ModelTypeEnum;

public class KServeImageEngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(KServeImageEngine.class);
	
	@Override
	public AskImageModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight, Map<String, Object> hyperParameters) {
		classLogger.debug("Handling KServeImage Request..");
		
		JSONObject payload = new JSONObject();
		
		payload.put("prompt", question);
		
		if (hyperParameters !=null) {
			if (hyperParameters.containsKey("negative_prompt")) {
				String negativePrompt = (String) hyperParameters.get("negative_prompt");
				payload.put("negative_prompt", negativePrompt);
			}
			
			if (hyperParameters.containsKey("height")) {
			    Object heightObj = hyperParameters.get("height");
			    payload.put("height", heightObj.toString());
			}

			if (hyperParameters.containsKey("width")) {
			    Object widthObj = hyperParameters.get("width");
			    payload.put("width", widthObj.toString());
			}

			if (hyperParameters.containsKey("num_inference_steps")) {
			    Object stepsObj = hyperParameters.get("num_inference_steps");
			    payload.put("num_inference_steps", stepsObj.toString());
			}

			if (hyperParameters.containsKey("guidance_scale")) {
			    Object scaleObj = hyperParameters.get("guidance_scale");
			    payload.put("guidance_scale", scaleObj.toString());
			}

			if (hyperParameters.containsKey("num_images")) {
			    Object numImagesObj = hyperParameters.get("num_images");
			    payload.put("num_images", numImagesObj.toString());
			}
		}
		
		try {
            JSONObject modelResponse = makeModelRequest(payload);
            if (modelResponse != null) {
                return AskImageModelEngineResponse.getKServeImageResponse(modelResponse);
            } else {
                classLogger.error("Received null response from model");
                Map<String, Object> responseMap = new HashMap<>();
                responseMap.put("output", "Error creating image.");
                return new AskImageModelEngineResponse(responseMap, 0, 0);
            }
		} catch (Exception e) {
            classLogger.error("Error making model request", e);
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("output", "Error creating image.");
            return new AskImageModelEngineResponse(responseMap, 0, 0);
        }
	}
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.KSERVE_IMAGE;
	}

}
