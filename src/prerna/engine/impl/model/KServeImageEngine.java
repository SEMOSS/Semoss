package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.responses.AskImageModelEngineResponse;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
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
				String height = (String) hyperParameters.get("height");
				payload.put("height", height);
			}
			
			if (hyperParameters.containsKey("width")) {
				String width = (String) hyperParameters.get("width");
				payload.put("width", width);
			}
			
			if (hyperParameters.containsKey("num_inference_steps")) {
				String num_inference_steps = (String) hyperParameters.get("num_inference_steps");
				payload.put("num_inference_steps", num_inference_steps);
			}
			
			if (hyperParameters.containsKey("guidance_scale")) {
				String guidance_scale = (String) hyperParameters.get("guidance_scale");
				payload.put("guidance_scale", guidance_scale);
			}
			
			if (hyperParameters.containsKey("seed")) {
				String seed = (String) hyperParameters.get("seed");
				payload.put("seed", seed);
			}
			
			if (hyperParameters.containsKey("num_images")) {
				String num_images = (String) hyperParameters.get("num_images");
				payload.put("num_images", num_images);
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

}
