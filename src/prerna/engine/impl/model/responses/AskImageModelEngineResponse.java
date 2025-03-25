package prerna.engine.impl.model.responses;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

public class AskImageModelEngineResponse extends AskModelEngineResponse<Map<String, Object>> {
	private static final Logger classLogger = LogManager.getLogger(AskImageModelEngineResponse.class);
	private static final long serialVersionUID = 1L;
	
    public AskImageModelEngineResponse(Map<String, Object> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

	public static AskImageModelEngineResponse getKServeImageResponse(JSONObject response) {
        if (response != null) {
            Map<String, Object> responseMap = new HashMap<>();
            
            if (response.has("output")) {
                JSONObject outputValue = response.getJSONObject("output");
                responseMap.put("output", outputValue);
            }
            
            if (response.has("prompt")) {
                String prompt = response.getString("prompt");
                responseMap.put("prompt", prompt);
            }
            
            if (response.has("negative_prompt")) {
                String negativePrompt = response.getString("negative_prompt");
                responseMap.put("negative_prompt", negativePrompt);
            }
            
            if (response.has("height")) {
                int height = response.getInt("height");
                responseMap.put("height", height);
            }
            
            if (response.has("width")) {
                int width = response.getInt("width");
                responseMap.put("width", width);
            }
            
            if (response.has("num_inference_steps")) {
                int numInferenceSteps = response.getInt("num_inference_steps");
                responseMap.put("num_inference_steps", numInferenceSteps);
            }
            
            if (response.has("guidance_scale")) {
                double guidanceScale = response.getDouble("guidance_scale");
                responseMap.put("guidance_scale", guidanceScale);
            }
            
            if (response.has("seed")) {
                int seed = response.getInt("seed");
                responseMap.put("seed", seed);
            }

            return new AskImageModelEngineResponse(responseMap, 0, 0);
        } else {
        	classLogger.error("Null response from model request");
	        Map<String, Object> errorMap = new HashMap<>();
	        errorMap.put("status", "error");
	        errorMap.put("message", "Null response from model request");
	        
	        return new AskImageModelEngineResponse(errorMap, 0, 0);
        }
    
    }
	
    @Override
    public String getStringResponse() {    
        JSONObject jsonObject = new JSONObject(this.getResponse());
        return jsonObject.toString();
    }
}
