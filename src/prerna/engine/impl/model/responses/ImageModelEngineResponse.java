package prerna.engine.impl.model.responses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONArray;


public class ImageModelEngineResponse extends AbstractModelEngineResponse<Map<String, Object>> {
	private static final Logger classLogger = LogManager.getLogger(ImageModelEngineResponse.class);
    private static final long serialVersionUID = 1L;
    
    public static final String MESSAGE_ID = "messageId";
	public static final String ROOM_ID = "roomId";

    private String messageId;
    private String roomId;
    
    public ImageModelEngineResponse(Map<String, Object> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}
	
	public String getMessageId() {
		return this.messageId;
	}
	
	public void setRoomId(String roomId) {
		this.roomId = roomId;
	}
	
	public String getRoomId() {
		return this.roomId;
	}
	
    public static ImageModelEngineResponse fromKServe(JSONObject response) {
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

            return new ImageModelEngineResponse(responseMap, 0, 0);
        } else {
        	classLogger.error("Null response from model request");
	        Map<String, Object> errorMap = new HashMap<>();
	        errorMap.put("status", "error");
	        errorMap.put("message", "Null response from model request");
	        
	        return new ImageModelEngineResponse(errorMap, 0, 0);
        }
    
    }
}
