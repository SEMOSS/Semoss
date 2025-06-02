package prerna.engine.impl.model.responses;

import java.util.HashMap;
import java.util.Map;
import java.util.List; 
import java.util.Arrays;  
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class AskImageModelEngineResponse extends AskModelEngineResponse<Map<String, Object>> {
	
	private static final Logger classLogger = LogManager.getLogger(AskImageModelEngineResponse.class);
	private static final long serialVersionUID = 1L;
	
	/**
	 * 
	 * @param response
	 * @param numberOfTokensInPrompt
	 * @param numberOfTokensInResponse
	 */
    public AskImageModelEngineResponse(Map<String, Object> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

    /**
     * 
     * @param response
     * @return
     */
    public static AskImageModelEngineResponse getKServeImageResponse(JSONObject response) {
        if (response != null) {
            Map<String, Object> responseMap = new HashMap<>();
            
            if (response.has("output")) {
                Object outputObj = response.get("output");
                String outputStr = null;
                
                if (outputObj instanceof JSONArray) {
                    JSONArray images = (JSONArray) outputObj;
                    String[] imageList = new String[images.length()];
                    for (int i = 0; i < images.length(); i++) {
                        imageList[i] = images.getString(i);
                    }
                    responseMap.put("images", imageList);
                } else if (outputObj instanceof String) {
                    outputStr = (String) outputObj;
                    try {
                        JSONArray images = new JSONArray(outputStr);
                        String[] imageList = new String[images.length()];
                        for (int i = 0; i < images.length(); i++) {
                            imageList[i] = images.getString(i);
                        }
                        responseMap.put("images", imageList);
                    } catch (JSONException e) {
                        responseMap.put("images", outputStr);
                    }
                } else {
                    responseMap.put("images", outputObj);
                }
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
    
    /**
     * Retrieves the list of images from the response.
     * The images are expected to be stored as a String[] or a single String
     * under the key "images" in the response map.
     * @return A List<String> containing image strings (e.g., URLs or base64 data),
     * or an empty List<String> if no images are found or the format is unexpected.
     */
    public List<String> getImagesResponse() {
        List<String> imageList = new ArrayList<>(); // Use ArrayList<String>
        Map<String, Object> responseData = this.getResponse();

        if (responseData != null && responseData.containsKey("images")) {
            Object imagesValue = responseData.get("images");

            if (imagesValue instanceof String[]) {
                // If it's an array of strings, add them all to the list
                String[] imagesArray = (String[]) imagesValue;
                imageList.addAll(Arrays.asList(imagesArray)); // Efficient way to add all elements
            } else if (imagesValue instanceof String) {
                // If it's a single string, add that to the list
                imageList.add((String) imagesValue);
            } else if (imagesValue != null) {
                // Optional: Log if it's an unexpected type but not null
                classLogger.warn("Unexpected type for 'images' in responseData: " + imagesValue.getClass().getName());
            }
        }
        return imageList;
    }
	
    @Override
    public String getStringResponse() {
        Map<String, Object> response = this.getResponse();
        JSONObject jsonObject = new JSONObject();
        
        if (response.containsKey("images")) {
            Object imagesObj = response.get("images");
            if (imagesObj instanceof JSONArray) {

                JSONArray imagesArray = (JSONArray) imagesObj;
                jsonObject.put("images", imagesArray);
            } else {
                jsonObject.put("images", imagesObj);
            }
        } else {
            for (Map.Entry<String, Object> entry : response.entrySet()) {
                jsonObject.put(entry.getKey(), entry.getValue());
            }
        }
        
        return jsonObject.toString();
    }
}
