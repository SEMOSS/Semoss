package prerna.engine.impl.model.responses;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class AskImageModelEngineResponse extends AskModelEngineResponse<Map<String, Object>> {
	
	private static final Logger classLogger = LogManager.getLogger(AskImageModelEngineResponse.class);
	private static final long serialVersionUID = 1L;
	
	public static final String SOURCE_KSERVE = "kserve";
	public static final String SOURCE_OPENAI = "openai";
	
	/**
	 * 
	 * @param response
	 * @param numberOfTokensInPrompt
	 * @param numberOfTokensInResponse
	 */
    public AskImageModelEngineResponse(Map<String, Object> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
        this.messageType = IMAGE; 
    }

    /**
     * Factory method for OpenAI image responses
     * @param imageList List of image strings (base64 or URLs)
     * @param numberOfTokensInPrompt
     * @param numberOfTokensInResponse
     * @return AskImageModelEngineResponse
     */
    public static AskImageModelEngineResponse getOpenAIImageResponse(List<String> imageList, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        Map<String, Object> responseMap = new HashMap<>();
        
        if (imageList != null && !imageList.isEmpty()) {
            // Convert List<String> to String[] for consistency with KServe format
            String[] imageArray = imageList.toArray(new String[0]);
            responseMap.put("images", imageArray);
            responseMap.put("source", SOURCE_OPENAI);
            responseMap.put("count", imageList.size());
            
            // Determine if images are base64 or URLs
            String firstImage = imageList.get(0);
            boolean isBase64 = !firstImage.toLowerCase().startsWith("http") && firstImage.length() > 100;
            responseMap.put("format", isBase64 ? "base64" : "url");
        } else {
            responseMap.put("images", new String[0]);
            responseMap.put("source", SOURCE_OPENAI);
            responseMap.put("count", 0);
            responseMap.put("format", "unknown");
        }
        
        return new AskImageModelEngineResponse(responseMap, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

    /**
     * Factory method for KServe image responses
     * @param response
     * @return
     */
    public static AskImageModelEngineResponse getKServeImageResponse(JSONObject response) {
        if (response != null) {
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("source", SOURCE_KSERVE);
            
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
                    responseMap.put("count", imageList.length);
                } else if (outputObj instanceof String) {
                    outputStr = (String) outputObj;
                    try {
                        JSONArray images = new JSONArray(outputStr);
                        String[] imageList = new String[images.length()];
                        for (int i = 0; i < images.length(); i++) {
                            imageList[i] = images.getString(i);
                        }
                        responseMap.put("images", imageList);
                        responseMap.put("count", imageList.length);
                    } catch (JSONException e) {
                        responseMap.put("images", outputStr);
                        responseMap.put("count", 1);
                    }
                } else {
                    responseMap.put("images", outputObj);
                    responseMap.put("count", 1);
                }
            }
            
            if (response.has("prompt")) {
                responseMap.put("prompt", response.getString("prompt"));
            }
            if (response.has("negative_prompt")) {
                responseMap.put("negative_prompt", response.getString("negative_prompt"));
            }
            if (response.has("height")) {
                responseMap.put("height", response.getInt("height"));
            }
            if (response.has("width")) {
                responseMap.put("width", response.getInt("width"));
            }
            if (response.has("num_inference_steps")) {
                responseMap.put("num_inference_steps", response.getInt("num_inference_steps"));
            }
            if (response.has("guidance_scale")) {
                responseMap.put("guidance_scale", response.getDouble("guidance_scale"));
            }
            if (response.has("seed")) {
                responseMap.put("seed", response.getInt("seed"));
            }

            return new AskImageModelEngineResponse(responseMap, 0, 0);
        } else {
            classLogger.error("Null response from model request");
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("status", "error");
            errorMap.put("message", "Null response from model request");
            errorMap.put("source", SOURCE_KSERVE);
            
            return new AskImageModelEngineResponse(errorMap, 0, 0);
        }
    }

    /**
     * Get the images as a String array, regardless of source
     * @return String array of images, or empty array if none
     */
    public String[] getImages() {
        Map<String, Object> response = this.getResponse();
        Object imagesObj = response.get("images");
        
        if (imagesObj instanceof String[]) {
            return (String[]) imagesObj;
        } else if (imagesObj instanceof String) {
            return new String[]{(String) imagesObj};
        } else {
            return new String[0];
        }
    }

    /**
     * Get the first image
     * @return First image string, or null if no images
     */
    public String getFirstImage() {
        String[] images = getImages();
        return (images.length > 0) ? images[0] : null;
    }

    /**
     * Get the number of images
     * @return Number of images
     */
    public int getImageCount() {
        Map<String, Object> response = this.getResponse();
        Object countObj = response.get("count");
        if (countObj instanceof Integer) {
            return (Integer) countObj;
        }
        // Fallback: count the images array
        return getImages().length;
    }

    /**
     * Check if images are base64 encoded or URLs
     * @return true if base64, false if URLs, null if unknown
     */
    public Boolean isBase64Format() {
        Map<String, Object> response = this.getResponse();
        Object formatObj = response.get("format");
        if (formatObj instanceof String) {
            return "base64".equals(formatObj);
        }
        
        // Fallback: check first image
        String firstImage = getFirstImage();
        if (firstImage != null) {
            return !firstImage.toLowerCase().startsWith("http") && firstImage.length() > 100;
        }
        
        return null;
    }

    /**
     * Get the source of the images (kserve, openai, etc.)
     * @return Source string
     */
    public String getSource() {
        Map<String, Object> response = this.getResponse();
        Object sourceObj = response.get("source");
        return (sourceObj instanceof String) ? (String) sourceObj : "unknown";
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
            } else if (imagesObj instanceof String[]) {
                // Convert String[] to JSONArray
                String[] imageArray = (String[]) imagesObj;
                JSONArray imagesArray = new JSONArray();
                for (String image : imageArray) {
                    imagesArray.put(image);
                }
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