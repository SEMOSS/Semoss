package prerna.engine.impl.model.responses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class NerModelEngineResponse extends AbstractModelEngineResponse<Map<String, Object>> {
    
	private static final Logger classLogger = LogManager.getLogger(NerModelEngineResponse.class);
    private static final long serialVersionUID = 1L;
    
    public static final String MESSAGE_ID = "messageId";
	public static final String ROOM_ID = "roomId";

    private String messageId;
    private String roomId;

    /**
     * 
     * @param response
     * @param numberOfTokensInPrompt
     * @param numberOfTokensInResponse
     */
    public NerModelEngineResponse(Map<String, Object> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
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
	
	/**
	 * 
	 * @param response
	 * @return
	 */
	public static NerModelEngineResponse fromJson(JSONObject response) {
	    Logger classLogger = LogManager.getLogger(NerModelEngineResponse.class);
	    
	    if (response != null) {      
	        Map<String, Object> responseMap = new HashMap<>();
	        
	        if (response.has("mask_values")) {
	            JSONObject maskValues = response.getJSONObject("mask_values");
	            Map<String, String> maskValuesMap = new HashMap<>();
	            for (String key : maskValues.keySet()) {
	                maskValuesMap.put(key, maskValues.getString(key));
	            }
	            responseMap.put("mask_values", maskValuesMap);
	        }
	        
	        if (response.has("entities")) {
	            JSONArray entitiesArray = response.getJSONArray("entities");
	            List<String> entitiesList = new ArrayList<>();
	            for (int i = 0; i < entitiesArray.length(); i++) {
	                entitiesList.add(entitiesArray.getString(i));
	            }
	            responseMap.put("entities", entitiesList);
	        }
	        
	        if (response.has("raw_output")) {
	            JSONArray rawOutput = response.getJSONArray("raw_output");
	            List<Map<String, Object>> rawOutputList = new ArrayList<>();
	            for (int i = 0; i < rawOutput.length(); i++) {
	                JSONObject entity = rawOutput.getJSONObject(i);
	                Map<String, Object> entityMap = new HashMap<>();
	                entityMap.put("start", entity.getInt("start"));
	                entityMap.put("end", entity.getInt("end"));
	                entityMap.put("text", entity.getString("text"));
	                entityMap.put("label", entity.getString("label"));
	                entityMap.put("score", entity.getDouble("score"));
	                rawOutputList.add(entityMap);
	            }
	            responseMap.put("raw_output", rawOutputList);
	        }
	        
	        if (response.has("output")) {
	            responseMap.put("output", response.getString("output"));
	        }
	        
	        if (response.has("input")) {
	            responseMap.put("input", response.getString("input"));
	        }
	        
	        if (response.has("status")) {
	            responseMap.put("status", response.getString("status"));
	        } else {
	            responseMap.put("status", "success");
	        }
	        
	        if (response.has("message")) {
	            responseMap.put("message", response.getString("message"));
	        } else {
	            responseMap.put("message", "");
	        }
	        
	        return new NerModelEngineResponse(responseMap, 0, 0);
	    } else {
	        classLogger.error("Null response from model request");
	        Map<String, Object> errorMap = new HashMap<>();
	        errorMap.put("status", "error");
	        errorMap.put("message", "Null response from model request");
	        
	        return new NerModelEngineResponse(errorMap, 0, 0);
	    }
	}
	
	/**
	 * Create NerModelEngineResponse from Python output
	 * 
	 * @param pythonOutput The output from the Python LocalNER.predict() method
	 * @return NerModelEngineResponse instance
	 */
	@SuppressWarnings("unchecked")
	public static NerModelEngineResponse fromPython(Object pythonOutput) {
	    Logger classLogger = LogManager.getLogger(NerModelEngineResponse.class);
	    
	    if (pythonOutput == null) {
	        classLogger.error("Null response from Python NER request");
	        Map<String, Object> errorMap = new HashMap<>();
	        errorMap.put("status", "error");
	        errorMap.put("message", "Null response from Python NER request");
	        
	        return new NerModelEngineResponse(errorMap, 0, 0);
	    }
	    
	    try {
	        // Expecting a Map from Python (dictionary)
	        if (!(pythonOutput instanceof Map)) {
	            classLogger.error("Expected Map from Python, got: " + pythonOutput.getClass().getSimpleName());
	            Map<String, Object> errorMap = new HashMap<>();
	            errorMap.put("status", "error");
	            errorMap.put("message", "Invalid response format from Python NER");
	            
	            return new NerModelEngineResponse(errorMap, 0, 0);
	        }
	        
	        Map<String, Object> pythonResponse = (Map<String, Object>) pythonOutput;
	        Map<String, Object> responseMap = new HashMap<>();
	        
	        // Extract mask_values
	        if (pythonResponse.containsKey("mask_values")) {
	            Object maskValuesObj = pythonResponse.get("mask_values");
	            if (maskValuesObj instanceof Map) {
	                Map<String, String> maskValuesMap = new HashMap<>();
	                Map<?, ?> rawMaskValues = (Map<?, ?>) maskValuesObj;
	                for (Map.Entry<?, ?> entry : rawMaskValues.entrySet()) {
	                    if (entry.getKey() != null && entry.getValue() != null) {
	                        maskValuesMap.put(entry.getKey().toString(), entry.getValue().toString());
	                    }
	                }
	                responseMap.put("mask_values", maskValuesMap);
	            }
	        }
	        
	        // Extract entities (the entity types that were searched for)
	        if (pythonResponse.containsKey("entities")) {
	            Object entitiesObj = pythonResponse.get("entities");
	            if (entitiesObj instanceof List) {
	                List<String> entitiesList = new ArrayList<>();
	                List<?> rawEntities = (List<?>) entitiesObj;
	                for (Object entity : rawEntities) {
	                    if (entity != null) {
	                        entitiesList.add(entity.toString());
	                    }
	                }
	                responseMap.put("entities", entitiesList);
	            }
	        }
	        
	        // Extract raw_output (detected entities)
	        if (pythonResponse.containsKey("raw_output")) {
	            Object rawOutputObj = pythonResponse.get("raw_output");
	            if (rawOutputObj instanceof List) {
	                List<Map<String, Object>> rawOutputList = new ArrayList<>();
	                List<?> rawOutput = (List<?>) rawOutputObj;
	                for (Object entityObj : rawOutput) {
	                    if (entityObj instanceof Map) {
	                        Map<?, ?> entityMap = (Map<?, ?>) entityObj;
	                        Map<String, Object> formattedEntity = new HashMap<>();
	                        
	                        // Extract entity properties with safe type conversion
	                        if (entityMap.containsKey("start")) {
	                            Object startObj = entityMap.get("start");
	                            formattedEntity.put("start", startObj instanceof Number ? ((Number) startObj).intValue() : 0);
	                        }
	                        
	                        if (entityMap.containsKey("end")) {
	                            Object endObj = entityMap.get("end");
	                            formattedEntity.put("end", endObj instanceof Number ? ((Number) endObj).intValue() : 0);
	                        }
	                        
	                        if (entityMap.containsKey("text")) {
	                            Object textObj = entityMap.get("text");
	                            formattedEntity.put("text", textObj != null ? textObj.toString() : "");
	                        }
	                        
	                        if (entityMap.containsKey("label")) {
	                            Object labelObj = entityMap.get("label");
	                            formattedEntity.put("label", labelObj != null ? labelObj.toString() : "");
	                        }
	                        
	                        if (entityMap.containsKey("score")) {
	                            Object scoreObj = entityMap.get("score");
	                            formattedEntity.put("score", scoreObj instanceof Number ? ((Number) scoreObj).doubleValue() : 0.0);
	                        }
	                        
	                        rawOutputList.add(formattedEntity);
	                    }
	                }
	                responseMap.put("raw_output", rawOutputList);
	            }
	        }
	        
	        // Extract output (masked text or original text)
	        if (pythonResponse.containsKey("output")) {
	            Object outputObj = pythonResponse.get("output");
	            responseMap.put("output", outputObj != null ? outputObj.toString() : "");
	        }
	        
	        // Extract input (original text)
	        if (pythonResponse.containsKey("input")) {
	            Object inputObj = pythonResponse.get("input");
	            responseMap.put("input", inputObj != null ? inputObj.toString() : "");
	        }
	        
	        // Set status and message
	        if (pythonResponse.containsKey("status")) {
	            Object statusObj = pythonResponse.get("status");
	            responseMap.put("status", statusObj != null ? statusObj.toString() : "success");
	        } else {
	            responseMap.put("status", "success");
	        }
	        
	        if (pythonResponse.containsKey("message")) {
	            Object messageObj = pythonResponse.get("message");
	            responseMap.put("message", messageObj != null ? messageObj.toString() : "");
	        } else {
	            responseMap.put("message", "");
	        }
	        
	        return new NerModelEngineResponse(responseMap, 0, 0);
	        
	    } catch (Exception e) {
	        classLogger.error("Error parsing Python NER response", e);
	        Map<String, Object> errorMap = new HashMap<>();
	        errorMap.put("status", "error");
	        errorMap.put("message", "Failed to parse Python NER response: " + e.getMessage());
	        
	        return new NerModelEngineResponse(errorMap, 0, 0);
	    }
	}
}
