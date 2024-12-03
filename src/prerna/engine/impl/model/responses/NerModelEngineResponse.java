package prerna.engine.impl.model.responses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONArray;


public class NerModelEngineResponse extends AbstractModelEngineResponse<Map<String, Object>> {
    private static final Logger classLogger = LogManager.getLogger(NerModelEngineResponse.class);
    private static final long serialVersionUID = 1L;
    
    public static final String MESSAGE_ID = "messageId";
	public static final String ROOM_ID = "roomId";

    private String messageId;
    private String roomId;

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
	
	public static NerModelEngineResponse fromJson(JSONObject response) {
		if (response != null) {      
	        Map<String, Object> responseMap = new HashMap<>();
	        
	        // Handle mask_values
	        JSONObject maskValues = response.getJSONObject("mask_values");
	        Map<String, String> maskValuesMap = new HashMap<>();
	        for (String key : maskValues.keySet()) {
	            maskValuesMap.put(key, maskValues.getString(key));
	        }
	        responseMap.put("mask_values", maskValuesMap);
	        
	        // Handle entities
	        JSONArray entitiesArray = response.getJSONArray("entities");
	        List<String> entitiesList = new ArrayList<>();
	        for (int i = 0; i < entitiesArray.length(); i++) {
	            entitiesList.add(entitiesArray.getString(i));
	        }
	        responseMap.put("entities", entitiesList);
	        
	        // Handle raw_output
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
	        
	        responseMap.put("output", response.getString("output"));
	        responseMap.put("input", response.getString("input"));
	        responseMap.put("message", response.getString("message"));
	        responseMap.put("status", response.getString("status"));
	        
	        return new NerModelEngineResponse(responseMap, 0, 0);
		} else {
            classLogger.error("Null response from model request");
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("status", "error");
            errorMap.put("message", "Null response from model request");
            
            return new NerModelEngineResponse(errorMap, 0, 0);
		}
		
	}
}
