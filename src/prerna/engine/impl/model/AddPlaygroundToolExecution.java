package prerna.engine.impl.model;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;


/**
 * AddToolExecutionReactor:
 *   Input: roomId, toolId, toolName, tool_execution_response
 */
public class AddPlaygroundToolExecution extends prerna.reactor.AbstractReactor {
	
    private static final Gson gson = new Gson();

    public AddPlaygroundToolExecution() {
        this.keysToGet = new String[]{
        	ReactorKeysEnum.ENGINE.getKey(),		
            "roomId",          // 1
            "toolId",          // 2
            "toolName",        // 3
            "tool_execution_response" // 4
        };
        this.keyRequired = new int[]{1, 1, 1, 1, 1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String modelId    = this.keyValue.get(this.keysToGet[0]);
        String roomId    = this.keyValue.get(this.keysToGet[1]);
        String toolId    = this.keyValue.get(this.keysToGet[2]);
        String toolName  = this.keyValue.get(this.keysToGet[3]);
        String toolResponseRaw = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[4]));

        User user = this.insight.getUser();
        String userId = user.getPrimaryLoginToken().getId();

    	if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
    		throw new IllegalArgumentException(
    				"Model " + modelId + " does not exist or user does not have access to this model");
    	}
    	IModelEngine modelEngine = Utility.getModel(modelId);

        // --- 1. Security/room loading ---
        if (!ModelInferenceLogsUtils.validUserRoom(roomId, userId)) {
            throw new IllegalArgumentException("User does not have access to room " + roomId);
        }
        Room room = ModelInferenceLogsUtils.getRoomById(roomId, userId);
        room.setInsight(insight);
        room.parseMessages();

        List<AbstractMessage> messages = room.getMessages();
        if (messages.isEmpty()) {
            throw new IllegalStateException("Room message history is empty. Cannot add tool execution results.");
        }

        AskModelEngineResponse response = room.addToolExecutionResult(toolId, toolName, toolResponseRaw, modelEngine, insight);
        
        Map<String, Object> pixelReturn = new HashMap<>();


         if(response==null) {
             pixelReturn.put("responseMessage", "Tool output added successfully. Additional tool executions required to continue");
            return new NounMetadata("Tool output added successfully", PixelDataType.CONST_STRING);
        } else {
            pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(room.getMessages().getLast())));
            return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
        }
    }
    
    /**
     * Converts a JSON object string to a Map<String, Object>
     * @param json The JSON string (must be a JSON object: { ... })
     * @return The parsed Map
     */
    public static Map<String, Object> jsonToMap(String json) {
        if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
            throw new IllegalArgumentException("Input must be a valid JSON object string.");
        }
        return gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
    }
}

