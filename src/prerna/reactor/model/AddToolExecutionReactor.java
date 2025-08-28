package prerna.reactor.model;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * AddToolExecutionReactor:
 *   Input: roomId, toolId, toolName, tool_execution_response
 */
public class AddToolExecutionReactor extends AbstractReactor {

	@Deprecated
	private final String tool_execution_response = "tool_execution_response";

    public AddToolExecutionReactor() {
        this.keysToGet = new String[]{
        	ReactorKeysEnum.ENGINE.getKey(), // 0	
            "roomId",          // 1
            "toolId",          // 2
            "toolName",        // 3
			"toolExecutionResponse", // 4
			"toolParameterValues", // 5
			tool_execution_response
        };
		//TODO: once we remove the legacy tool_execution_response, we will make toolExecutionResponse mandatory field
        this.keyRequired = new int[]{1, 1, 1, 0, 0, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String modelId    = this.keyValue.get(this.keysToGet[0]);
        String roomId    = this.keyValue.get(this.keysToGet[1]);
        String toolId    = this.keyValue.get(this.keysToGet[2]);
        String toolName  = this.keyValue.get(this.keysToGet[3]);
		String toolResponseRaw = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[4]));
		if(toolResponseRaw == null) {
			toolResponseRaw = Utility.decodeURIComponent(this.keyValue.get(tool_execution_response));
		}
		if(toolResponseRaw == null) {
			throw new IllegalArgumentException("Field " + this.keysToGet[4] + " cannot be empty");
		}
		Map<String, Object> toolParamterValues = getToolParamterValues();

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
        
        Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);

        List<AbstractMessage> messages = room.getMessages();
        if (messages.isEmpty()) {
            throw new IllegalStateException("Room message history is empty. Cannot add tool execution results.");
        }

        AskModelEngineResponse response = room.addToolExecutionResult(toolId, toolName, toolResponseRaw, toolParamterValues,
        		null, modelEngine, insight);
        
        if(response==null) {
            return new NounMetadata("Tool output added successfully", PixelDataType.CONST_STRING);
        } else {
            return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
        }
    }
    
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getToolParamterValues() {
		GenRowStruct toolParamValuesGrs = this.store.getNoun(this.keysToGet[5]);
		if(toolParamValuesGrs != null) {
			Object toolParamValuesObj = toolParamValuesGrs.get(0);
			if(toolParamValuesObj instanceof Map) {
				return (Map<String, Object>) toolParamValuesObj;
			} else {
				throw new IllegalArgumentException("Expected " + this.keysToGet[5] + " to be a Map object");
			}
		}
		
		return null;
	}
}

