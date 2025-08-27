package prerna.reactor.model;

import java.util.List;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
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
	
    public AddToolExecutionReactor() {
        this.keysToGet = new String[]{
        	ReactorKeysEnum.ENGINE.getKey(),		
            "roomId",          // 1
            "toolId",          // 2
            "toolName",        // 3
            "tool_execution_response" // 4
        };
        this.keyRequired = new int[]{1, 1, 1, 1};
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
        
        Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);

        List<AbstractMessage> messages = room.getMessages();
        if (messages.isEmpty()) {
            throw new IllegalStateException("Room message history is empty. Cannot add tool execution results.");
        }

        AskModelEngineResponse response = room.addToolExecutionResult(toolId, toolName, toolResponseRaw, 
        		null, modelEngine, insight);
        
        if(response==null) {
            return new NounMetadata("Tool output added successfully", PixelDataType.CONST_STRING);
        } else {
            return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
        }
    }
}

