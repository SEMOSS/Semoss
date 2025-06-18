package prerna.engine.impl.model;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class LLM2Reactor extends AbstractReactor {
	
	public LLM2Reactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM.getKey(), ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(),
				 ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 0, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		
		////// SET UP //////////
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM.getKey());
		User user = this.insight.getUser();
		String userId = user.getPrimaryLoginToken().getId();
		AccessToken userToken = user.getPrimaryLoginToken();

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
//		// default is true
//		Boolean useHistoryParam = Boolean.parseBoolean(this.keyValue.getOrDefault(ReactorKeysEnum.USE_HISTORY.getKey(), "true")+"");

		String question = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.COMMAND.getKey()));
		String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
		if (context != null) {
			context = Utility.decodeURIComponent(context);
		}
		
		Map<String, Object> paramMap = getMap();
		IModelEngine modelEngine = Utility.getModel(engineId);
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		Room room = null;
		
		//TODO i should check the user has first if its there then the db if it exits
        boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckConversationExists(roomId);
        if(!roomExistsInDB) {
        	//create room a room
        	if(roomId == null) {
        		roomId=insight.getInsightId();
        	}
        	
	    	String agentType = modelEngine.getCatalogSubType(modelEngine.getSmssProp());
			String userName = userToken.getName();
			String userUsername = userToken.getUsername();
			String userEmail = userToken.getEmail();
			
			String projectId = insight.getContextProjectId();
			if (projectId == null) {
				projectId = insight.getProjectId();
			}
			String projectName = null;
			if (projectId != null) {
				IProject project = Utility.getProject(projectId);
				projectName = project.getProjectName();
			}
			
			String roomName = question.substring(0, Math.min(question.length(), 100));;
			ModelInferenceLogsUtils.doCreateNewConversation(
				roomId,
				roomName, 
				null, 
				userId,
				userName,
				userEmail,
				agentType,
				modelEngine.getEngineId(),
				true, 
				projectId, 
				projectName
			);
			room = RoomUtils.getOrLoadRoom(insight.getInsightId(), userId, this.insight);
        } else {
    		RoomUtils.getOrLoadRoom(roomId,userId , insight);
        }
        
        ///// MESSAGE CREATION //////////

        
        InputMessage msg;
        msg = InputMessage.builder().withInputUIPrompt(question).withInputPrompt(question)
        .withModelType(modelEngine.getModelType())
        .withParamMap(paramMap)
        .build();
        
        
        
        AskModelEngineResponse response = room.ask(msg, insight, modelEngine);
		return new NounMetadata(response.toMap(), PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }
	
	@Override
	public String getReactorDescription() {
		return "This method is used to run an LLM text-generation call";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "This is the prompt to execute against the LLM";
		} else if(key.equals(ReactorKeysEnum.CONTEXT.getKey())) {
			return "The system prompt to use for the LLM call";
		} else if(key.equals(ReactorKeysEnum.ROOM.getKey())) {
			return "This is the room ID that will be used for storing messages. If no room id is passed in, then insight id will be used for the room";
		} else if(key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Map containing the key-value pairs for model parameters like 'temperature', 'top_p', etc. "
					+ "In addition, you can pass in 'full_prompt' to represent a full prompt and history via ChatML format which will ignore inputs for " + 
					Arrays.asList(ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.USE_HISTORY.getKey());
		}

		return super.getDescriptionForKey(key);
	}
	
}
