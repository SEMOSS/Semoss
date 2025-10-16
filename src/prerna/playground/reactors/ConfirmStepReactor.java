package prerna.playground.reactors;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ConfirmStepReactor extends AbstractReactor {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();
	
	public ConfirmStepReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),      // 0, required
	            ReactorKeysEnum.ROOM_ID.getKey(),     // 1, required (this + stepNumber necessary to grab plan/step)
	            "stepNumber",						  //2, required
	            "toolResponse",						  //3, required
	            ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 4, optional

		};
		
		this.keyRequired = new int[] {1, 0, 1, 1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String modelId = this.keyValue.get(this.keysToGet[0]);
		
		User user = this.insight.getUser();
        if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
            throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access to this model");
        }
        IModelEngine modelEngine = Utility.getModel(modelId);
        
        //For now, just grab first response message (index 1)
        //This needs to be changed (probably)
        
        String roomId = this.keyValue.get(this.keysToGet[1]);
		Room room = RoomUtils.getOrLoadRoom(roomId, insight);        

		AbstractMessage abstractMessage = room.getMessages().get(1); //Second Message should be the COT
		ResponseMessage message = null;
		if (abstractMessage instanceof ResponseMessage) message = (ResponseMessage) abstractMessage;
		
		String plan = message.getContent();

		String userPrompt = String.format(PlaygroundUtils.CONFIRM_STEP_PROMPT_TEMPLATE, plan, this.keyValue.get("stepNumber"), this.keyValue.get("toolReseponse"));
		
		
		Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();
        
        Map<String, Object> jsonSchemaMap = PlaygroundUtils.jsonToMap(PlaygroundUtils.CONFIRM_STEP_SCHEMA);
        
        paramMap.put("schema", jsonSchemaMap);
        
        
        InputMessage inputMsg = InputMessage.builder(room)
                .withInputUIPrompt(userPrompt)
                .withInputPrompt(userPrompt)
                .withModelType(modelEngine.getModelType())
                .withParamMap(paramMap)
                .build();
            
        ResponseMessage response = room.ask(inputMsg, PlaygroundUtils.CONFIRM_STEP_SYSTEM_PROMPT, modelEngine);
        response.setParentMessageId(inputMsg.getParentMessageId()); //remove from message history
        
        
        String jsonResponse = response.getContent();
        
        Map<String, Object> toolCall = PlaygroundUtils.jsonToMap(jsonResponse);
        return new NounMetadata(toolCall, PixelDataType.MAP);
        //TODO: determine if we return the response and description map, true/false,
        //or full inputMessage and responseMessages.
        
        
//        Map<String, Object> pixelReturn = new LinkedHashMap<>();
//
//		pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(inputMsg)));
//		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));
//
//		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	
	
	private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
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
	
}
