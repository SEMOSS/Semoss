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

//public GetCOTToolResponseReactor() {
//	this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
//			ReactorKeysEnum.ROOM_ID.getKey(), // 1 (optional, for history)
//			"stepNumber", // 2 (required)
//			"toolName", // 3 (required)
//			// TODO remove this - likely not needed
//			"toolMeta", // 4 (optional: schema/options/desc for the tool)
//			// TODO remove this - likely not needed
//			"context", // 5 (optional: additional context)
//	};

public class ConfirmStepReactor extends AbstractReactor {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();
	
	public ConfirmStepReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),      // 0, required
	            ReactorKeysEnum.ROOM_ID.getKey(),     // 1, required (this + stepNumber necessary to grab plan/step)
	            "stepNumber",						  //2, required
	            "toolResponse",
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
        
        //String query = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[2]));
        //String toolResponse = this.keyValue.get(this.keysToGet[3]);
        
        //String queryForModel = query + toolResponse;
        
        //possibly, switch from createRoom to loadRoom, as room is now (or should be) a necessary reactor)
        
        //TODO: we need some standardized way to grab the COT message every time.
        //QUestions: Is there only one COT message per Room? probably not since we regenerate?
        //So how can we determine which COT we want just based on the RoomID.
        
        //For now, just grab first response message (index 1)
        
        String roomId = this.keyValue.get(this.keysToGet[1]);
		Room room = RoomUtils.getOrLoadRoom(roomId, insight);        

		AbstractMessage abstractMessage = room.getMessages().get(1); //Second Message should be the COT
		ResponseMessage message = null;
		if (abstractMessage instanceof ResponseMessage) message = (ResponseMessage) abstractMessage;
		
		String plan = message.getContent();

		/**
		 * Now time to construct the prompt!
		 * 
		 * We will need the full message (so hopefully thats the steps)
		 * The step number, which will clarify the... well yea
		 * and the tool response.
		 */
		
		String userPrompt = String.format(PlaygroundUtils.CONFIRM_STEP_PROMPT_TEMPLATE, plan, this.keyValue.get("stepNumber"), this.keyValue.get("toolReseponse"));
		
		//that should be the step message, as thats the "last one saved", and as such, that + stepNUmber
		//should be all the context the llm needs
		
		
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
        
        Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
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
