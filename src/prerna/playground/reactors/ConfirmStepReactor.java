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

//Info

//Inputs are 1. the tool response. 2. the plan
//and we have some prompt and Schema i'm guessing to inform and format the answer
//and thats it?
//I can do that then get back to Neel

public class ConfirmStepReactor extends AbstractReactor {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();
	
	public ConfirmStepReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),      // 0, required
	            ReactorKeysEnum.ROOM_ID.getKey(),     // 1, optional (not required, will use insight)
	            ReactorKeysEnum.COMMAND.getKey(),     // 2, required (query from frontend)
				ReactorKeysEnum.JSON.getKey(),	      // 3, tool response, formatted as a string
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
        
        String query = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[2]));
        String toolResponse = this.keyValue.get(this.keysToGet[3]);
        
        String queryForModel = query + toolResponse;
        
        String roomId = this.keyValue.get(this.keysToGet[1]);
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, query);        

		Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();
        
        Map<String, Object> jsonSchemaMap = PlaygroundUtils.jsonToMap(PlaygroundUtils.CONFIRM_STEP_SCHEMA);
        
        paramMap.put("schema", jsonSchemaMap);
        
        
        InputMessage inputMsg = InputMessage.builder(room)
                .withInputUIPrompt(query)
                .withInputPrompt(queryForModel)
                .withModelType(modelEngine.getModelType())
                .withParamMap(paramMap)
                .build();
            
        ResponseMessage response = room.ask(inputMsg, PlaygroundUtils.CONFIRM_STEP_PROMPT, modelEngine);
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
