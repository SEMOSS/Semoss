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

public class ConfirmCOTStepReactor extends AbstractReactor {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();
	
	public ConfirmCOTStepReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),      // 0, required
	            ReactorKeysEnum.ROOM_ID.getKey(),     // 1, required (this + stepNumber necessary to grab plan/step)
	            "stepNumber",						  //2, required
	            ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 3, optional

		};
		
		this.keyRequired = new int[] {1, 0, 1, 0};
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
        
        
        String roomId = this.keyValue.get(this.keysToGet[1]);
		Room room = RoomUtils.getOrLoadRoom(roomId, insight);        
		String lastMessageId = room.getMessages().getLast().getMessageId();
		
		String roomHistory = MessageUtils.getMessageHistoryFromMessageId(room.getMessages(), lastMessageId);

		String userPrompt = String.format(PlaygroundUtils.CONFIRM_STEP_PROMPT_TEMPLATE, this.keyValue.get("stepNumber"), roomHistory);
		
		Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();
        
        Map<String, Object> jsonSchemaMap = PlaygroundUtils.jsonToMap(PlaygroundUtils.CONFIRM_STEP_SCHEMA);
        
        paramMap.put("schema", jsonSchemaMap);
        
        
        InputMessage inputMsg = InputMessage.builder(room)
        		.withSystemPrompt(PlaygroundUtils.CONFIRM_STEP_SYSTEM_PROMPT)
                .withInputUIPrompt(userPrompt)
                .withInputPrompt(userPrompt)
                .withModelType(modelEngine.getModelType())
                .withParamMap(paramMap)
                .build();
        
        Boolean appendToHistory = false;
        String parentMessageId = null;
        ResponseMessage response = room.ask(inputMsg, modelEngine, parentMessageId, appendToHistory);        
        
        String jsonResponse = response.getContent();
        
        Map<String, Object> pixelResponse = PlaygroundUtils.jsonToMap(jsonResponse);
        return new NounMetadata(pixelResponse, PixelDataType.MAP);
        
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
	
	@Override
	public String getReactorDescription() {
		return "Generates a review of a single step of a Chain of Thought (COT) message. Will return either [continue] or "
				+ "[regenerate] depending on the viability of said step";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The model engine that generates the step confirmation";
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room id corresponding to the COT and its message history.";
		} else if (key.equals("stepNumber")) {
			return "The step number of the Chain of Thought (COT) message";
		}
		return super.getDescriptionForKey(key);
	}
	
}
