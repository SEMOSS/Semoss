package prerna.playground.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class CreateQuestionPreviewsReactor extends AbstractReactor {

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();
	
	public CreateQuestionPreviewsReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),      // 1, required
	            ReactorKeysEnum.ROOM_ID.getKey(),     // 1, required (Used to grab history)
				"number",							  // 0, default to 3
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()

		};
		
		this.keyRequired = new int[] {1, 1, 0, 0};
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
		Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();
        
		String numResponses = this.keyValue.get(this.keysToGet[2]);
		if (numResponses == null) {
			numResponses = "3";
		}
		String jsonSchemaString = String.format(PlaygroundUtils.CREATE_QUESTION_PREVIEWS_SCHEMA, numResponses, numResponses);

        Map<String, Object> jsonSchemaMap = PlaygroundUtils.jsonToMap(jsonSchemaString);
        
        paramMap.put("schema", jsonSchemaMap);
        
        
        InputMessage inputMsg = InputMessage.builder(room)
                .withInputUIPrompt(PlaygroundUtils.CREATE_QUESTION_PREVIEWS_PROMPT)
                .withInputPrompt(PlaygroundUtils.CREATE_QUESTION_PREVIEWS_PROMPT)
                .withModelType(modelEngine.getModelType())
                .withParamMap(paramMap)
                .build();

        String parentMessageId = null;
        Boolean appendToHistory = false;
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
		return "Generates a list of suggested follow up responses to use as the next message based on the room conversation";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The model engine that generates the follow up responses";
		} 
		return super.getDescriptionForKey(key);
	}

	
}

