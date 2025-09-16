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

public class AskCOTTriageReactor extends AbstractReactor {

	
	
    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .disableHtmlEscaping()
            .create();

	
	//TODO:
	//1. Determine params?
	//2. Add schema to thing
	//3. blah blah blah
    public AskCOTTriageReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.ENGINE.getKey(),      // 0, required
            ReactorKeysEnum.ROOM_ID.getKey(),     // 1, optional (not required, will use insight)
            ReactorKeysEnum.COMMAND.getKey(),     // 2, required (actual user query)
            ReactorKeysEnum.CONTEXT.getKey(),     // 3, tbd on how it is used
            ReactorKeysEnum.IMAGE.getKey(),       // 4, optional, TODO: add in support
            ReactorKeysEnum.URL.getKey(),         // 5, optional, TODO: add in support
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 6, optional
        };
        this.keyRequired = new int[]{1, 0, 1, 0, 0, 0, 0};
    }
	
	@Override
	public NounMetadata execute() {

		String modelId = this.keyValue.get(this.keysToGet[0]);
		
        User user = this.insight.getUser();
        if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
            throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access to this model");
        }
		
        String userQuery = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[2]));
        String roomId = this.keyValue.get(this.keysToGet[1]);
        
        IModelEngine modelEngine = Utility.getModel(modelId);
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, userQuery);        
        
        Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();
        paramMap.put("schema", PlaygroundUtils.COT_JSON_SCHEMA);

        InputMessage inputMsg = InputMessage.builder(room)
            .withInputUIPrompt(userQuery)
            .withInputPrompt(userQuery)
            .withModelType(modelEngine.getModelType())
            .withParamMap(paramMap)
            .build(); // 
        
        ResponseMessage response = room.ask(inputMsg, PlaygroundUtils.TRIAGE_PROMPT, modelEngine);
        
		Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
    /** Converts a JSON object string to a Map<String, Object>. */
    public static Map<String, Object> jsonToMap(String json) {
        if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
            throw new IllegalArgumentException("Input must be a valid JSON object string.");
        }
        return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
    }
	
	private Map<String, Object> getParamMap() {
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


}
