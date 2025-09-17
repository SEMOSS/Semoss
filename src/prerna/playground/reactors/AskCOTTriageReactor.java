package prerna.playground.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
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
		organizeKeys();
		String modelId = this.keyValue.get(this.keysToGet[0]);
		
        User user = this.insight.getUser();
        if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
            throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access to this model");
        }
		
        String userQuery = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[2]));
        String roomId = this.keyValue.get(this.keysToGet[1]);
        
        IModelEngine modelEngine = Utility.getModel(modelId);
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, userQuery);        
        
        // ==== Step 2. Gather Tool Descriptions from the room ====
        List<String> mcpToolNames = new ArrayList<>();
		List<Map<String, Object>> toolMap = room.getAllToolsJsonForRoom();
		for(Map<String, Object> tool : toolMap) {
			mcpToolNames.add((String) tool.get("name"));
		}
		String toolsDescription=  GSON.toJson(room.getAllToolsJsonForRoom());
		
		
        Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();
		
        //get all the mcp IDs and format them into a string
        String formattedEnum;
        if (mcpToolNames.isEmpty()) {
            formattedEnum = "\"none, DO NOT CHOOSE THIS ANY OF.\""; //this is some experimentation ... 
        } else {
            formattedEnum = mcpToolNames.stream().map(t -> "\"" + t + "\"").collect(Collectors.joining(", "));
        }
		
		//put the string of mcp ids into the below
        String formattedSchemaJson = PlaygroundUtils.TRIAGE_SCHEMA.formatted(formattedEnum);
        Map<String, Object> jsonSchemaMap = GSON.fromJson(formattedSchemaJson, new TypeToken<Map<String, Object>>(){}.getType());
		
		
        
        

        paramMap.put("schema", jsonSchemaMap);

        InputMessage inputMsg = InputMessage.builder(room)
            .withInputUIPrompt(userQuery)
            .withInputPrompt(userQuery)
            .withModelType(modelEngine.getModelType())
            .withParamMap(paramMap)
            .build(); // 
        
        ResponseMessage response = room.ask(inputMsg, PlaygroundUtils.TRIAGE_PROMPT, modelEngine);
        
       
        //TODO: test. If JSON Schema responses are enforced, this should work without catching.
        //Copy AskCOTRoom ResponseMessage parsing if this is currently nonfunctional
        Object pixelReturn = "";
        try {
        	pixelReturn = GSON.fromJson(response.getContent(), new TypeToken<Map<String, Object>>() {}.getType());
        }
        catch(JsonSyntaxException e) {
        	throw e;
        }
        
        
		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
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
