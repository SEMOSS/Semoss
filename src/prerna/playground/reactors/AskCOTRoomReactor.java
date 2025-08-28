package prerna.playground.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AskCOTRoomReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(Room.class);

	
    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .disableHtmlEscaping()
            .create();

    public AskCOTRoomReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.ENGINE.getKey(),      // 0, required
            ReactorKeysEnum.VECTORDB.getKey(),    // 1, optional (can be null)
            ReactorKeysEnum.ROOM_ID.getKey(),     // 2, optional (not required, will use insight)
            ReactorKeysEnum.COMMAND.getKey(),     // 3, required (actual user query)
            ReactorKeysEnum.CONTEXT.getKey(),     // 4, tbd on how it is used
            ReactorKeysEnum.IMAGE.getKey(),       // 5, optional, TODO: add in support
            ReactorKeysEnum.URL.getKey(),         // 6, optional, TODO: add in support
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 7, optional
        };

        this.keyRequired = new int[]{1, 0, 0, 1, 0, 0, 0, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        
        // Required
        String modelId = this.keyValue.get(this.keysToGet[0]);
        String userQuery = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[3]));
        // Optional
        List<String> vectorDbIds = getVectorDbIds();
        String roomId = this.keyValue.get(this.keysToGet[2]);
        // context, images, URLs: future - see keysToGet map
        
        User user = this.insight.getUser();
        if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
            throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access to this model");
        }
        
        // Room and Engine
        IModelEngine modelEngine = Utility.getModel(modelId);
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, userQuery);

	    // ==== Step 1. Grab RAG context if vectorDB present ==== 
		StringBuilder joinedContextBuilder = new StringBuilder();

		if (vectorDbIds != null && !vectorDbIds.isEmpty()) {
		    int chunkLimit = 3; // TODO: configurable
		    for (String vectorDbId : vectorDbIds) {
		        if (vectorDbId == null || vectorDbId.trim().isEmpty()) continue;
		        if (!SecurityEngineUtils.userCanViewEngine(user, vectorDbId)) {
		        	classLogger.info("User does not have access to vector db : " + vectorDbId);
		            continue;
		        }
		        IVectorDatabaseEngine vectorDbEng = Utility.getVectorDatabase(vectorDbId);
		        if (vectorDbEng == null) continue; // Or log as missing/bad engine.

		        List<Map<String, Object>> output = vectorDbEng.nearestNeighbor(this.insight, userQuery, chunkLimit, null);
		        for (Map<String, Object> chunk : output) {
		            String content = (String) chunk.get(VectorDatabaseCSVTable.CONTENT);
		            if (content != null && !content.isEmpty()) {
		                joinedContextBuilder.append(content).append("\n");
		            }
		        }
		    }
		}
		String joinedChunks = joinedContextBuilder.toString();
        // ==== Step 2. Gather Tool Descriptions from the room ====
        List<String> toolIDs = getToolIdsForRoom(room);
        String toolsDescription = assembleToolsDescription(toolIDs);

        
        // ==== Step 3. Build Prompts ====
        String cotSchema = PlaygroundUtils.COT_JSON_SCHEMA.replaceAll("\\s+", " ");
        String userPrompt = String.format(
            PlaygroundUtils.COT_PROMPT_TEMPLATE,
            toolsDescription,
            joinedChunks,
            userQuery,
            cotSchema
        );

        Map<String, Object> paramMap = getParamMap();
        if (paramMap == null) paramMap = new HashMap<>();
        paramMap.put("schema", PlaygroundUtils.COT_JSON_SCHEMA);

        InputMessage inputMsg = InputMessage.builder(room)
            .withInputUIPrompt(userQuery)
            .withInputPrompt(userPrompt)
            .withModelType(modelEngine.getModelType())
            .withParamMap(paramMap)
            .build(); // 

        // ==== Step 4. Run LLM ====
        ResponseMessage response = room.ask(inputMsg, PlaygroundUtils.COT_SYSTEM_PROMPT, modelEngine);

        
     // ==== Step 5. Try to parse as COT JSON ====
        Map<String, Object> cotJson = null;
        boolean isValidJson = false;
        try {
            cotJson = jsonToMap(response.getContent());
            isValidJson = (cotJson != null && cotJson.containsKey("steps"));
        } catch (Exception ex) {
        	classLogger.info("Could not parse response from model into a COT json");
        }

        if (isValidJson) {
            response.setMessageType(MessageType.RESPONSE_COT); // 
        }
        
		// ---- Return both messages as a Map
		Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);

    }
    
    // ====== UTILITIES ======

	/**
	 * 
	 * @return
	 */
	public List<String> getVectorDbIds() {
		List<String> inputStrings = new ArrayList<>();
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.VECTORDB.getKey());
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
			return inputStrings;
		}
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
		return inputStrings;
	}
    /**
     * 
     * @return
     */
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

    /**
     * Get list of tool IDs from room options (no lookup or map building).
     */
    private List<String> getToolIdsForRoom(Room room) {
        if (room != null && room.getOptionsMap() != null && room.getOptionsMap().containsKey("tools")) {
            Object toolsObj = room.getOptionsMap().get("tools");
            if (toolsObj instanceof List<?>) {
                List<?> toolsList = (List<?>) toolsObj;
                List<String> result = new LinkedList<>();
                for (Object t : toolsList) {
                    if (t instanceof String) {
                        result.add((String) t);
                    }
                }
                return result;
            }
        }
        return new LinkedList<>();
    }

    private String assembleToolsDescription(List<String> toolId) {
        if (toolId == null || toolId.isEmpty()) return "No tools.";
        // TO DO
        return "some tool";
    }

    /** Converts a JSON object string to a Map<String, Object>. */
    public static Map<String, Object> jsonToMap(String json) {
        if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
            throw new IllegalArgumentException("Input must be a valid JSON object string.");
        }
        return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
    }

    @Override
    public String getReactorDescription() {
        return """
        Takes a user's query and returns a chain-of-thought JSON plan by combining tool descriptions,
        RAG context (if vector db is provided), and the original query according to a strict JSON schema.
        If the output matches the schema, the type is 'COT'. Otherwise, returns as simple chat (type 'CHAT').
        """;
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if(key.equals(ReactorKeysEnum.ENGINE.getKey())) {
            return "The engine id of the model used for the message.";
        } else if(key.equals(ReactorKeysEnum.VECTORDB.getKey())) {
            return "The vector db for knowledge search for this room/query (optional).";
        } else if(key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
            return "The room id corresponding to message history (optional, used for context/history/tools if provided).";
        } else if(key.equals(ReactorKeysEnum.COMMAND.getKey())) {
            return "The raw user query.";
        }
        return super.getDescriptionForKey(key);
    }
}