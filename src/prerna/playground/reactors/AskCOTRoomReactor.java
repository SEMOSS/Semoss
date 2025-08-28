package prerna.playground.reactors;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AskCOTRoomReactor extends AbstractReactor {

	private static final Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

		//TODO: fields?
	
	public AskCOTRoomReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.VECTORDB.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.CONTEXT.getKey(),
				ReactorKeysEnum.IMAGE.getKey(),
				ReactorKeysEnum.URL.getKey(),
				"mcpToolID",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
		};
		this.keyRequired = new int[] { 1, 1, 1, 1, 0, 0, 0, 0, 0 };
	}
	
	
	@Override
	public NounMetadata execute() {
		
		String vectorDbId = this.keyValue.get(this.keysToGet[1]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), vectorDbId)) {
			throw new IllegalArgumentException("Vector db " + vectorDbId + " does not exist or user does not have access to it.");
		}
		
        IVectorDatabaseEngine vectorDbEng = Utility.getVectorDatabase(vectorDbId);
        if (vectorDbEng == null) {
            throw new RuntimeException("Unable to find engine");
        }
        
        //TODO: 
        //1. move into new method/class potentially, process the searchStatement rather
        //   than use raw user query
        //2. do not hardcode limit
        //3. configure paramMap (for now its fine?)
       
        String searchStatement = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[3]));
        int limit = 3;
        Map<String, Object> paramMap = null;
        
        //TODO: verify, do we want to directly work with output? which is a map of the
        //		json response from the vectorDB. or is there some universal handler? etc
        //		so we can get shared datatypes, like chunks, pulled out.
        
        //CLARIFICATION: wait, I found this is the actual output
        
        
		/**
		JsonObject sourceDetails = hitJson.get("_source").getAsJsonObject();
		thisMatch.put(VectorDatabaseCSVTable.SOURCE, sourceDetails.get(VectorDatabaseCSVTable.SOURCE).getAsString());
		thisMatch.put(VectorDatabaseCSVTable.MODALITY, sourceDetails.get(VectorDatabaseCSVTable.MODALITY).getAsString());
		thisMatch.put(VectorDatabaseCSVTable.DIVIDER, sourceDetails.get(VectorDatabaseCSVTable.DIVIDER).getAsString());
		thisMatch.put(VectorDatabaseCSVTable.PART, sourceDetails.get(VectorDatabaseCSVTable.PART).getAsString());
		thisMatch.put(VectorDatabaseCSVTable.TOKENS, sourceDetails.get(VectorDatabaseCSVTable.TOKENS).getAsLong());
		thisMatch.put(VectorDatabaseCSVTable.CONTENT, sourceDetails.get(VectorDatabaseCSVTable.CONTENT).getAsString());
	}
	return vectorSearchResults;
	**/
        
        
        List<Map<String, Object>> output = vectorDbEng.nearestNeighbor(this.insight, searchStatement, limit, paramMap);
		//Grab from 0 to end, eaach VectorDatabaseCSVTable.CONTENT
        //put that into the message
        //use that to build teh inputMessage
        
        List<String> chunkList = new LinkedList<String>();
        for (Map<String, Object> chunk: output) {
        	chunkList.add((String) chunk.get(VectorDatabaseCSVTable.CONTENT));
        }
        
        String template = "A RAG context delimited by triple backticks is provided below. \n"
        		+ "                \\`\\`\\` %s \\`\\`\\`\\\\n\r\n"
        		+ "User Query: %s";
        String joinedResults = String.join("\n", chunkList);
        String command = String.format(template, joinedResults, searchStatement);
        
        
        //TODO: insert JSON schema somehow? may not be implemented yet
		// ---- Build the InputMessage
		InputMessage msg = InputMessage.builder(room)
				.withInputUIPrompt(question)
				.withInputPrompt(question)
				.withModelType(modelEngine.getModelType())
				.withParamMap(paramMap)
				.withImages(copiedImages, room)
				.withImageUrls(inputImageURLs)
				.withTools(tools)
				.build();

		// ---- Actually run LLM call
		ResponseMessage response = room.ask(msg, context, modelEngine);

		//TODO: determine if response is JSON
		
		//TODO: if not JSON, just return, if is JSON, we need to break down the steps? and store them?
	
		//TODO: return step objects (COT) and other components to front end in map?
		
		//TODO: we potentially store steps/response in the chat history as well. (room?)
		
		return null;
	}
	
    /**
     * Converts a JSON object string to a Map<String, Object>
     * @param json The JSON string (must be a JSON object: { ... })
     * @return The parsed Map
     */
    public static Map<String, Object> jsonToMap(String json) {
        if (json == null || json.trim().isEmpty() || !json.trim().startsWith("{")) {
            throw new IllegalArgumentException("Input must be a valid JSON object string.");
        }
        return gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
    }
	
	}
