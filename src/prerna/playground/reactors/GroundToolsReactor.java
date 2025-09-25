package prerna.playground.reactors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.util.Utility;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.playground.PlaygroundUtils;



public class GroundToolsReactor extends AbstractReactor {

    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .disableHtmlEscaping()
            .create();

	
	public GroundToolsReactor() {
        this.keysToGet = new String[]{
            "toolsMap", //A map of tool:description pairs to ground
        	ReactorKeysEnum.VECTORDB.getKey() //vector db for grounding
        };
        this.keyRequired = new int[]{1, 1};
    }
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
        String tools = this.keyValue.get(this.keysToGet[0]); //TODO: GSON TURN TO MAP
        String vectorDbId = this.keyValue.get(this.keysToGet[1]);
        User user = this.insight.getUser();

		//gson map the toolsMap json string to a real map we can use
		
		//make a loop, and rag search each.
		
		if (!SecurityEngineUtils.userCanViewEngine(user, vectorDbId)) {
            throw new IllegalArgumentException("vector Database " + vectorDbId + " does not exist or user does not have access to this model");
		}
		
		List<Map<String, Object>> toolList = GSON.fromJson(tools, new TypeToken<List<Map<String, Object>>>(){}.getType());

		
		
	    IVectorDatabaseEngine vectorDbEng = Utility.getVectorDatabase(vectorDbId);

	    
	    
	    List<Map<String, Object>> returnList = new ArrayList<Map<String, Object>>();

	    //change to toolList
	    for (Map<String, Object> toolData : toolList) {
	    	String query = "Tool: " + toolData.get("tool") + " \n Description: " + toolData.get("description");
	    	List<Map<String, Object>> results = vectorDbEng.nearestNeighbor(insight, query, 1, null);
	    	//TODO: examine output type. determine if it formats into map, or string.
	    	//if string, we can directly append? else gson convert then append, then deconvert
	    	
	    	Map<String, Object> returnMap = new HashMap<>();
	    	returnMap.put("ungroundedTool", toolData.get("tool"));
	    	returnMap.put("ungroundedDescription", toolData.get("description"));
	    	
	    	//TODO: add component which does more calculation to determine "this is a good response"
	    	//or "this is a bad response"
	    	//TODO: for now, grab the equivalent item from results. determine if map or string.
	    	//if string, convert first, then add.
	    	returnMap.put("groundedTool", null);
	    	returnMap.put("groundedDescription", null);
	    	
	    	
	    	returnList.add(returnMap);
	    	//parse results. add to return map
	    	//return will be
	    	//
	    	//List<Map<String, String>> listOfGroups;
	    	//each map is ungroundedTool, ungroundedExplanation, groundedTool, groundedDescription
	    }
	    
		
        return new NounMetadata(returnList, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	
	
	
	
	
	
	/**
	 * Alright, so as input, we receive certain things from the ungrounded reactor
	 * For right now, lets say I accept a list of tools.
	 * In the future, we can parse the raw input to grab the list of tools, and also
	 * append and throw in the whole plan, for context during rag
	 * 
	 * seemingly, we want to do a rag query for each recommended tool, and respond with,
	 * well, a tool to use, or a result "NAH".
	 * 
	 * For this, we iterate through the list. for every instance, call rag on the input and description,
	 * and try to match to some part of the, presumably... idk for now just treat it
	 * as a text file, with tool: description
	 */
}
