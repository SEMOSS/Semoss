package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * This reactor calls the specified model with a command, any optional context and tools to use, and returns the response message
 * This reactor will eventually replace the current contents of AskPlaygroundReactor
 */
public class AskPlayground2Reactor extends AbstractReactor {
	
	private static final Gson gson = new Gson();
	
	public AskPlayground2Reactor() {
		this.keysToGet = new String[] { 
				ReactorKeysEnum.ENGINE.getKey(), 
				ReactorKeysEnum.ROOM_ID.getKey(), 
				ReactorKeysEnum.COMMAND.getKey(), 
				ReactorKeysEnum.CONTEXT.getKey(), 
				ReactorKeysEnum.USE_HISTORY.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.IMAGE.getKey(),
				ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.ENGINE_TOOLS.getKey(),
				ReactorKeysEnum.PROJECT_TOOLS.getKey()
		};
//		TODO: add this back when needed to test
//		this.keyRequired = new int[] { 1, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		User user = this.insight.getUser();
		AccessToken userToken = user.getPrimaryLoginToken();
		
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		String question = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.COMMAND.getKey()));
		String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
		if (context != null) {
			context = Utility.decodeURIComponent(context);
		}
		
		Map<String, Object> paramMap = getParamMap();
		IModelEngine modelEngine = Utility.getModel(engineId);
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		Boolean useHistoryParam = Boolean.parseBoolean(this.keyValue.getOrDefault(ReactorKeysEnum.USE_HISTORY.getKey(), "true")+"");
		paramMap.put("use_history", useHistoryParam);
		
        List<String> inputImages = getImages();
        List<String> inputImageURLs = getImageURLs();
        
//        TODO: Gonna need to move the files to a room if exists
        List<String> inputFiles = getFiles();
        
        addToolsToParamMap(paramMap);
		
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);
		MessageUtils.moveFilesToRoomFolder(inputImages, room, insight);
		List<String> roomFilePaths = MessageUtils.moveFilesToRoomFolder(inputFiles, room, insight);
		
	///// MESSAGE CREATION //////////

//		Need to make sure we are adding files to the message (if applicable/necessary) using withRagChunks
        InputMessage msg;
        msg = InputMessage.builder(room)
        .withInputUIPrompt(question)
        .withInputPrompt(question)
        .withModelType(modelEngine.getModelType())
        .withParamMap(paramMap)
        .withImages(inputImages, room)
        .withImageUrls(inputImageURLs)
        .build();
        
        /**
         * Send message and incorporate tools if necessary
         */
        
        ResponseMessage response = room.ask(msg, context, modelEngine);
        
        // ---- Return both messages as a Map
        Map<String, Object> pixelReturn = new LinkedHashMap<>();

         pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(msg)));
         pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));

        return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private void addToolsToParamMap(Map<String, Object> paramMap) {
		List<String> engineToolIDs = getEngineToolIDs();
        List<String> projectToolIDs = getProjectToolIDs();
        
        if (!engineToolIDs.isEmpty() || !projectToolIDs.isEmpty()) {

            // Check if the "tools_choice" key exists in the paramMap, else add it
            if (!paramMap.containsKey("tool_choice")) {
              paramMap.put("tool_choice", "auto");
            }

            // Check if the "tools" key exists in the paramMap
            List<Map<String, Object>> toolsList;
            if (paramMap.containsKey("tools")) {
              // Retrieve the existing list of tools
              toolsList = (List<Map<String, Object>>) paramMap.get("tools");
            } else {
              // Create a new list for tools
              toolsList = new ArrayList<Map<String, Object>>();
              paramMap.put("tools", toolsList);
            }

            // Iterate over each engine ID and add the function tool to the tools list
            for (String engineToolID : engineToolIDs) {
              IFunctionEngine function = Utility.getFunctionEngine(engineToolID);
              assert function.getCatalogType() == IFunctionEngine.CATALOG_TYPE.FUNCTION;
              Map<String, Object> functionToolMap = function.buildFunctionEngineToolMap();
              toolsList.add(functionToolMap);
            }

            // Iterate over each project ID and add the tool to the tools list
            for (String projectToolID : projectToolIDs) {
              IProject project = Utility.getProject(projectToolID);
              assert project.getProjectType() == IProject.PROJECT_TYPE.CODE;
              Map<String, Object> projectToolMap = project.buildOpenAIFunctionEngineToolMap();
              toolsList.add(projectToolMap);
            }
          }
	}
	
    public List<String> getImages() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.IMAGE.getKey());
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
    }
    
    public List<String> getFiles() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FILE_PATH.getKey());
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
    }
    
    public List<String> getImageURLs() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.URL.getKey());
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
    }
    
    @SuppressWarnings("unchecked")
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
    
    @Override
	public String getReactorDescription() {
//    	TODO: fill this in
		return "TODO";
	}
    
    /**
     * TODO: See @LLM2Reactor for how to do this
     */
	@Override
	protected String getDescriptionForKey(String key) {
		return "";
	}
	
	 /** @return list of engines */
	  public List<String> getEngineToolIDs() {
	    List<String> inputStrings = new ArrayList<>();

	    // see if added as key
	    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.ENGINE_TOOLS.getKey());
	    if (grs != null && !grs.isEmpty()) {
	      int size = grs.size();
	      for (int i = 0; i < size; i++) {
	        inputStrings.add(grs.get(i).toString());
	      }
	      return inputStrings;
	    }

	    // no key is added, grab all inputs
	    int size = this.curRow.size();
	    for (int i = 0; i < size; i++) {
	      inputStrings.add(this.curRow.get(i).toString());
	    }

	    return inputStrings;
	  }

	  /** @return list of project IDs */
	  public List<String> getProjectToolIDs() {
	    List<String> inputStrings = new ArrayList<>();

	    // see if added as key
	    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PROJECT_TOOLS.getKey());
	    if (grs != null && !grs.isEmpty()) {
	      int size = grs.size();
	      for (int i = 0; i < size; i++) {
	        inputStrings.add(grs.get(i).toString());
	      }
	      return inputStrings;
	    }

	    // no key is added, grab all inputs
	    int size = this.curRow.size();
	    for (int i = 0; i < size; i++) {
	      inputStrings.add(this.curRow.get(i).toString());
	    }

	    return inputStrings;
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
