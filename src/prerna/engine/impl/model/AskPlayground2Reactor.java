package prerna.engine.impl.model;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * This reactor calls the specified model with a command, any optional context and tools to use, and returns the response message
 * This reactor will eventually replace the current contents of AskPlaygroundReactor
 */
public class AskPlayground2Reactor extends AbstractReactor {
	
  private static Logger logger = LogManager.getLogger(AskPlayground2Reactor.class);
	private static final Gson gson = new Gson();

  private String promptScript = null;
	private Map<String, Object> promptResult = null;
	private Map<String, Object> askArguments = null; // includes systemPrompt and userPrompt
	
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
				ReactorKeysEnum.PROJECT_TOOLS.getKey(),
        ReactorKeysEnum.VECTORDB.getKey(),
        ReactorKeysEnum.WORKSPACE_ID.getKey(),
		};
		this.keyRequired = new int[] { 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		User user = this.insight.getUser();
    String userId = user.getPrimaryLoginToken().getId();
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
		
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);
		MessageUtils.moveFilesToRoomFolder(inputImages, room, insight);
		List<String> roomFilePaths = MessageUtils.moveFilesToRoomFolder(inputFiles, room, insight);

    /**
     * Vector DB
     */

     List<Map<String, Object>> chunks = new ArrayList<>();

     try {
       String vectorId = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.VECTORDB.getKey()));

       String workspaceId = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey()));

       JsonObject options = null;
       String rawOptions = room.getOptions();
       if (rawOptions != null) {
         try {
           options = JsonParser.parseString(rawOptions).getAsJsonObject();
         } catch (Exception e) {
           logger.warn("Failed to parse room options for room with id " + roomId, e);
         }
       }

       if (workspaceId == null && options != null) {
         JsonElement workspaceElement = options.get("workspace");
         if (workspaceElement != null) {
           if (workspaceElement.isJsonPrimitive()) {
             workspaceId = workspaceElement.getAsString();
           } else if (workspaceElement.isJsonObject()) {
             JsonElement idElement = workspaceElement.getAsJsonObject().get("id");
             if (idElement.isJsonPrimitive()) {
               workspaceId = idElement.getAsString();
             }
           }
         }
       }

       Map<String, Object> workspace = null;
       if (workspaceId != null) {
         workspace = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
         if (workspace == null) {
           throw new IllegalArgumentException("Workspace not found");
         }
         String currentOwner = (String) workspace.get("owner");
         Boolean currentlyShared = (Boolean) workspace.get("sharing_enabled");
         boolean hasPermission = false;
         if (currentOwner != null) {
           for (AuthProvider provider : user.getLogins()) {
             if (currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
               hasPermission = true;
               break;
             }
           }
         }
         if (!hasPermission
             && (Boolean.TRUE != currentlyShared
                 || !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(workspaceId, user))) {
           throw new IllegalArgumentException("User unauthorized to use workspace for this query");
         }
       }

       String givenSystemPrompt = context;
       if (givenSystemPrompt == null && options != null) {
         JsonElement instructionsElement = options.get("instructions");
         if (instructionsElement != null && instructionsElement.isJsonPrimitive()) {
           givenSystemPrompt = instructionsElement.getAsString();
         }
       }
       if (givenSystemPrompt == null && workspace != null) {
         givenSystemPrompt = (String) workspace.get("system_prompt");
       }

       if (vectorId == null && workspace != null && AbstractSecurityUtils.containsEngineId(workspaceId)) {
         vectorId = workspaceId;
       }

       if (vectorId != null && !SecurityEngineUtils.userCanViewEngine(user, vectorId)) {
         throw new IllegalArgumentException(
             "Vector " + vectorId + " does not exist or user does not have access to this vector");
       }
       Map<String, Object> metadata = SecurityEngineUtils.getAggregateEngineMetadata(workspaceId,
           Arrays.asList("description"), true);
       String vectorDesc = StringUtils.trimToEmpty((String) metadata.get("description"));

       long charCount = 0;
       List<String> selectedFiles = null;
       if (vectorId != null && vectorId.equals(workspaceId)) {
         if (options != null) {
           try {
             JsonElement resourceFiltersElement = options.get("resourceFilters");
             if (resourceFiltersElement != null && resourceFiltersElement.isJsonObject()) {
               JsonElement entriesForVector = ((JsonObject) resourceFiltersElement).get(vectorId);
               if (entriesForVector != null && entriesForVector.isJsonArray()) {
                 JsonArray filterEntries = (JsonArray) entriesForVector;
                 if (filterEntries != null) {
                   Set<String> distinctFiles = new HashSet<>();
                   for (JsonElement e : (JsonArray) filterEntries) {
                     distinctFiles.add(e.getAsString());
                   }
                   selectedFiles = new ArrayList<>(distinctFiles);
                 }
               }
             }
           } catch (Exception e) {
             logger.warn("Failed to retrieve resource filters from room options for room with id " + roomId, e);
           }
         }

         // TODO: Add a helper to ModelInferenceLogsUtils to get filesData
         // Map<String, Object> selectedFilesData =
         // PlaygroundUtils.getInstance().getWorkspaceDocDataFromFilter(workspaceId,
         // selectedFiles);
         // charCount = (Long) selectedFilesData.get("persistent_char_count");
         // selectedFiles = (List<String>)
         // selectedFilesData.get("workspace_selected_files");
       }

       if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
         this.insight.getUser().getUserSymlinkHelper().symlinkFolder(room.getRoomFolderPath());
       }

       String vectorDocumentsPath = null;
       if (vectorId != null) {
         vectorDocumentsPath = Utility.getVectorDatabase(vectorId).getDocumentsFilesPath(null);
         Path documentsDir = Paths.get(vectorDocumentsPath);
         if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
           this.insight.getUser().getUserSymlinkHelper().symlinkFolder(documentsDir.toString());
         }
       }

       if (workspaceId != null) {
         promptScript = buildWorkspaceScript(userId, question, engineId, roomFilePaths, null, vectorId, vectorDesc,
             vectorDocumentsPath, givenSystemPrompt, selectedFiles, charCount);
       } else {
         promptScript = buildPromptScript(userId, question, engineId, roomFilePaths, null, vectorId, vectorDesc);
       }
       logger.info("AskElsa running: " + promptScript);
       promptResult = (Map<String, Object>) this.insight.getPyTranslator()
           .runScript(promptScript);

       String builtPrompt = (String) promptResult.getOrDefault("built_prompt", "");
       String userPrompt = (String) promptResult.getOrDefault("user_question", question);
       String systemPrompt = (String) promptResult.getOrDefault("system_prompt", context);
       chunks = (List<Map<String, Object>>) promptResult.remove("chunks");
       if (builtPrompt.isEmpty())
         builtPrompt = userPrompt;

     } catch (Exception e) {
       logger.warn("Rag failed with error: ", e.getMessage());
     }
		
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
        .withRAGChunks(chunks)
        .withEngineTools(getEngineToolIDs(), user)
        .withProjectTools(getProjectToolIDs(), user)
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

      public List<String> getEngineToolIDs() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.ENGINE_TOOLS.getKey());
        if (grs != null && !grs.isEmpty()) {
            int size = grs.size();
            for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
            return inputStrings;
        }
        int size = this.curRow.size();
        for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
        return inputStrings;
    }

    
      public List<String> getProjectToolIDs() {
        List<String> inputStrings = new ArrayList<>();
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PROJECT_TOOLS.getKey());
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

    private String buildWorkspaceScript(String userId, String question, String modelId, List<String> absFilePaths,
			List<String> absImagePaths, String vectorId, String vectorDescription, String vectorDocumentsPath, String systemPrompt, List<String> relSelectedPaths, long selectedCharCount) {
		
		String projectId = insight.getContextProjectId();
		if (projectId == null) {
			projectId = insight.getProjectId();
		}
		
		String appFolder = AssetUtility.getProjectAssetsFolder(projectId) + "/" + Constants.PY_BASE_FOLDER;
		appFolder = appFolder.replace("\\", "/");
		
		return StringUtils.joinWith("\n", 
			"from smssutil import load_module_from_file", 
			"if globals().get('workspace_prompt_builder') is None:", 
			"    global workspace_prompt_builder", 
			"    workspace_prompt_builder = load_module_from_file(module_name='workspace_prompt_builder', file_path='" + appFolder + "/chat_service/workspace_prompt_builder.py', search='" + appFolder + "/chat_service')", 
			"result = globals().get('workspace_prompt_builder').build_prompt(", 
			"    user_id = " + safePyString(userId, "None"),
			"    , user_prompt = " + safePyString(question, "None"),
			"    , model_id = " + safePyString(modelId, "None"),
			"    , byod_files = " + pyFileList(absFilePaths, "None"),
			"    , image_files = " + pyFileList(absImagePaths, "None"),
			"    , vector_id = " + safePyString(vectorId, "None"),
			"    , vector_descriptions = " + pyStringList(Arrays.asList(vectorDescription), "None"),
			"    , vector_documents_path = " + safePyString(vectorDocumentsPath, "None"),
			"    , workspace_system_prompt = " + safePyString(systemPrompt, "None"),
			"    , workspace_selected_files = " + pyFileList(relSelectedPaths, "None"),
			"    , persistent_char_count = " + Long.toString(selectedCharCount),
			")",
			"result"
		);
	}

  private String buildPromptScript(String userId, String question, String modelId, List<String> absFilePaths,
			List<String> absImagePaths, String vectorId, String vectorDescription) {
		
		String projectId = insight.getContextProjectId();
		if (projectId == null) {
			projectId = insight.getProjectId();
		}
		
		String appFolder = AssetUtility.getProjectAssetsFolder(projectId) + "/" + Constants.PY_BASE_FOLDER;
		appFolder = appFolder.replace("\\", "/");
		
		return StringUtils.joinWith("\n", 
			"from smssutil import load_module_from_file", 
			"if globals().get('prompt_builder') is None:", 
			"    global prompt_builder", 
			"    prompt_builder = load_module_from_file(module_name='prompt_builder', file_path='" + appFolder + "/chat_service/prompt_builder.py', search='" + appFolder + "/chat_service')", 
			"result = globals().get('prompt_builder').build_prompt(", 
			"    user_id = " + safePyString(userId, "False"),
			"    , user_prompt = " + safePyString(question, "False"),
			"    , model_id = " + safePyString(modelId, "False"),
			"    , byod_files = " + pyDictList(absFilePaths),
			"    , image_files = " + pyDictList(absImagePaths),
			"    , vector_id = " + safePyString(vectorId, "False"),
			"    , vector_description = " + safePyString(vectorDescription, "False"),
			")",
			"result"
		);
	}

  /** JSON-escape for Python string arguments; returns None if null/empty. */
	private static String safePyString(String s, String defaultValue) {
		if (s == null || s.trim().isEmpty())
			return defaultValue;
		return gson.toJson(s);
	}

  /** JSON-escape for lists of string arguments; returns None if null/empty. */
	private static String pyStringList(List<String> list, String defaultValue) {
		if (list == null || list.isEmpty())
			return defaultValue;
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < list.size(); i++) {
			sb.append(gson.toJson(list.get(i)));
			if (i < list.size() - 1)
				sb.append(", ");
		}
		sb.append("]");
		return sb.toString();
	}

	private static String pyFileList(List<String> absPaths, String defaultValue) {
		if (absPaths == null || absPaths.isEmpty())
			return defaultValue;
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < absPaths.size(); i++) {
			sb.append(gson.toJson(absPaths.get(i).replace("\\", "/").replace("'", "\\'")));
			if (i < absPaths.size() - 1)
				sb.append(", ");
		}
		sb.append("]");
		return sb.toString();
	}

	private static String pyDictList(List<String> absPaths) {
        if (absPaths == null || absPaths.isEmpty()) return "False";
        List<Map<String, String>> dicts = fileListToDict(absPaths);
        return gson.toJson(dicts);
    }
	private static List<Map<String, String>> fileListToDict(List<String> paths) {
		List<Map<String, String>> out = new ArrayList<>();
		for (String path : paths) {
			String fileType = "";
			int idx = path.lastIndexOf('.');
			if (idx > -1 && idx < path.length() - 1) {
				fileType = path.substring(idx + 1).toLowerCase();
			}
			out.add(makeFileDict(path, fileType));
		}
		return out;
	}
	private static Map<String, String> makeFileDict(String fileKey, String fileType) {
		Map<String, String> map = new HashMap<>();
		map.put("fileKey", fileKey);
		map.put("fileType", fileType);
		return map;
	}

}
