package prerna.reactor.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AskPlaygroundReactor extends AbstractReactor {

	private static final Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	public AskPlaygroundReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.CONTEXT.getKey(),
				ReactorKeysEnum.IMAGE.getKey(),
				ReactorKeysEnum.URL.getKey(),
				"mcpToolID",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
		};
		this.keyRequired = new int[] { 1, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		////// SET UP //////////
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		User user = this.insight.getUser();
		if (user == null) throw new IllegalArgumentException("You are not properly logged in");
		String userId = user.getPrimaryLoginToken().getId();

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}

		String question = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.COMMAND.getKey()));
		String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
		if (context != null) context = Utility.decodeURIComponent(context);

		Map<String, Object> paramMap = getParamMap();
		if (paramMap == null) paramMap = new HashMap<>();

		List<String> inputImages = getImages();
		List<String> inputImageURLs = getImageURLs();
		List<String> mcpToolIDs = getMCPToolIDs();
		List<Map<String, Object>> tools = new ArrayList<Map<String, Object>>();
		if(mcpToolIDs != null && !mcpToolIDs.isEmpty()) {
			for (String appId : mcpToolIDs) {
				List<Map<String, Object>> toolJsons = getToolJson(appId);
				if (toolJsons != null) {
					tools.addAll(toolJsons);
				}
			}
		}

		IModelEngine modelEngine = Utility.getModel(engineId);

		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);
		List<String> copiedImages = MessageUtils.copyFilesToRoomFolder(inputImages, room, insight);

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

		// parse the response for code blocks
		if(response.getMessageType() == MessageType.RESPONSE_TEXT) {
			response = MessageUtils.processMarkdownCodeBlocks(response, modelEngine, room);
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(), insight.getUser().getPrimaryLoginToken().getId(),
					room.getMessagesAsString());
		} else if(response.getMessageType() == MessageType.RESPONSE_TOOL) {
			Map<String, JSONObject> mcpToolsJsonCache = new HashMap<>();
			List<Map<String, Object>> toolResponses = response.getToolResponses();
			for(int toolResponseIndex = 0; toolResponseIndex < toolResponses.size(); toolResponseIndex++) {
				Map<String, Object> responseToolMap = toolResponses.get(toolResponseIndex);
				// we start the function name with _projectid_ so lets remove that
				String responseProjectIdToolFunctionName = (String) responseToolMap.get("name");
				String[] responseProjectIdToolFunctionNameSplit = responseProjectIdToolFunctionName.substring(1).split("_", 2);
				String projectId = responseProjectIdToolFunctionNameSplit[0];
				String origFunctionName = responseProjectIdToolFunctionNameSplit[1];
				
				// now that we have the projectId
				// lets append some of the mcp metadata back into the response
				
				JSONObject mcpToolsJson = mcpToolsJsonCache.get(projectId);
				if(mcpToolsJson == null) {
					IProject project = Utility.getProject(projectId);
					mcpToolsJson = MCPUtility.getAggregatedTools(project);
					mcpToolsJsonCache.put(projectId, mcpToolsJson);
				}
				
				if(mcpToolsJson != null) {
					JSONArray mcpToolsArray = mcpToolsJson.getJSONArray("tools");
					JSONObject mcpTool = null;
					PROJECT_MCP_LOOP : for(int toolIndex = 0; toolIndex < mcpToolsArray.length(); toolIndex++) {
						JSONObject _tool = mcpToolsArray.getJSONObject(toolIndex);
						if(_tool.has("name") && _tool.getString("name").equals(origFunctionName)) {
							mcpTool = _tool;
							break PROJECT_MCP_LOOP;
						}
					}
					
					// add back the title from mcp structure
					if(mcpTool != null && mcpTool.has("title")) {
						responseToolMap.put("title", mcpTool.getString("title"));
					}
					
					if(mcpToolsJson.has("_meta")) {
						responseToolMap.put("_meta", mcpToolsJson.get("_meta"));
					}
				}
			}
		}

		// ---- Return both messages as a Map
		Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(msg)));
		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/**
	 * 
	 * @param appId
	 * @return
	 */
	private List<Map<String, Object>> getToolJson(String appId) {
		IProject project = Utility.getProject(appId);
		JSONObject toolMap = MCPUtility.getAggregatedTools(project);
		JSONObject updatedToolMap = MCPUtility.appendProjectIdToTooslMethodName(appId, toolMap);
		if(updatedToolMap != null && updatedToolMap.has("tools")) {
			JSONArray arr = updatedToolMap.getJSONArray("tools");
			List<Map<String, Object>> result = new ArrayList<>();
			for (int i = 0; i < arr.length(); i++) {
				JSONObject toolObj = arr.getJSONObject(i);
				Map<String, Object> map = toolObj.toMap();
				result.add(map);
			}
			return result;
		}

		// Fallback: always return an empty list if nothing found
		return Collections.emptyList();
	}

	// ------- image/file helpers, paramMap etc. ---------------
	public List<String> getImages() {
		List<String> inputStrings = new ArrayList<>();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[4]);
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
		GenRowStruct grs = this.store.getNoun(this.keysToGet[5]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
			return inputStrings;
		}
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
		return inputStrings;
	}

	public List<String> getMCPToolIDs() {
		List<String> inputStrings = new ArrayList<>();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[6]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
			return inputStrings;
		}
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
		return inputStrings;
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

	@Override
	public String getReactorDescription() {
		return "This method is used to run an LLM text-generation call (Playground)—returns both input and response message objects.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "This is the prompt to execute against the LLM";
		} else if(key.equals(ReactorKeysEnum.CONTEXT.getKey())) {
			return "The system prompt to use for the LLM call";
		} else if(key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "This is the room ID that will be used for storing messages. If no room id is passed in, then insight id will be used for the room";
		} else if(key.equals(ReactorKeysEnum.IMAGE.getKey())) {
			return "This is  an array of image file names that have already been uploaded to the insight folder.";
		} else if(key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Map containing the key-value pairs for model parameters like 'temperature', 'top_p', etc. "
					+ "In addition, you can pass in 'full_prompt' to represent a full prompt and history via ChatML format which will ignore inputs for " +
					Arrays.asList(ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.USE_HISTORY.getKey());
		}
		return super.getDescriptionForKey(key);
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