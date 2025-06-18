package prerna.playground.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.ArrayList;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse.ToolResponse;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.NotebookExecution;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import prerna.util.AssetUtility;
import prerna.util.Constants;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AskRoomPromptReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AskRoomPromptReactor.class);

	private static final Pattern MARKDOWN_CODE_PATTERN = Pattern.compile("```" + // Opening backticks
			"(?:([a-zA-Z0-9]+))?" + // Language (optional, group 1)
			"(?:" + // Non-capturing group for title alternatives
			"\\s+title=\"([^\"]+)\"" + // Either title="filename" (group 2)
			"|\\s+([^\\s\\n]+)" + // Or direct filename (group 3)
			")?" + // Title is optional
			"\\s*\\n" + // Whitespace and mandatory newline
			"(.*?)" + // Code content (group 4)
			"```", // Closing backticks
			Pattern.DOTALL);

	private static final String CHAIN_OF_THOUGHT_PROMPT = "You are an AI assistant that always reasons step by step before answering."
			+ " You break down each problem logically, show your thought process clearly, and only then give the final answer.";

	public AskRoomPromptReactor() {
		this.keysToGet = new String[] { "roomId", "modelId", "question", "context",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), "engine_tools", "project_tools", "execute_tool",
				"chain_of_thought" };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = this.keyValue.get(this.keysToGet[0]);
		String modelId = this.keyValue.get(this.keysToGet[1]);
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}

		// check if the user has access and the room is active
		List<Map<String, Object>> roomActiveOutput =  ModelInferenceLogsUtils.getUserActiveRooms(roomId, user.getPrimaryLoginToken().getId());

		// if there are no rooms or more than one returned, throw an error
		if (roomActiveOutput.size() != 1) {
			throw new IllegalArgumentException("Unable to find room");
		}

		// if it isn't active, throw an error
		if (roomActiveOutput.get(0).get("IS_ACTIVE").equals(false)) {
			throw new IllegalArgumentException("Room is closed");
		}

		String question = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[2]));
		String context = this.keyValue.get(this.keysToGet[3]);
		if (context != null) {
			context = Utility.decodeURIComponent(context);
		}

		Map<String, Object> paramMap = getMap();
		IModelEngine modelEngine = Utility.getModel(modelId);
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		List<String> engineToolIDs = getEngineToolIDs();
		List<String> projectToolIDs = getProjectToolIDs();

		boolean executeTool = false;
		if (this.keyValue.get("execute_tool") != null) {
			executeTool = Boolean.parseBoolean(this.keyValue.get("execute_tool"));
		}

		boolean chainOfThought = false;
		if (this.keyValue.get("chain_of_thought") != null) {
			chainOfThought = Boolean.parseBoolean(this.keyValue.get("chain_of_thought"));
		}

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
				// TODO add a safety check here for function engines only
				IFunctionEngine function = Utility.getFunctionEngine(engineToolID);
				Map<String, Object> functionToolMap = function.buildFunctionEngineToolMap();
				toolsList.add(functionToolMap);
			}

			// Iterate over each project ID and add the tool to the tools list
			for (String projectToolID : projectToolIDs) {
				// TODO add a safety check here for code projects only
				IProject project = Utility.getProject(projectToolID);
				Map<String, Object> projectToolMap = project.buildProjectToolMap();
				toolsList.add(projectToolMap);
			}
		}

		if (chainOfThought) {
			String insightId = this.insight.getInsightId();
			String task = question;
			String num_steps = "3";
			if (num_steps == null) {
				num_steps = "at your discretion";
			}

			String projectId = this.insight.getContextProjectId();
			String appFolder = AssetUtility.getProjectAssetFolder(projectId);
			String path = appFolder + File.separator + Constants.PY_BASE_FOLDER + File.separator;
			path = path.replace("\\", "/");

			String commands = "import sys\n" + "sys.path.append(\"" + path + "\")\n"
					+ "from PyDecomposeTask import decompose_task\n" + "decompose_task(\"" + modelId + "\", \""
					+ insightId + "\", \"" + task + "\", \"" + num_steps + "\")\n";

			// Set Python interfacing Class
			PyTranslator pt = this.insight.getPyTranslator();

			List<Map<String, Object>> pyResponse = (List<Map<String, Object>>) pt.runSmssWrapperEval(commands,
					this.insight);

			Map<String, Object> output = new HashMap<String, Object>();
			output.put("response", new ArrayList<Map<String, Object>>());

			for (Map<String, Object> taskStep : pyResponse) {
				Map<String, Object> outputObject = new HashMap<String, Object>();
				outputObject.put("type", "CONTENT");
				outputObject.put("content", "**" + taskStep.get("name") + "**");
				((ArrayList<Map<String, Object>>) output.get("response")).add(outputObject);

				AskModelEngineResponse modelResponse = modelEngine.ask(question,
						"Given the question and the list of tools, give me the tool call most appropriate. Do not return the tool call stringified.",
						this.insight, paramMap);

				Map<String, Object> toolObject = processModelResponse(modelResponse, modelEngine);

				((ArrayList<Map<String, Object>>) output.get("response"))
						.add(((ArrayList<Map<String, Object>>) toolObject.get("response")).get(0));
			}

			return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
		}

		AskModelEngineResponse modelResponse = modelEngine.ask(question, context, this.insight, paramMap);

		if (modelResponse.getMessageType().equalsIgnoreCase(AskModelEngineResponse.TOOL) && executeTool) {
			modelResponse = executeToolAndSendToModel((AskToolModelEngineResponse) modelResponse, modelEngine);
		}
//
//		// record the output
//		String messageId = PlaygroundUtils.getInstance().recordRoomMessage(UUID.randomUUID().toString(), roomId,
//				modelId, paramMap, context, question, modelResponse.toMap(), user.getPrimaryLoginToken().getId());

		Map<String, Object> output = processModelResponse(modelResponse, modelEngine);

		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private Map<String, Object> processModelResponse(AskModelEngineResponse modelResponse, IModelEngine modelEngine) {
		Map<String, Object> output = new HashMap<String, Object>();
		output.put("response", new ArrayList<Map<String, Object>>());
		if (modelResponse.getMessageType().equalsIgnoreCase(AskModelEngineResponse.TOOL)) {

			// the response is for a tool call
			AskToolModelEngineResponse toolResponse = (AskToolModelEngineResponse) modelResponse;

			List<ToolResponse> tools = toolResponse.getTools();

			for (ToolResponse tool : tools) {

				Map<String, Object> toolArguments = tool.getArguments();

				String toolName = null;
				String toolType = null;

				if (tool.getName().equals("project_engine")) {
					IProject project = Utility.getProject((String) toolArguments.get("id"));
					toolName = project.getProjectName();
					toolType = "PROJECT";
				} else if (tool.getType().equals("function")) {
					IFunctionEngine function = Utility.getFunctionEngine((String) toolArguments.get("id"));
					toolName = function.getEngineName();
					toolType = "FUNCTION";
				} else {
					classLogger.info("Tool " + tool.getName() + " is not enabled on AskRook");
					continue;
				}

				// Array list of maps to store params needed to call the tools
				List<HashMap<String, Object>> toolCallInfoData = new ArrayList<HashMap<String, Object>>();

				Map<String, Object> singleToolMap = new HashMap<String, Object>();
				singleToolMap.put("type", toolType);
				singleToolMap.put("name", toolName);
				singleToolMap.put("id", (String) toolArguments.get("id"));
				for (Entry<String, Object> functionParam : ((Map<String, Object>) toolArguments.get("map"))
						.entrySet()) {
					HashMap<String, Object> paramInfo = new HashMap<String, Object>();
					paramInfo.put("name", functionParam.getKey());
					paramInfo.put("type", functionParam.getValue().getClass().getSimpleName());
					paramInfo.put("value", functionParam.getValue());
					toolCallInfoData.add(paramInfo);
				}
				singleToolMap.put("parameters", toolCallInfoData);
				singleToolMap.put("tool_id", tool.getId());
				singleToolMap.put("tool_name", tool.getName());
				((ArrayList<Map<String, Object>>) output.get("response")).add(singleToolMap);
			}
//            // this is just doing tools.getArguments
//            ObjectMapper mapper = new ObjectMapper();
//            Map<String, Object> functionParams = new HashMap<String, Object>();
//            try {
//                functionParams = mapper.readValue(toolArguments, Map.class);
//            } catch (Exception e) {
//                // Handle parsing error
//                functionParams = null;
//            }
//
//            Map<String, Object> outputObject = new HashMap<String, Object>();
//            String toolName;
//            String toolType;
//            
//            if(toolResponse.getResponse().get("name").equals("project_engine")){
//                IProject project = Utility.getProject((String) functionParams.get("id"));
//                toolName = project.getProjectName();
//                toolType = "PROJECT";
//            } else {
//                IFunctionEngine function = Utility.getFunctionEngine((String) functionParams.get("id"));
//                toolName = function.getEngineName();
//                toolType = "FUNCTION";
//            }
//            
//            // object to store params needed to call the tool
//            List<HashMap<String, Object>> toolCallInfoData = new ArrayList<HashMap<String, Object>>();
//            for(Entry<String, Object> functionParam : ((Map<String, Object>)functionParams.get("map")).entrySet()){
//                HashMap<String, Object> paramInfo = new HashMap<String, Object>();
//                paramInfo.put("name", functionParam.getKey());
//                paramInfo.put("type", functionParam.getValue().getClass().getSimpleName());
//                paramInfo.put("value", functionParam.getValue());
//                toolCallInfoData.add(paramInfo);
//            }
//
//            outputObject.put("type", toolType);
//            outputObject.put("name", toolName);
//            outputObject.put("id", (String) functionParams.get("id"));
//            outputObject.put("parameters", toolCallInfoData);
//            outputObject.put("tool_id", toolResponse.getToolCallId());
//            outputObject.put("tool_name", toolResponse.getToolCallName());
//            ((ArrayList<Map<String, Object>>) output.get("response")).add(outputObject);
		} else {
			// this is a standard response - process it for code blocks.

			// Process the response to extract code blocks and replace with UUID references
			ProcessedResponse processedResponse = processMarkdownCodeBlocks(modelResponse.getStringResponse(),
					modelEngine);

			// Add code blocks to output if any exist
			if (!processedResponse.getCodeBlocks().isEmpty()) {
				Pattern pattern = Pattern.compile("<CODEBLOCK>(.*)<\\/CODEBLOCK>");
				Matcher matcher = pattern.matcher(processedResponse.getModifiedResponse());
				Map<String, Object> outputObject = new HashMap<String, Object>();

				int lastIndex = 0;
				while (matcher.find()) {
					Map<String, Object> textObject = new HashMap<String, Object>();
					String codeBlockId = matcher.group(1);
					int start = matcher.start();

					String textChunk = processedResponse.getModifiedResponse().substring(lastIndex, start).trim();
					if (!textChunk.isEmpty()) {
						textObject.put("type", "CONTENT");
						textObject.put("content", textChunk);
						((ArrayList<Map<String, Object>>) output.get("response")).add(textObject);
					}

					lastIndex = matcher.end();

					String nextChunk = "";
					if (matcher.find()) {
						nextChunk = processedResponse.getModifiedResponse().substring(matcher.start(), matcher.start())
								.trim();
						matcher.region(matcher.start(), processedResponse.getModifiedResponse().length());
					} else {
						nextChunk = processedResponse.getModifiedResponse().substring(lastIndex).trim();
					}

					Map<String, Object> nextTextObject = new HashMap<String, Object>();
					nextTextObject.put("type", "CONTENT");
					nextTextObject.put("content", nextChunk);
					((ArrayList<Map<String, Object>>) output.get("response")).add(nextTextObject);

					CodeBlock codeBlock = (CodeBlock) processedResponse.getCodeBlocks().get(codeBlockId);
					HashMap<String, Object> codeBlockInfo = new HashMap<String, Object>();
					codeBlockInfo.put("type", "CODE");
					codeBlockInfo.put("language", codeBlock.getLanguage());
					codeBlockInfo.put("name", codeBlock.getTitle());
					codeBlockInfo.put("content", codeBlock.getCode());
					((ArrayList<Map<String, Object>>) output.get("response")).add(codeBlockInfo);
				}

				outputObject.put("originalResponse", modelResponse.getStringResponse());
				((ArrayList<Map<String, Object>>) output.get("response")).add(outputObject);
			} else {
				Map<String, Object> outputObject = new HashMap<String, Object>();
				outputObject.put("type", "CONTENT");
				outputObject.put("content", modelResponse.getStringResponse());
				((ArrayList<Map<String, Object>>) output.get("response")).add(outputObject);
			}
		}
		output.put("messageId", modelResponse.getMessageId());
		return output;
	}

	private AskModelEngineResponse executeToolAndSendToModel(AskToolModelEngineResponse toolResponse,
			IModelEngine modelEngine) {

		// will execute just the first tool for now.

		// tool result will be a custom element in the paramMap
		HashMap<String, String> toolExecutionMap = new HashMap<String, String>();
		toolExecutionMap.put(AbstractModelEngine.ROLE, "tool");
		toolExecutionMap.put("tool_call_id", toolResponse.getToolCallId());
		toolExecutionMap.put("name", toolResponse.getToolCallName());

		// {"function_id":"123-3345-567","map":{"lat":"123","lon":"321"}}
		String toolArguments = toolResponse.getToolCallArgumentsAsString();

		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> toolParams = new HashMap<String, Object>();
		try {
			toolParams = mapper.readValue(toolArguments, Map.class);
		} catch (Exception e) {
			// Handle parsing error
			toolParams = null;
		}

		Object toolExecutionReturn;
		// if it is a project, execute that:
		if (toolResponse.getToolCallName().equals("project_engine")) {
			IProject project = Utility.getProject((String) toolParams.get("id"));
			NotebookExecution notebookExec = project.executeNotebooks(this.insight,
					(Map<String, String>) toolParams.get("map"));
			toolExecutionReturn = notebookExec.getRunner().getResults();
		}
		// if it is a function, execute that:
		else {
			IFunctionEngine function = Utility.getFunctionEngine((String) toolParams.get("id"));
			toolExecutionReturn = function.execute((Map<String, Object>) toolParams.get("map"));
		}
		String toolReturnString = null;

		try {
			toolReturnString = mapper.writeValueAsString(toolExecutionReturn);
		} catch (com.fasterxml.jackson.core.JsonProcessingException e) {
			// Handle the exception, maybe log it or return a default value
			e.printStackTrace();
			toolReturnString = "{}";
		}

		HashMap<String, Object> paramMap = new HashMap<String, Object>();
		toolExecutionMap.put("content", toolReturnString);
		paramMap.put("toolExecution", toolExecutionMap);
		AskModelEngineResponse toolExecutionResponse = modelEngine.ask("", null, this.insight, paramMap);
		return toolExecutionResponse;
	}

	// Method to parse markdown code blocks
	private ProcessedResponse processMarkdownCodeBlocks(String response, IModelEngine modelEngine) {
		Map<String, CodeBlock> codeBlocks = new HashMap<>();
		Matcher matcher = MARKDOWN_CODE_PATTERN.matcher(response);
		StringBuffer modifiedResponse = new StringBuffer();

		while (matcher.find()) {
			String language = matcher.group(1) != null ? matcher.group(1).trim() : "";
			// Check both title formats and use the first non-null one
			String title = matcher.group(2) != null ? matcher.group(2).trim()
					: matcher.group(3) != null ? matcher.group(3).trim() : "";
			String code = matcher.group(4).trim();

			String uuid = UUID.randomUUID().toString();

			if (title == "") {
				HashMap<String, Object> paramMap = new HashMap<String, Object>();
				AskModelEngineResponse modelResponse = modelEngine.ask(
						"Given the following code block, give it a title: " + code + " Just give me the title", null,
						this.insight, paramMap);
				title = modelResponse.getStringResponse();
			}

			codeBlocks.put(uuid, new CodeBlock(language, code, title));

			matcher.appendReplacement(modifiedResponse,
					Matcher.quoteReplacement("<CODEBLOCK>" + uuid + "</CODEBLOCK>"));
		}
		matcher.appendTail(modifiedResponse);

		return new ProcessedResponse(modifiedResponse.toString(), codeBlocks);
	}

	/**
	 * 
	 * @return list of engines
	 */
	public List<String> getEngineToolIDs() {
		List<String> inputStrings = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[5]);
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
	 * 
	 * @return list of project IDs
	 */
	public List<String> getProjectToolIDs() {
		List<String> inputStrings = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[6]);
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
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getNoun(keysToGet[4]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	// Helper class to represent the processed response
	private static class ProcessedResponse {
		private final String modifiedResponse;
		private final Map<String, CodeBlock> codeBlocks;

		public ProcessedResponse(String modifiedResponse, Map<String, CodeBlock> codeBlocks) {
			this.modifiedResponse = modifiedResponse;
			this.codeBlocks = codeBlocks;
		}

		public String getModifiedResponse() {
			return modifiedResponse;
		}

		public Map<String, CodeBlock> getCodeBlocks() {
			return codeBlocks;
		}
	}

	// Class to represent a code block
	private static class CodeBlock {
		private final String language;
		private final String code;
		private final String title;

		public CodeBlock(String language, String code, String title) {
			this.language = language;
			this.code = code;
			this.title = title;
		}

		public String getLanguage() {
			return language;
		}

		public String getCode() {
			return code;
		}

		public String getTitle() {
			return title;
		}
	}
}