package prerna.reactor.agent.mcp;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.ReactorFactory;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.reactor.storage.DeleteFromStorageReactor;
import prerna.reactor.storage.ListStoragePathDetailsReactor;
import prerna.reactor.storage.ListStoragePathReactor;
import prerna.reactor.storage.PullFromStorageReactor;
import prerna.reactor.storage.PushToStorageReactor;
import prerna.reactor.function.ExecuteFunctionEngineReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class MakeEngineMCPReactor extends AbstractReactor {
	
	private static String instructContext = "You are a helpful SEMOSS backend agent that helps do some behind-the-scenes processing for the system.";
	private static String question = "Your specific task is to take json that represents some metadata about the engine, and see if you can improve upon it in any way. Do not omit any critical details or necessary information. Do improve any/all descriptions as necessary. You are required to use the attached JSON Schema in your response";

	private static final Logger classLogger = LogManager.getLogger(MakeEngineMCPReactor.class);

	private static final Map<IEngine.CATALOG_TYPE, List<Class<? extends IReactor>>> STANDARD_ENGINE_TOOLS = new HashMap<>() {
		{
		// @formatter:off
        put(IEngine.CATALOG_TYPE.STORAGE, new ArrayList<>(Arrays.asList(
            ListStoragePathReactor.class,
            ListStoragePathDetailsReactor.class,
            PullFromStorageReactor.class,
            PushToStorageReactor.class,
            DeleteFromStorageReactor.class
        )));
        // @formatter:on
		}
		
		{
		// @formatter:off
        put(IEngine.CATALOG_TYPE.FUNCTION, new ArrayList<>(Arrays.asList(
        	ExecuteFunctionEngineReactor.class
        )));
        // @formatter:on
		}
	};

	public MakeEngineMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.REACTOR.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.MODEL.getKey(), ReactorKeysEnum.MCP_EXECUTION.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String modelId = this.keyValue.get(ReactorKeysEnum.MODEL.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit.");
		}

		IEngine engine = Utility.getEngine(engineId);
		IEngine.CATALOG_TYPE engineCatalogType = engine.getCatalogType();
		String engineName = engine.getEngineName();

		String engineAssetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engineCatalogType, engineId, engineName);
		engineAssetsFolder = engineAssetsFolder.replace("\\", "/");

		JSONObject mcpJson = new JSONObject();
		JSONArray toolsArray = new JSONArray();

		List<String> reactorNames = getNounAsStringList(ReactorKeysEnum.REACTOR.getKey());

		if (reactorNames == null || reactorNames.isEmpty()) {
			List<Class<? extends IReactor>> reactors = STANDARD_ENGINE_TOOLS.getOrDefault(engineCatalogType, new ArrayList<>());
			int numReactors = reactors.size();
			List<String> resolvedExecModes = new ArrayList<>(numReactors);
			List<String> mcpExecutionList = getNounAsStringList(ReactorKeysEnum.MCP_EXECUTION.getKey());
			for (int i = 0; i < numReactors; i++) {
				String execModeInput = (mcpExecutionList != null && i < mcpExecutionList.size())
						? mcpExecutionList.get(i)
						: null;
				MCPExecution execModeEnum = MCPExecution.fromValue(execModeInput);

				String execModeStr;
				if (execModeInput == null || execModeEnum == null) {
					execModeStr = MCPExecution.ASK.getValue();
					// Only log if there actually was user input;
					if (execModeInput != null) {
						classLogger.warn("Invalid mcpExecution value '{}' for reactor '{}'; falling back to 'ask'.",
								execModeInput, reactorNames.get(i));
					}
				} else {
					execModeStr = execModeEnum.getValue();
				}
				resolvedExecModes.add(execModeStr);
			}

			for (int i = 0; i < reactors.size(); i++) {
				Class<? extends IReactor> reactorClass = reactors.get(i);
				try {
					IReactor thisReactor = reactorClass.getConstructor().newInstance();
					JSONObject reactorTool = thisReactor.asMcpTool();
					JSONObject inputSchema = reactorTool.getJSONObject("inputSchema");
					JSONObject properties = inputSchema.getJSONObject("properties");
					String engineCatalogTypeLower = engineCatalogType.name().toLowerCase();
					String paramName = Arrays.asList(((AbstractReactor) thisReactor).keysToGet).contains(engineCatalogTypeLower)
							? engineCatalogTypeLower
							: "engine";
					JSONObject engineObj = properties.getJSONObject(paramName);
					engineObj.put("enum", new JSONArray().put(engineId));
					JSONObject engineMeta = engine.getEngineMetadata();
					engineObj.put("engineMetadata", (engineMeta == null || engineMeta.isEmpty()) ? null : improveEngineMeta(engineMeta, modelId));

					String execMode = resolvedExecModes.get(i);
					JSONObject meta = reactorTool.optJSONObject("_meta");
					if (meta == null) {
						meta = new JSONObject();
					}
					meta.put(MCPUtility.SMSS_MCP_EXECUTION, execMode);
					reactorTool.put("_meta", meta);
					toolsArray.put(reactorTool);
				} catch (Exception e) {
					classLogger.error(
							"Unexpected error creating MCP tool from reactor class: " + reactorClass.getName(), e);
				}
			}
			mcpJson.put("tools", toolsArray);
		} else {
			int numReactors = reactorNames.size();
			List<String> resolvedExecModes = new ArrayList<>(numReactors);
			List<String> mcpExecutionList = getNounAsStringList(ReactorKeysEnum.MCP_EXECUTION.getKey());
			for (int i = 0; i < numReactors; i++) {
				String execModeInput = (mcpExecutionList != null && i < mcpExecutionList.size())
						? mcpExecutionList.get(i)
						: null;
				MCPExecution execModeEnum = MCPExecution.fromValue(execModeInput);

				String execModeStr;
				if (execModeInput == null || execModeEnum == null) {
					execModeStr = MCPExecution.ASK.getValue();
					// Only log if there actually was user input;
					if (execModeInput != null) {
						classLogger.warn("Invalid mcpExecution value '{}' for reactor '{}'; falling back to 'ask'.",
								execModeInput, reactorNames.get(i));
					}
				} else {
					execModeStr = execModeEnum.getValue();
				}
				resolvedExecModes.add(execModeStr);
			}
			for (int i = 0; i < reactorNames.size(); i++) {
				IReactor thisReactor = ReactorFactory.getReactor(this.insight, reactorNames.get(i), null,
						this.insight.getCurFrame());
				JSONObject reactorTool = thisReactor.asMcpTool();
				String execMode = resolvedExecModes.get(i);
				JSONObject meta = reactorTool.optJSONObject("_meta");
				if (meta == null) {
					meta = new JSONObject();
				}
				meta.put(MCPUtility.SMSS_MCP_EXECUTION, execMode);
				reactorTool.put("_meta", meta);
				toolsArray.put(reactorTool);
			}
		}

		JSONObject _meta = new JSONObject();
		LocalDate todayUTC = LocalDate.now(ZoneOffset.UTC);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		_meta.put("last_modified_date", todayUTC.format(formatter));
		mcpJson.put("_meta", _meta);

		if (mcpJson == null || mcpJson.isEmpty()) {
			throw new IllegalArgumentException("Engine " + engine + " does not exist or has no MCP tools defined.");
		}

		String outputFileLoc = engineAssetsFolder + "/mcp/pixel_mcp.json";
		File outputFile = new File(outputFileLoc);
		if (!outputFile.getParentFile().exists() || !outputFile.getParentFile().isDirectory()) {
			outputFile.getParentFile().mkdirs();
		}
		if (outputFile.exists()) {
			outputFile.delete();
		}
		try (FileWriter writer = new FileWriter(outputFile)) {
			String prettyJson = mcpJson.toString(4);
			writer.write(prettyJson);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(
					"Unable to write pixel_mcp.json file. Detailed error = " + e.getMessage());
		}

		Map<String, Object> metadata = SecurityEngineUtils.getAggregateEngineMetadata(engineId, null, false);

		List<Object> s = new ArrayList<>();
		if (metadata.containsKey("tag")) {
			Object metaTag = metadata.get("tag");
			if (metaTag instanceof List<?>) {
				s.addAll((List<Object>) metaTag);
			} else if (metaTag instanceof String) {
				s.add(metaTag);
			}
		}

		if (!s.contains("MCP")) {
			s.add("MCP");
		}

		metadata.put("tag", s);
		SecurityEngineUtils.updateEngineMetadata(engineId, metadata);

		String smssFilePath = engine.getSmssFilePath();
		Map<String, String> mcpEnabledMap = new HashMap<>();
		mcpEnabledMap.put(Constants.MCP_ENABLED, "true");
		try {
			Utility.changePropertiesFileValue(smssFilePath, mcpEnabledMap, false);
			engine.open(smssFilePath);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error enabling mcp in smss");
		}

		String versionGitFolder = EngineUtility.getSpecificEngineVersionFolder(engineCatalogType, engineId, engineName);
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
			comment = "add: MakeEngineMCP executed";
		}

		// add file to git
		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + "/mcp/pixel_mcp.json");

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushEngineFolder(engine, engineAssetsFolder);

		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}
	
	public JSONObject improveEngineMeta(JSONObject engineMeta, String modelId) {
//		TODO: Pass through if no LLMs available or error, for the improve call require json output.
		try {
			if (modelId != null && !(modelId = modelId.trim()).isEmpty()) {
				if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), modelId)) {
					throw new IllegalArgumentException(
							"Model " + modelId + " does not exist or user does not have access.");
				}
				IModelEngine model = Utility.getModel(modelId);
//				InstructModelEngineResponse response = model.instruct(instructTask, instructContext, List.of(Map.of()), insight, Map.of());
				Map<String, Object> kwArgs = new HashMap<>();
//				TODO: swap out types for engineMeta content
				kwArgs.put("schema", MCPUtility.getJsonSchema(engineMeta, callback));
				Room room = RoomUtils.createRoomIfNotExists(GUID.v7().toUUID().toString(), insight, model, question);
				InputMessage inputMessage = InputMessage.builder(room).withInputPrompt(question).withParamMap(kwArgs).withSystemPrompt(instructContext).build();
				inputMessage.setParamMap(kwArgs);
				ResponseMessage response = RoomUtils.askOnceAndDeleteRoom(insight, inputMessage, model);
//				TODO: Get response json struct and convert
				return new JSONObject(response.getContent());
			}
		} catch (Exception e) {
			classLogger.error("Unable to run metadata improve:", e);
		}
		return engineMeta;
	}
	
	BiConsumer<Object, JSONObject> callback = new BiConsumer<Object, JSONObject>() {
		@Override
		public void accept(Object node, JSONObject schema) {
			schemaGeneration(node, schema);
		}
	};
	
	/**
	 * Function to generate the json schema based on the following specifications:
	 * - Keep all objects with a type other than strings the same
	 * - Keep the "type" field the same regardless
	 * @param node
	 * @param schema
	 */
	private void schemaGeneration(Object node, JSONObject schema) {
        if (node instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) node;
            schema.put("type", "object");
            JSONObject properties = new JSONObject();
            JSONArray required = new JSONArray();
            for (String key : jsonObj.keySet()) {
                required.put(key);
                Object value = jsonObj.get(key);
                JSONObject propSchema = new JSONObject();
                // Special handling for "type" keys
                if ("type".equals(key) && value instanceof String) {
                    propSchema.put("type", "string");
                    propSchema.put("const", value);
                } else {
                	schemaGeneration(value, propSchema);
                }
                properties.put(key, propSchema);
            }
            schema.put("properties", properties);
            schema.put("required", required);
            schema.put("additionalProperties", false);
        } else if (node instanceof JSONArray) {
            schema.put("type", "array");
            JSONArray array = (JSONArray) node;
            if (array.length() > 0) {
                JSONObject itemSchema = new JSONObject();
                schemaGeneration(array.get(0), itemSchema);
                schema.put("items", itemSchema);
            }
        } else if (node instanceof String) {
            schema.put("type", "string");
        } else if (node instanceof Integer || node instanceof Long || node instanceof Double || node instanceof Float) {
            schema.put("type", "number");
        } else if (node instanceof Boolean) {
            schema.put("type", "boolean");
        } else if (JSONObject.NULL.equals(node)) {
            schema.put("type", "null");
        }
    }

	@Override
	public String getReactorDescription() {
		return "Generates a mcp/pixel_mcp.json file from a set of reactors";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.REACTOR.getKey())) {
			return "The list of reactors to turn into mcp tools in the pixel_mcp.json";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		} else if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine to add MCP tools from";
		}
		return super.getDescriptionForKey(key);
	}

}