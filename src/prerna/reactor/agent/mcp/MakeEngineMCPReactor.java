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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.ReactorFactory;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.reactor.storage.DeleteFromStorageReactor;
import prerna.reactor.storage.ListStoragePathDetailsReactor;
import prerna.reactor.storage.ListStoragePathReactor;
import prerna.reactor.storage.PullFromStorageReactor;
import prerna.reactor.storage.PushToStorageReactor;
import prerna.reactor.storage.SyncLocalToStorageReactor;
import prerna.reactor.storage.SyncStorageToLocalReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class MakeEngineMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeEngineMCPReactor.class);

    private static final Map<IEngine.CATALOG_TYPE, List<Class<? extends IReactor>>> STANDARD_ENGINE_TOOLS = new HashMap<>() {{

        // Storage tools
        put(IEngine.CATALOG_TYPE.STORAGE, new ArrayList<>(Arrays.asList(
            ListStoragePathReactor.class,
            ListStoragePathDetailsReactor.class,
            PullFromStorageReactor.class,
            PushToStorageReactor.class,
            SyncStorageToLocalReactor.class,
            SyncLocalToStorageReactor.class,
            DeleteFromStorageReactor.class
        )));
    }};

	public MakeEngineMCPReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.REACTOR.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.MCP_EXECUTION.getKey()};
		this.keyRequired = new int[] {1, 0, 0, 0};
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
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have access to edit.");
		}

		IEngine engine = Utility.getEngine(engineId);
		IEngine.CATALOG_TYPE eType = engine.getCatalogType();
		String engineName = engine.getEngineName();

		String engineAssetsFolder = EngineUtility.getSpecificEngineAssetsFolder(eType, engineId, engineName);
		engineAssetsFolder = engineAssetsFolder.replace("\\", "/");

        JSONObject mcpJson = new JSONObject();
		JSONArray toolsArray = new JSONArray();

        List<String> reactorNames = getNounAsStringList(ReactorKeysEnum.REACTOR.getKey());

        if (reactorNames == null || reactorNames.isEmpty()) {
            List<Class<? extends IReactor>> reactors = STANDARD_ENGINE_TOOLS.getOrDefault(eType, new ArrayList<>());
            int numReactors = reactors.size();
            List<String> resolvedExecModes = new ArrayList<>(numReactors);
            List<String> mcpExecutionList = getNounAsStringList(ReactorKeysEnum.MCP_EXECUTION.getKey());
            for (int i = 0; i < numReactors; i++) {
                String execModeInput = (mcpExecutionList != null && i < mcpExecutionList.size()) ? mcpExecutionList.get(i) : null;
                MCPExecution execModeEnum = MCPExecution.fromValue(execModeInput);

                String execModeStr;
                if (execModeInput == null || execModeEnum == null) {
                    execModeStr = MCPExecution.ASK.getValue();
                    // Only log if there actually was user input;
                    if (execModeInput != null) {
                        classLogger.warn("Invalid mcpExecution value '{}' for reactor '{}'; falling back to 'ask'.", execModeInput, reactorNames.get(i));
                    }
                } else {
                    execModeStr = execModeEnum.getValue();
                }
                resolvedExecModes.add(execModeStr);
            }
            Map<String, JSONObject> keys = Map.of(ReactorKeysEnum.ENGINE.getKey(), new JSONObject().put("enum", new JSONArray().put(engineId)));
            
            for (int i = 0; i < reactors.size(); i++) {
            	Class<? extends IReactor> reactorClass = reactors.get(i);
                try {
                	IReactor thisReactor = reactorClass.getConstructor().newInstance();
                    JSONObject reactorTool = thisReactor.asMcpToolWithPresetKeys(keys);
                    String execMode = resolvedExecModes.get(i);
                    JSONObject meta = reactorTool.optJSONObject("_meta");
                    if (meta == null) meta = new JSONObject();
                    meta.put(MCPUtility.SMSS_MCP_EXECUTION, execMode);
                    reactorTool.put("_meta", meta);
                    toolsArray.put(reactorTool);
                } catch (Exception e) {
                    classLogger.error("Unexpected error creating MCP tool from reactor class: " + reactorClass.getName(), e);
                }
            }
            mcpJson.put("tools", toolsArray);
        } else {
            int numReactors = reactorNames.size();
            List<String> resolvedExecModes = new ArrayList<>(numReactors);
            List<String> mcpExecutionList = getNounAsStringList(ReactorKeysEnum.MCP_EXECUTION.getKey());
            for (int i = 0; i < numReactors; i++) {
                String execModeInput = (mcpExecutionList != null && i < mcpExecutionList.size()) ? mcpExecutionList.get(i) : null;
                MCPExecution execModeEnum = MCPExecution.fromValue(execModeInput);

                String execModeStr;
                if (execModeInput == null || execModeEnum == null) {
                    execModeStr = MCPExecution.ASK.getValue();
                    // Only log if there actually was user input;
                    if (execModeInput != null) {
                        classLogger.warn("Invalid mcpExecution value '{}' for reactor '{}'; falling back to 'ask'.", execModeInput, reactorNames.get(i));
                    }
                } else {
                    execModeStr = execModeEnum.getValue();
                }
                resolvedExecModes.add(execModeStr);
            }
            for(int i = 0; i < reactorNames.size(); i++) {
                IReactor thisReactor = ReactorFactory.getReactor(this.insight, reactorNames.get(i), null, this.insight.getCurFrame());
                JSONObject reactorTool = thisReactor.asMcpTool();
                String execMode = resolvedExecModes.get(i);
                JSONObject meta = reactorTool.optJSONObject("_meta");
                if (meta == null) meta = new JSONObject();
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

		if(mcpJson == null || mcpJson.isEmpty()) {
			throw new IllegalArgumentException("Engine " + engine + " does not exist or has no MCP tools defined.");
		}

		String outputFileLoc = engineAssetsFolder + "/mcp/pixel_mcp.json";
		File outputFile = new File(outputFileLoc);
		if(!outputFile.getParentFile().exists() || !outputFile.getParentFile().isDirectory()) {
			outputFile.getParentFile().mkdirs();
		}
		if(outputFile.exists()) {
			outputFile.delete();
		}
		try (FileWriter writer = new FileWriter(outputFile)) {
            String prettyJson = mcpJson.toString(4);
            writer.write(prettyJson);
        } catch (IOException e) {
        	classLogger.error(Constants.STACKTRACE, e);
        	throw new IllegalArgumentException("Unable to write pixel_mcp.json file. Detailed error = " + e.getMessage());
		}

		Map<String, Object> metadata = SecurityEngineUtils.getAggregateEngineMetadata(engineId, null, false);

		List<Object> s = new ArrayList<>();
		if (metadata.containsKey("tag")) {
			Object metaTag = metadata.get("tag");
			if (metaTag instanceof List<?>) {
			    s.addAll((List<Object>) metaTag);
			} else if (metaTag instanceof String) {
			    s.add((String) metaTag);
			}
		}

		if (!s.contains("MCP")) {
			s.add("MCP");
		}

		metadata.put("tag", s);

		SecurityEngineUtils.updateEngineMetadata(engineId, metadata);

		String versionGitFolder = EngineUtility.getSpecificEngineVersionFolder(eType, engineId, engineName);
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if(comment == null) {
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

	@Override
	public String getReactorDescription() {
		return "Generates a mcp/pixel_mcp.json file from a set of reactors";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if(key.equals(ReactorKeysEnum.REACTOR.getKey())) {
			return "The list of reactors to turn into mcp tools in the pixel_mcp.json";
		} else if(key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		} else if(key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine to add MCP tools from";
		}
		return super.getDescriptionForKey(key);
	}
}