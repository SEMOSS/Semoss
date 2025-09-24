package prerna.reactor.agent.mcp;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.ReactorFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class MakePixelMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakePixelMCPReactor.class);

	public MakePixelMCPReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.REACTOR.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.DATABASE.getKey()};
		this.keyRequired = new int[] {1, 0, 0, 0, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have access to edit.");
		}
		IProject project = Utility.getProject(projectId);
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);

		JSONArray toolsArray = new JSONArray();
		List<String> reactorNames = getNounAsStringList(ReactorKeysEnum.REACTOR.getKey());
		for(String reactor : reactorNames) {
			IReactor thisReactor = ReactorFactory.getReactor(this.insight, reactor, null, this.insight.getCurFrame());
			JSONObject reactorTool = thisReactor.asMcpTool();
			toolsArray.put(reactorTool);
		}

		String engine = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if(engine != null && !engine.isEmpty()) {
			List<String> engineTools = Utility.getEngine(engine).getMCPTools();
			if(engineTools != null) {
				for(String toolName : engineTools) {
					IReactor thisReactor = ReactorFactory.getReactor(this.insight, toolName, null, this.insight.getCurFrame());
					JSONObject reactorTool = thisReactor.asMcpTool();
					toolsArray.put(reactorTool);
				}
			}
		}

		String databaseId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if(databaseId != null && !databaseId.isEmpty()) {
			List<String> dbTools = Utility.getDatabase(databaseId).getMCPTools();
			if(dbTools != null) {
				for(String toolName : dbTools) {
					IReactor thisReactor = ReactorFactory.getReactor(this.insight, toolName, null, this.insight.getCurFrame());
					JSONObject reactorTool = thisReactor.asMcpTool();
					toolsArray.put(reactorTool);
				}
			}
		}
		JSONObject mcpJson = new JSONObject();
		mcpJson.put("tools", toolsArray);
		JSONObject _meta = new JSONObject();
		LocalDate todayUTC = LocalDate.now(ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        _meta.put("last_modified_date", todayUTC.format(formatter));
		mcpJson.put("_meta", _meta);

		String outputFileLoc = projectAssetFolder + "/mcp/pixel_mcp.json";
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

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if(comment == null) {
			comment = "add: MakePixelMCP executed";
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
		ClusterUtil.pushProjectFolder(project, assetFolder);
		
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
