package prerna.reactor.agent.mcp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import net.snowflake.client.jdbc.internal.google.gson.Gson;
import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.project.api.IProject;
import prerna.project.impl.notebook.INotebookHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class MakeNotebookCellMCPReactor extends AbstractReactor {

	public MakeNotebookCellMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.MODEL.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey(), "cellId" };
		this.keyRequired = new int[] { 1, 0, 0, 1 };
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
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit.");
		}
		IProject project = Utility.getProject(projectId);
		if (project.getProjectType() != IProject.PROJECT_TYPE.BLOCKS) {
			throw new IllegalArgumentException("Can only call this reactor on a no-code (blcoks) app");
		}
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);
		String pythonMcpDriver = projectAssetFolder + "/py/smss_driver.py";

		IModelEngine modelEngine = null;
		String modelId = this.keyValue.get(this.keysToGet[1]);
		if (modelId != null && !(modelId = modelId.trim()).isEmpty()) {
			if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
				throw new IllegalArgumentException(
						"Model " + modelId + " does not exist or user does not have access.");
			}
			modelEngine = Utility.getModel(modelId);
		}

		String cellId = this.keyValue.get("cellId");
		// first see if we have this cellId exists as metadata on any of the functions
		// if yes, then we will grab the tool that has this cellId as metadata
		// and then find the name which must match the python function name
		// then we will parse the file to delete the function
		JSONObject existingTool = MCPUtility.findPythonToolWithCellId(project, cellId);
		if (existingTool != null) {
			MCPUtility.removeExistingFunctionFromPyFile(this.insight, pythonMcpDriver, existingTool.get("name") + "");
		}

		INotebookHelper helper = project.getNotebookHelper();
		Map<String, String> functionNameToCellId = helper.transformNotebookCellToMcpDriver(pythonMcpDriver, modelEngine,
				this.insight, cellId);

		List<String> gitRelativeFilePaths = new ArrayList<>();
		// add file to git
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/py/smss_driver.py");

		String pyFolderLoc = projectAssetFolder + "/py";
		File pyFolder = new File(pyFolderLoc);

		if (!pyFolder.exists() || !pyFolder.isDirectory()) {
			String errorOutput = "There is no py/smss_driver.py that was created from the notebook smss_driver. Please create make sure the notebook cell passed is accurate. "
					+ "File smss_driver.py is the main driver which is utilized in terms of creating the MCP tools.";
			throw new IllegalArgumentException(errorOutput);
		}

		String mcpPyFileLoc = pyFolderLoc + "/smss_driver.py";
		File mcpPyFile = new File(mcpPyFileLoc);
		if (!mcpPyFile.exists() || !mcpPyFile.isFile()) {
			String errorOutput = "There is no py/smss_driver.py that exists. Please create this file and then try. "
					+ "File smss_driver.py is the main driver which is utilized in terms of creating the MCP tools.";
			throw new IllegalArgumentException(errorOutput);
		}

		// use the smss_util to get the needed information
		String mcpFolderLoc = projectAssetFolder + "/mcp";
		File mcpFolder = new File(mcpFolderLoc);
		if (!mcpFolder.exists()) {
			mcpFolder.mkdir();
		}
		String outputFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		mcpPyFileLoc = mcpPyFileLoc.replace("\\", "/");
		outputFileLoc = outputFileLoc.replace("\\", "/");
		String script = "smssutil.add_function_to_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc
				+ "', function_name_to_cell=" + (new Gson().toJson(functionNameToCellId)) + ")";
		Map<String, Object> mcpJson = (Map<String, Object>) insight.getPyTranslator().runScript(script);

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
			comment = "add: MakeNotebookCellMCP executed";
		}

		// add file to git
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/mcp/py_mcp.json");

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushProjectFolder(project, assetFolder);

		return new NounMetadata(mcpJson, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return """
				Generates a function from a specific cell in the smss_driver that is written to py/smss_driver.py.
				The function is then added to the mcp/py_mcp.json.
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		} else if (key.equals("cellId")) {
			return "The cell id in the smss_driver notebook to convert into an mcp tool";
		}
		return super.getDescriptionForKey(key);
	}
}
