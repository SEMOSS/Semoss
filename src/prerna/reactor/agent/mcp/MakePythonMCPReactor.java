package prerna.reactor.agent.mcp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class MakePythonMCPReactor extends AbstractReactor {

	public MakePythonMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.MODEL.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);

		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the engineId id");
		}

		// get engine
		IEngine engine = null;
		try {
			engine = Utility.getEngine(engineId);
		} catch (Exception ex) {
			// ignore
		}
		if (engine == null) {
			engine = Utility.getProject(engineId);
		}
		IEngine.CATALOG_TYPE engineType = engine.getCatalogType();
		User user = this.insight.getUser();

		// check security
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		if (engineType == CATALOG_TYPE.PROJECT) {
			if (!SecurityProjectUtils.userCanViewProject(user, engineId)) {
				throw new IllegalArgumentException(
						"Project " + engineId + " does not exist or user does not have access");
			}
		} else {
			if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
				throw new IllegalArgumentException(
						"Engine " + engineId + " does not exist or user does not have access");
			}
		}

		/*
		 * if (projectId == null || projectId.isEmpty()) { projectId =
		 * insight.getContextProjectId(); if (projectId == null || projectId.isEmpty())
		 * { projectId = insight.getProjectId(); } }
		 */

		String engineAssetsFolder = null;
		String versionGitFolder = null;

		if (engineType == CATALOG_TYPE.PROJECT) {
			engineAssetsFolder = AssetUtility.getProjectAssetsFolder(engineId);

			versionGitFolder = AssetUtility.getProjectVersionFolder(((IProject) engine).getProjectName(),
					((IProject) engine).getProjectId());
		} else {
			String engineName = engine.getEngineName();

			engineAssetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId, engineName);
			engineAssetsFolder = engineAssetsFolder.replace("\\", "/");

			versionGitFolder = EngineUtility.getSpecificEngineVersionFolder(engineType, engineId, engineName);
		}

		List<String> gitRelativeFilePaths = new ArrayList<>();

		Map<String, String> functionNameToCellId = null;
//		if (project.getProjectType() == IProject.PROJECT_TYPE.BLOCKS) {
//			IModelEngine modelEngine = null;
//			String modelId = this.keyValue.get(this.keysToGet[1]);
//			if (modelId != null && !(modelId = modelId.trim()).isEmpty()) {
//				if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
//					throw new IllegalArgumentException(
//							"Model " + modelId + " does not exist or user does not have access.");
//				}
//				modelEngine = Utility.getModel(modelId);
//			}
//			INotebookHelper helper = project.getNotebookHelper();
//			functionNameToCellId = helper.transformNotebookToMcpDriver(
//					projectAssetFolder + "/py/" + MCPUtility.MCP_PY_FILE_NAME, modelEngine, this.insight);
//			// add file to git
//			gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/py/" + MCPUtility.MCP_PY_FILE_NAME);
//		}

		String pyFolderLoc = engineAssetsFolder + "/py";
		String mcpPyFileLoc = pyFolderLoc + "/" + MCPUtility.MCP_PY_FILE_NAME;
		File mcpPyFile = new File(mcpPyFileLoc);
		if (!mcpPyFile.exists() || !mcpPyFile.isFile()) {
			// test legacy file name
			mcpPyFileLoc = pyFolderLoc + "/" + MCPUtility.LEGACY_PY_FILE_NAME;
			mcpPyFile = new File(mcpPyFileLoc);
			if (!mcpPyFile.exists() || !mcpPyFile.isFile()) {
				String errorOutput = ("There is no py/<file_placeholder> that exists. Please create this file and then try. "
						+ "File <file_placeholder> is the main driver which is utilized in terms of creating the MCP tools.")
						.replace("<file_placeholder>", MCPUtility.MCP_PY_FILE_NAME);
				throw new IllegalArgumentException(errorOutput);
			}
		}

		// use the smss_util to get the needed information
		String mcpFolderLoc = engineAssetsFolder + "/mcp";
		File mcpFolder = new File(mcpFolderLoc);
		if (!mcpFolder.exists()) {
			mcpFolder.mkdir();
		}
		String outputFileLoc = engineAssetsFolder + "/mcp/py_mcp.json";
		mcpPyFileLoc = mcpPyFileLoc.replace("\\", "/");
		outputFileLoc = outputFileLoc.replace("\\", "/");
		String script = null;
		if (functionNameToCellId == null || functionNameToCellId.isEmpty()) {
			script = "smssutil.gen_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc + "')";
		} else {
			script = "smssutil.gen_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc
					+ "', function_name_to_cell=" + (new Gson().toJson(functionNameToCellId)) + ")";
		}
		Map<String, Object> mcpJson = (Map<String, Object>) insight.getPyTranslator().runScript(script);

		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
			comment = "add: MakePythonMCP executed";
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
		if (engineType == CATALOG_TYPE.PROJECT) {
			ClusterUtil.pushProjectFolder((IProject) engine, engineAssetsFolder);
		} else {
			ClusterUtil.pushEngineFolder(engine, engineAssetsFolder);
		}

		return new NounMetadata(mcpJson, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return """
				Generates a mcp/py_mcp.json file from the py/<file_placeholder> file function.
				If the project is a no-code app, the <notebook_placeholder> notebook sheet will be transformed
				into a py/<file_placeholder> file to then generate the mcp/py_mcp.json.
				""".replace("<file_placeholder>", MCPUtility.MCP_PY_FILE_NAME).replace("<notebook_placeholder>",
				MCPUtility.MCP_NOTEBOOK_NAME);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app or engine. If not passed, will try to use the app context.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}
}
