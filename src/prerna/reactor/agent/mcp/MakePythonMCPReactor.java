package prerna.reactor.agent.mcp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

public class MakePythonMCPReactor extends AbstractReactor {

	public MakePythonMCPReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.MODEL.getKey(), 
				ReactorKeysEnum.COMMENT_KEY.getKey()};
		this.keyRequired = new int[] {1, 0, 0};
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
		
		List<String> gitRelativeFilePaths = new ArrayList<>();
		
		if(project.getProjectType() == IProject.PROJECT_TYPE.BLOCKS) {
			IModelEngine modelEngine = null;
			String modelId = this.keyValue.get(this.keysToGet[1]);
			if(modelId != null && !(modelId=modelId.trim()).isEmpty()) {
				if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
					throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access.");
				}
				modelEngine = Utility.getModel(modelId);
			}
			INotebookHelper helper = project.getNotebookHelper();
			helper.createMcpJson(projectAssetFolder+"/py/smss_driver.py", modelEngine, this.insight);
			// add file to git
			gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/py/smss_driver.py");
		}
		
		String pyFolderLoc = projectAssetFolder + "/py";
		File pyFolder = new File(pyFolderLoc);

		if(!pyFolder.exists() || !pyFolder.isDirectory()) {
			String errorOutput = "There is no py/main.py that exists. Please create this file and then try. "
					+ "File main.py is the main driver which is utilized in terms of creating the MCP tools.";
			throw new IllegalArgumentException(errorOutput);
		}

		String mcpPyFileLoc = pyFolderLoc + "/smss_driver.py";
		File mcpPyFile = new File(mcpPyFileLoc);
		if(!mcpPyFile.exists() || !mcpPyFile.isFile()) {
			String errorOutput = "There is no py/smss_driver.py that exists. Please create this file and then try. "
					+ "File main.py is the main driver which is utilized in terms of creating the MCP tools.";
			throw new IllegalArgumentException(errorOutput);
		}
		
		// use the smss_util to get the needed information
		String mcpFolderLoc = projectAssetFolder + "/mcp";
		File mcpFolder = new File(mcpFolderLoc);
		if(!mcpFolder.exists()) {
			mcpFolder.mkdir();
		}
		String outputFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		mcpPyFileLoc = mcpPyFileLoc.replace("\\", "/");
		outputFileLoc = outputFileLoc.replace("\\", "/");
		String[] script = new String[] {"smssutil.gen_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc + "')"};
		Map<String, Object> mcpJson = (Map<String, Object>) insight.getPyTranslator().runScript(script);
		
		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if(comment == null) {
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
		ClusterUtil.pushProjectFolder(project, assetFolder);
		
		return new NounMetadata(mcpJson, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Generates a mcp/py_mcp.json file from the py/main.py file function";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if(key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}
}
