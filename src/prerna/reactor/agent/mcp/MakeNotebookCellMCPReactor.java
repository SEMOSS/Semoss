/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.agent.mcp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

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
		this.keyRequired = new int[] { 0, 0, 0, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = resolveContextEngineId(this.keyValue.get(this.keysToGet[0]));

		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit.");
		}
		IProject project = Utility.getProject(projectId);
		if (project.getProjectType() != IProject.PROJECT_TYPE.BLOCKS) {
			throw new IllegalArgumentException("Can only call this reactor on a no-code (blcoks) app");
		}
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);
		String pythonMcpDriver = projectAssetFolder + "/py/" + MCPUtility.MCP_PY_FILE_NAME;

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
		// only need to do this if the python file exists - user might have deleted it
		if (new File(pythonMcpDriver).isFile()) {
			JSONObject existingTool = MCPUtility.findPythonToolWithCellId(project, cellId);
			if (existingTool != null) {
				MCPUtility.removeExistingFunctionFromPyFile(this.insight, pythonMcpDriver,
						existingTool.get("name") + "");
				MCPUtility.removePythonFunctionFromMCPJson(project, existingTool.get("name") + "");
			}
		}

		INotebookHelper helper = project.getNotebookHelper();
		Map<String, String> functionNameToCellId = helper.transformNotebookCellToMcpDriver(pythonMcpDriver, modelEngine,
				this.insight, cellId);

		List<String> gitRelativeFilePaths = new ArrayList<>();
		// add file to git
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/py/" + MCPUtility.MCP_PY_FILE_NAME);

		String pyFolderLoc = projectAssetFolder + "/py";
		String mcpPyFileLoc = pyFolderLoc + "/" + MCPUtility.MCP_PY_FILE_NAME;
		File mcpPyFile = new File(mcpPyFileLoc);
		if (!mcpPyFile.exists() || !mcpPyFile.isFile()) {
			String errorOutput = ("There is no py/<file_placeholder> that exists. Please create this file and then try. "
					+ "File <file_placeholder> is the main driver which is utilized in terms of creating the MCP tools.")
					.replace("<file_placeholder>", MCPUtility.MCP_PY_FILE_NAME);
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
				+ "', function_name='" + functionNameToCellId.keySet().iterator().next() + "', function_name_to_cell="
				+ (GSON.toJson(functionNameToCellId)) + ")";
		Map<String, Object> mcpJson = (Map<String, Object>) insight.getPyTranslator().runScript(script);

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
			comment = "add: configured Notebook Cell MCP tool";
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
				Generates a function from a specific cell in the <notebook_placeholder> that is written to py/<file_placeholder>.
				The function is then added to the mcp/py_mcp.json.
				"""
				.replace("<file_placeholder>", MCPUtility.MCP_PY_FILE_NAME)
				.replace("<notebook_placeholder>", MCPUtility.MCP_NOTEBOOK_NAME);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app. If not passed, will try to use the app context.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		} else if (key.equals("cellId")) {
			return "The cell id in the " + MCPUtility.MCP_NOTEBOOK_NAME + " notebook to convert into an mcp tool";
		}
		return super.getDescriptionForKey(key);
	}
}
