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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class MakePythonMCPReactor extends AbstractReactor {

	public MakePythonMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() + "," + ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.MODEL.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = resolveContextEngineId(this.keyValue.get(this.keysToGet[0].split(",")[0]));

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

		if (engine == null) {
			throw new NullPointerException("Unknown engine or project with id " + engineId);
		}

		IEngine.CATALOG_TYPE engineType = engine.getCatalogType();
		User user = this.insight.getUser();

		// check security
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		if (engineType == CATALOG_TYPE.PROJECT) {
			if (!SecurityProjectUtils.userCanEditProject(user, engineId)) {
				throw new IllegalArgumentException(
						"Project " + engineId + " does not exist or user does not have access");
			}
		} else {
			if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
				throw new IllegalArgumentException(
						"Engine " + engineId + " does not exist or user does not have access");
			}
		}

		String engineAssetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId,
				engine.getEngineName());
		String versionGitFolder = EngineUtility.getSpecificEngineVersionFolder(engineType, engineId,
				engine.getEngineName());

		List<String> gitRelativeFilePaths = new ArrayList<>();

		Map<String, String> functionNameToCellId = null;

		// no longer mixing responsibility of project blocks with py files

//		if (engineType == CATALOG_TYPE.PROJECT
//				&& ((IProject) engine).getProjectType() == IProject.PROJECT_TYPE.BLOCKS) {
//			IModelEngine modelEngine = null;
//			String modelId = this.keyValue.get(this.keysToGet[1]);
//			if (modelId != null && !(modelId = modelId.trim()).isEmpty()) {
//				if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
//					throw new IllegalArgumentException(
//							"Model " + modelId + " does not exist or user does not have access.");
//				}
//				modelEngine = Utility.getModel(modelId);
//			}
//			INotebookHelper helper = ((IProject) engine).getNotebookHelper();
//			functionNameToCellId = helper.transformNotebookToMcpDriver(
//					engineAssetsFolder + "/py/" + MCPUtility.MCP_PY_FILE_NAME, modelEngine, this.insight);
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
//		String script = null;
//		if (functionNameToCellId == null || functionNameToCellId.isEmpty()) {
//			script = "smssutil.gen_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc + "')";
//		} else {
//			script = "smssutil.gen_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc
//					+ "', function_name_to_cell=" + (new Gson().toJson(functionNameToCellId)) + ")";
//		}
		String script = "smssutil.gen_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc + "')";
		Map<String, Object> mcpJson = (Map<String, Object>) insight.getPyTranslator().runScript(script);

		// add tags
		MCPUtility.addMCPTag(engine);
		// update smss of engine
		boolean mcpEnabled = engine.isMCPEnabled();
		if (!mcpEnabled && engineType != CATALOG_TYPE.PROJECT) {
			String smssFilePath = engine.getSmssFilePath();
			Map<String, String> mcpEnabledMap = new HashMap<>();
			mcpEnabledMap.put(Constants.MCP_ENABLED, "true");
			try {
				Utility.changePropertiesFileValue(smssFilePath, mcpEnabledMap, false);
				engine.open(smssFilePath);
			} catch (Exception e) {
				throw new IllegalArgumentException("Error enabling mcp in smss");
			}
		}

		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
			comment = "add: configured Python MCP tool";
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
			if (!mcpEnabled) {
				ClusterUtil.pushEngineSmss(engineId);
			}
		}

		return new NounMetadata(mcpJson, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return """
				Generates a mcp/py_mcp.json file from the py/<file_placeholder> file function.
				""".replace("<file_placeholder>", MCPUtility.MCP_PY_FILE_NAME).replace("<notebook_placeholder>",
				MCPUtility.MCP_NOTEBOOK_NAME);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey()) || key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the engine or project/app. If not passed, will try to use the app context.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}
}
