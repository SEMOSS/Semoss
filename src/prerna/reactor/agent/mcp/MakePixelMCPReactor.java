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
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class MakePixelMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakePixelMCPReactor.class);

	private static final String PACKAGE_KEY = "package";

	/**
	 * Stamped into every generated tool as {@link MCPUtility#SMSS_MCP_GENERATOR}.
	 */
	private static final String GENERATOR_ID = "MakePixelMCP";

	public MakePixelMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.REACTOR.getKey(), PACKAGE_KEY,
				ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.MCP_METADATA.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0 };
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
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);

		List<String> reactorNames = getNounAsStringList(ReactorKeysEnum.REACTOR.getKey());
		List<String> packageNames = getNounAsStringList(PACKAGE_KEY);
		List<Map<String, Object>> mcpMetadataList = getList(ReactorKeysEnum.MCP_METADATA.getKey());
		boolean scanAll = PixelMCPToolBuilder.isFullScan(reactorNames, packageNames);

		JSONArray toolsArray = PixelMCPToolBuilder.buildTools(this.insight, project, reactorNames, packageNames,
				mcpMetadataList);

		// Both the scan and the explicit-reactor path feed this array.
		MCPUtility.stampGenerator(toolsArray, GENERATOR_ID);

		String outputFileLoc = projectAssetFolder + MCPUtility.PIXEL_MCP_RELATIVE_PATH;

		// Only a full scan rebuilds every tool, so a run narrowed to named reactors or
		// a package must not prune what it did not look at.
		toolsArray = MCPUtility.mergeGeneratedTools(MCPUtility.readMcpJson(outputFileLoc), toolsArray, GENERATOR_ID,
				scanAll);

		JSONObject mcpJson = PixelMCPToolBuilder.wrapMcpJson(toolsArray);

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
			classLogger.error("Unable to write pixel_mcp.json file", e);
			throw new IllegalArgumentException(
					"Unable to write pixel_mcp.json file. Detailed error = " + e.getMessage());
		}

		// add tags
		MCPUtility.addMCPTag(project);

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
			comment = "add: configured Pixel MCP tool";
		}

		// add file to git
		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + MCPUtility.PIXEL_MCP_RELATIVE_PATH);

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
		return """
				Generates a mcp/pixel_mcp.json file from a set of reactors. \
				By default, scans all of the app's reactors and includes those that override getMcpToolMetadata(). \
				Optionally filter by Java package via 'package', or list reactors explicitly via 'reactor'.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app. If not passed, will try to use the app context.";
		} else if (key.equals(ReactorKeysEnum.REACTOR.getKey())) {
			return "The list of reactors to turn into mcp tools in the pixel_mcp.json";
		} else if (key.equals(PACKAGE_KEY)) {
			return """
					Java package(s) to scan for reactor classes that override getMcpToolMetadata(). \
					Scans the project's compiled reactors and includes those whose package matches. \
					Example: 'reactors.vaapi' includes all MCP reactors in reactors.vaapi and sub-packages.\
					""";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}

}
