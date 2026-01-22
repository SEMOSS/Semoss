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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.ReactorFactory;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.GenRowStruct;
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
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.REACTOR.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.MCP_METADATA.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
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
		if (projectId == null || projectId.isEmpty()) {
			projectId = insight.getContextProjectId();
			if (projectId == null || projectId.isEmpty()) {
				projectId = insight.getProjectId();
			}
		}
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the project id or set the app context");
		}

		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit.");
		}
		IProject project = Utility.getProject(projectId);
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);

		JSONArray toolsArray = new JSONArray();
		List<String> reactorNames = getNounAsStringList(ReactorKeysEnum.REACTOR.getKey());
		List<Map<String, Object>> mcpMetadataList = getMetadataMapList();
		boolean mcpMetaExists = false;
		if (mcpMetadataList != null) {
			mcpMetaExists = true;
			if (mcpMetadataList.size() != reactorNames.size()) {
				throw new IllegalArgumentException("The number of MCP_METADATA entries must match the number of REACTOR entries.");
			}
		}

		for (int i = 0; i < reactorNames.size(); i++) {
			IReactor thisReactor = ReactorFactory.getReactor(this.insight, reactorNames.get(i), null,
					this.insight.getCurFrame());
			JSONObject reactorTool = thisReactor.asMcpTool();
			JSONObject meta = reactorTool.optJSONObject("_meta");
			if (meta == null) {
				meta = new JSONObject();
			}
			// Populate additional metadata from the parameter
			Map<String, Object> additionalMeta = mcpMetaExists ? mcpMetadataList.get(i) : new HashMap<>();
			// Parse for specific known keys

			// execution mode
			String execModeInput = (String) additionalMeta.getOrDefault(MCPUtility.SMSS_MCP_EXECUTION, "ask");
			MCPExecution execModeEnum = MCPExecution.fromValue(execModeInput);
			if (execModeEnum != null) {
				meta.put(MCPUtility.SMSS_MCP_EXECUTION, execModeEnum.getValue());
			} else {
				// default to ASK
				meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
				if (execModeInput != null) {
					classLogger.warn("Invalid SMSS_MCP_EXECUTION value '{}' for reactor '{}'; falling back to 'ask'.",
							execModeInput, reactorNames.get(i));
				}
			}
			// UI
			Map<String, Object> uiMap = null;
			try {
				uiMap = (Map<String, Object>) additionalMeta.get(MCPUtility.SMSS_MCP_UI);
			} catch (ClassCastException e) {
				classLogger.error("Invalid type for SMSS_MCP_UI in reactor '{}'; expected a map of key-value pairs.",
						reactorNames.get(i));
			}
			
			String resourceURI = null;
			String loadingMessage = null;
			
			if (uiMap != null) {
				// Only add known keys
				resourceURI = (String) uiMap.getOrDefault(MCPUtility.UI_RESOURCE_URI, null);
				loadingMessage = (String) uiMap.getOrDefault(MCPUtility.UI_LOADING_MESSAGE, null);
			}
			
			JSONObject uiJson = new JSONObject();
			uiJson.put(MCPUtility.UI_RESOURCE_URI, resourceURI != null ? resourceURI : JSONObject.NULL);
			uiJson.put(MCPUtility.UI_LOADING_MESSAGE, loadingMessage != null ? loadingMessage : JSONObject.NULL);
			meta.put(MCPUtility.SMSS_MCP_UI, uiJson);

			reactorTool.put("_meta", meta);
			toolsArray.put(reactorTool);
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

		Map<String, Object> metadata = SecurityProjectUtils.getAggregateProjectMetadata(projectId, null, false);

		List<Object> s = new ArrayList<>();
		if (metadata.containsKey("tag")) {
			Object metaTag = metadata.get("tag");
			if (metaTag instanceof List<?>) {
				s.addAll((List<Object>) metaTag);
			} else if (metaTag instanceof String) {
				s.add(metaTag);
			}
		}

		// we only need to add MCP if it is not already there
		if (!s.contains("MCP")) {
			s.add("MCP");

			metadata.put("tag", s);
			SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
		}

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
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
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app. If not passed, will try to use the app context.";
		} else if (key.equals(ReactorKeysEnum.REACTOR.getKey())) {
			return "The list of reactors to turn into mcp tools in the pixel_mcp.json";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		} else if (key.equals(ReactorKeysEnum.MCP_EXECUTION.getKey())) {
			return "Optional list of execution modes for each reactor: auto, ask, or disabled";
		}
		return super.getDescriptionForKey(key);
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getMetadataMapList() {
		List<Map<String, Object>> metadataMapList = null;
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MCP_METADATA.getKey());
		if (grs != null && !grs.isEmpty()) {
			metadataMapList = new ArrayList<>();
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				metadataMapList.add((Map<String, Object>) grs.get(i));
			}
		}
		return metadataMapList;
	}
}
