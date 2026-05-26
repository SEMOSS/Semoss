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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import prerna.reactor.agent.mcp.MCPUtility.MCPDisplayOption;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
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
		if (reactorNames == null) {
			reactorNames = new ArrayList<>();
		}
		List<String> packageNames = getNounAsStringList(PACKAGE_KEY);
		List<Map<String, Object>> mcpMetadataList = getList(ReactorKeysEnum.MCP_METADATA.getKey());
		boolean mcpMetaExists = false;
		if (mcpMetadataList != null) {
			mcpMetaExists = true;
			if (mcpMetadataList.size() != reactorNames.size()) {
				throw new IllegalArgumentException("The number of " + ReactorKeysEnum.MCP_METADATA.getKey()
						+ " entries must match the number of REACTOR entries.");
			}
		}

		boolean hasPackages = packageNames != null && !packageNames.isEmpty();
		boolean hasReactors = !reactorNames.isEmpty();
		// Default behavior: if no reactor or package is specified, scan all app
		// reactors
		boolean scanAll = !hasReactors && !hasPackages;

		// Track reactor names already added to avoid duplicates when both scan and
		// reactor are provided
		Set<String> addedReactorNames = new LinkedHashSet<>();

		// Phase 1: Scan for reactors that override getMcpToolMetadata()
		// If neither reactor nor package is provided, scans every reactor in the app
		// If package is provided, filters by package prefix
		if (scanAll || hasPackages) {
			// Trigger compilation if reactors haven't been loaded yet.
			// getAvailableReactors() only returns the cache - calling compileReactors()
			if (project.getAvailableReactors().isEmpty()) {
				project.compileReactors();
			}
			for (String availableName : project.getAvailableReactors()) {
				IReactor reactor = project.getReactor(availableName);
				if (reactor instanceof AbstractReactor && ((AbstractReactor) reactor).getMcpToolMetadata() != null
						&& (scanAll || matchesPackage(reactor.getClass().getPackageName(), packageNames))) {
					JSONObject reactorTool = reactor.asMcpTool();
					String functionName = reactorTool.getString("name");

					// if we encounter the same reactor name
					// we should make it unique
					// this is possible as we are allowing subpackages where files can share names
					// outside of exact package match
					String addedToolName = functionName;
					int counter = 1;
					while (addedReactorNames.contains(addedToolName)) {
						addedToolName = functionName + (++counter);
					}
					// update the toolname
					// this works because we use the {@code MCPUtility.SMSS_FUNCTION_NAME} in meta
					// for the actual reactor to run
					if (functionName != addedToolName) {
						reactorTool.put("name", addedToolName);
					}

					toolsArray.put(reactorTool);
					addedReactorNames.add(functionName);
				}
			}
		}

		// Phase 2: Process explicitly listed reactors (existing behavior)
		for (int i = 0; i < reactorNames.size(); i++) {
			IReactor thisReactor = ReactorFactory.getReactor(this.insight, reactorNames.get(i), null,
					this.insight.getCurFrame());
			JSONObject reactorTool = thisReactor.asMcpTool();
			String functionName = reactorTool.getString("name");

			// if we encounter the same reactor name
			// we should make it unique
			String addedToolName = functionName;
			int counter = 1;
			while (addedReactorNames.contains(addedToolName)) {
				addedToolName = functionName + (++counter);
			}
			// update the toolname
			// this works because we use the {@code MCPUtility.SMSS_FUNCTION_NAME} in meta
			// for the actual reactor to run
			if (functionName != addedToolName) {
				reactorTool.put("name", addedToolName);
			}

			JSONObject meta = reactorTool.optJSONObject("_meta");
			if (meta == null) {
				meta = new JSONObject();
			}
			meta.put(MCPUtility.SMSS_FUNCTION_NAME, functionName);

			// Determine if explicit mcpMetadata was provided for this reactor
			Map<String, Object> additionalMeta = mcpMetaExists ? mcpMetadataList.get(i) : new HashMap<>();
			boolean hasMethodMeta = meta.has(MCPUtility.SMSS_MCP_EXECUTION);

			// execution mode: mcpMetadata overrides getMcpToolMetadata(), which overrides
			// default
			if (additionalMeta.containsKey(MCPUtility.SMSS_MCP_EXECUTION)) {
				String execModeInput = (String) additionalMeta.get(MCPUtility.SMSS_MCP_EXECUTION);
				MCPExecution execModeEnum = MCPExecution.fromValue(execModeInput);
				if (execModeEnum == null && !execModeInput.isBlank()) {
					throw new IllegalArgumentException(MCPUtility.SMSS_MCP_EXECUTION + " can only be a value of: "
							+ Arrays.toString(MCPExecution.values()));
				}
				if (execModeEnum != null) {
					meta.put(MCPUtility.SMSS_MCP_EXECUTION, execModeEnum.getValue());
				} else {
					meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
					classLogger.warn("Invalid SMSS_MCP_EXECUTION value '{}' for reactor '{}'; falling back to 'auto'.",
							execModeInput, reactorNames.get(i));
				}
			} else if (!hasMethodMeta) {
				meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
			}

			// UI: mcpMetadata overrides getMcpToolMetadata() values
			Map<String, Object> uiMap = new HashMap<>();
			try {
				uiMap = (Map<String, Object>) additionalMeta.getOrDefault(MCPUtility.SMSS_MCP_UI, new HashMap<>());
			} catch (ClassCastException e) {
				classLogger.error("Invalid type for SMSS_MCP_UI in reactor '{}'; expected a map of key-value pairs.",
						reactorNames.get(i));
			}

			if (!uiMap.isEmpty()) {
				JSONObject uiJson = new JSONObject();
				if (uiMap.containsKey(MCPUtility.UI_RESOURCE_URI)) {
					uiJson.put(MCPUtility.UI_RESOURCE_URI, uiMap.get(MCPUtility.UI_RESOURCE_URI));
				}
				if (uiMap.containsKey(MCPUtility.UI_LOADING_MESSAGE)) {
					uiJson.put(MCPUtility.UI_LOADING_MESSAGE, uiMap.get(MCPUtility.UI_LOADING_MESSAGE));
				}
				if (uiMap.containsKey(MCPUtility.UI_DISPLAY_LOCATION)) {
					String displayLocation = (String) uiMap.getOrDefault(MCPUtility.UI_DISPLAY_LOCATION, null);
					MCPDisplayOption displayEnum = MCPDisplayOption.fromValue(displayLocation);
					if (displayEnum == null && !displayLocation.isBlank()) {
						throw new IllegalArgumentException(MCPUtility.UI_DISPLAY_LOCATION + " can only be a value of: "
								+ Arrays.toString(MCPDisplayOption.values()));
					}
					String displayString = (displayEnum != null) ? displayEnum.getValue() : null;
					uiJson.put(MCPUtility.UI_DISPLAY_LOCATION, displayString);
				}
				meta.put(MCPUtility.SMSS_MCP_UI, uiJson);
			} else if (!meta.has(MCPUtility.SMSS_MCP_UI)) {
				meta.put(MCPUtility.SMSS_MCP_UI, new JSONObject());
			}

			reactorTool.put("_meta", meta);
			toolsArray.put(reactorTool);
			addedReactorNames.add(functionName);
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
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/mcp/pixel_mcp.json");

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

	/**
	 * Checks if a reactor's package matches any of the requested package prefixes.
	 * Matches the exact package or any sub-package.
	 * 
	 * @param reactorPackage
	 * @param packageNames
	 * @return
	 */
	private boolean matchesPackage(String reactorPackage, List<String> packageNames) {
		for (String pkg : packageNames) {
			if (reactorPackage.equals(pkg) || reactorPackage.startsWith(pkg + ".")) {
				return true;
			}
		}
		return false;
	}

}
