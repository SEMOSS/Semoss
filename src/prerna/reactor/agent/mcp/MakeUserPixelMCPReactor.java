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

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Generates MCP tools from named reactors into a definition file in the user's
 * own asset folder.
 *
 * <p>
 * Builds the same tools {@link MakeRoomPixelMCPReactor} does and merges them
 * into the file named, leaving tools other generators wrote there in place. The
 * file belongs to the user rather than to a room or a project, so a client can
 * keep one set of tools for the user and copy it into each of the user's rooms:
 * the playground keeps the user's connectors this way.
 *
 * <p>
 * The caller may name the generator the tools are stamped with, so that the
 * copies it makes can be told apart from every other tool in a room's file.
 */
public class MakeUserPixelMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeUserPixelMCPReactor.class);

	/** Stamped into the tools when the caller names no generator of its own. */
	private static final String DEFAULT_GENERATOR_ID = "MakeUserPixelMCP";

	/** The generator whose tools this run writes and may replace. */
	private static final String GENERATOR = "generator";

	public MakeUserPixelMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.REACTOR.getKey(),
				ReactorKeysEnum.MCP_METADATA.getKey(), GENERATOR };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		IProject project = user.getAssetProject();
		if (project == null) {
			throw new IllegalArgumentException("Unable to find user asset app");
		}

		String filePath = toRelativeJsonPath(this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
		String generatorId = this.keyValue.get(GENERATOR);
		if (generatorId == null || generatorId.isBlank()) {
			generatorId = DEFAULT_GENERATOR_ID;
		}

		// naming no reactors writes none, which removes this generator's tools
		List<String> reactorNames = getNounAsStringList(ReactorKeysEnum.REACTOR.getKey());
		List<Map<String, Object>> mcpMetadataList = getList(ReactorKeysEnum.MCP_METADATA.getKey());
		JSONArray toolsArray = reactorNames == null || reactorNames.isEmpty() ? new JSONArray()
				: PixelMCPToolBuilder.buildTools(this.insight, null, reactorNames, null, mcpMetadataList);

		// stamp first: ownership decides which existing tools the merge may replace
		MCPUtility.stampGenerator(toolsArray, generatorId);

		String assetFolder = AssetUtility.getUserAssetFolder(project.getProjectName(), project.getProjectId());
		JSONArray mergedTools = MCPUtility.mergeGeneratedTools(MCPUtility.readMcpJson(assetFolder + "/" + filePath),
				toolsArray, generatorId, true);
		JSONObject mcpJson = PixelMCPToolBuilder.wrapMcpJson(mergedTools);
		FileSystemUtil.saveAssetFiles(assetFolder, List.of(filePath), List.of(mcpJson.toString(4)));

		// recorded and synchronized the way saving any user asset is
		String gitFolder = AssetUtility.getUserAssetVersionFolder(project.getProjectName(), project.getProjectId());
		GitRepoUtils.addSpecificFiles(gitFolder, List.of(Constants.ASSETS_FOLDER + DIR_SEPARATOR + filePath));
		GitRepoUtils.commitAddedFiles(gitFolder, "add: generated " + filePath, user);
		ClusterUtil.pushUserAsset(project.getProjectId());

		classLogger.info("Saved user MCP to {}/{} ({} tool(s) generated, {} other tool(s) preserved)", assetFolder,
				filePath, toolsArray.length(), mergedTools.length() - toolsArray.length());
		// a map, which the pixel response writes out as the file's JSON
		return new NounMetadata(mcpJson.toMap(), PixelDataType.MAP);
	}

	/**
	 * The file's path within the user's folder, checked to name a JSON file inside
	 * it.
	 *
	 * @param filePath the path as passed
	 * @return the path, relative to the user's asset folder
	 */
	private static String toRelativeJsonPath(String filePath) {
		String normalized = filePath == null ? null : Utility.normalizePath(filePath.trim());
		while (normalized != null && normalized.startsWith("/")) {
			normalized = normalized.substring(1);
		}
		if (normalized == null || normalized.isEmpty() || !normalized.toLowerCase(Locale.ROOT).endsWith(".json")) {
			throw new IllegalArgumentException(
					"Name a .json file in the user's folder with '" + ReactorKeysEnum.FILE_PATH.getKey() + "'.");
		}
		return normalized;
	}

	@Override
	public String getReactorDescription() {
		return """
				Generates an MCP definition file in the user's own asset folder from the reactors named, \
				merging them with the tools other generators wrote to that file. Clients keep a set of \
				tools for the user this way and copy it into the user's rooms.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The .json file to write, relative to the user's asset folder, such as mcp/playground_connector_mcp.json";
		} else if (key.equals(ReactorKeysEnum.REACTOR.getKey())) {
			return "The reactors to turn into mcp tools. Naming none removes the tools this generator wrote before.";
		} else if (key.equals(GENERATOR)) {
			return "The generator the tools are stamped with as SMSS_MCP_GENERATOR. A run replaces only that "
					+ "generator's tools. Defaults to " + DEFAULT_GENERATOR_ID + ".";
		}
		return super.getDescriptionForKey(key);
	}

}
