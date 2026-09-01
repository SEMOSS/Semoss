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

import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.ReactorFactory;
import prerna.reactor.agent.mcp.MCPUtility.MCPDisplayOption;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Turns a project's reactors into MCP tool definitions.
 *
 * <p>
 * Shared by {@link MakePixelMCPReactor}, which writes the result into the
 * project's assets, and {@link MakeRoomPixelMCPReactor}, which writes it into a
 * room folder. Holding the tool shape in one place is what keeps the two scopes
 * from drifting apart, the same way {@code PlaywrightMCPToolBuilder} serves its
 * project and room reactors.
 */
public class PixelMCPToolBuilder {

	private static final Logger classLogger = LogManager.getLogger(PixelMCPToolBuilder.class);

	private PixelMCPToolBuilder() {
	}

	/**
	 * Whether a run rebuilds every tool the project can produce. With no reactor
	 * and no package named, the whole project is scanned, so a previously generated
	 * tool that is absent afterwards was genuinely dropped. A run narrowed to named
	 * reactors or packages must not prune what it never looked at, so callers pass
	 * this to the merge.
	 *
	 * @param reactorNames explicitly requested reactors, possibly null or empty
	 * @param packageNames requested package prefixes, possibly null or empty
	 * @return whether every reactor was considered
	 */
	public static boolean isFullScan(List<String> reactorNames, List<String> packageNames) {
		return (reactorNames == null || reactorNames.isEmpty()) && (packageNames == null || packageNames.isEmpty());
	}

	/**
	 * Builds the tool definitions for a run.
	 *
	 * <p>
	 * Reactors found by scanning contribute the metadata they declare. Reactors
	 * named explicitly may additionally be given per-tool metadata, which overrides
	 * what the reactor declares for itself.
	 *
	 * <p>
	 * Scanning reads a project's compiled reactors, so it only happens when a
	 * project is supplied. Without one the caller has to name what it wants, which
	 * is the case for any scope where a project context is absent or can change
	 * under the caller.
	 *
	 * @param insight         insight the named reactors are resolved against
	 * @param project         project whose reactors are scanned, or null to build
	 *                        only the named reactors
	 * @param reactorNames    reactors to include by name, possibly null or empty
	 * @param packageNames    package prefixes to scan, possibly null or empty
	 * @param mcpMetadataList per-reactor metadata, positionally matched to
	 *                        reactorNames, or null when none was supplied
	 * @return the tool definitions, unstamped
	 */
	public static JSONArray buildTools(Insight insight, IProject project, List<String> reactorNames,
			List<String> packageNames, List<Map<String, Object>> mcpMetadataList) {
		List<String> requestedReactors = reactorNames == null ? new ArrayList<>() : reactorNames;
		boolean mcpMetaExists = mcpMetadataList != null;
		if (mcpMetaExists && mcpMetadataList.size() != requestedReactors.size()) {
			throw new IllegalArgumentException("The number of " + ReactorKeysEnum.MCP_METADATA.getKey()
					+ " entries must match the number of REACTOR entries.");
		}

		boolean hasPackages = packageNames != null && !packageNames.isEmpty();
		if (project == null && hasPackages) {
			throw new IllegalArgumentException(
					"Scanning by package needs a project to read the reactors from; name the reactors instead.");
		}
		boolean scanAll = isFullScan(requestedReactors, packageNames);

		JSONArray toolsArray = new JSONArray();
		// Track reactor names already added to avoid duplicates when both scan and
		// reactor are provided
		Set<String> addedReactorNames = new LinkedHashSet<>();

		// Phase 1: Scan for reactors that override getMcpToolMetadata()
		// If neither reactor nor package is provided, scans every reactor in the app
		// If package is provided, filters by package prefix
		if (project != null && (scanAll || hasPackages)) {
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
					String addedToolName = uniqueToolName(functionName, addedReactorNames);
					// update the toolname
					// this works because we use the {@code MCPUtility.SMSS_FUNCTION_NAME} in meta
					// for the actual reactor to run
					if (!functionName.equals(addedToolName)) {
						reactorTool.put("name", addedToolName);
					}

					toolsArray.put(reactorTool);
					addedReactorNames.add(functionName);
				}
			}
		}

		// Phase 2: Process explicitly listed reactors
		for (int i = 0; i < requestedReactors.size(); i++) {
			String reactorName = requestedReactors.get(i);
			IReactor thisReactor = ReactorFactory.getReactor(insight, reactorName, null, insight.getCurFrame());
			JSONObject reactorTool = thisReactor.asMcpTool();
			String functionName = reactorTool.getString("name");

			// if we encounter the same reactor name
			// we should make it unique
			String addedToolName = uniqueToolName(functionName, addedReactorNames);
			if (!functionName.equals(addedToolName)) {
				reactorTool.put("name", addedToolName);
			}

			JSONObject meta = reactorTool.optJSONObject("_meta");
			if (meta == null) {
				meta = new JSONObject();
			}
			meta.put(MCPUtility.SMSS_FUNCTION_NAME, functionName);

			// Determine if explicit mcpMetadata was provided for this reactor
			Map<String, Object> additionalMeta = mcpMetaExists ? mcpMetadataList.get(i) : new HashMap<>();
			applyExecutionMode(meta, additionalMeta, reactorName);
			applyUiMetadata(meta, additionalMeta, reactorName);

			reactorTool.put("_meta", meta);
			toolsArray.put(reactorTool);
			addedReactorNames.add(functionName);
		}

		return toolsArray;
	}

	/**
	 * Wraps tool definitions in the object shape written to a pixel_mcp.json.
	 *
	 * @param tools tool definitions to write
	 * @return the file's root object
	 */
	public static JSONObject wrapMcpJson(JSONArray tools) {
		JSONObject mcpJson = new JSONObject();
		mcpJson.put("tools", tools);
		JSONObject meta = new JSONObject();
		LocalDate todayUTC = LocalDate.now(ZoneOffset.UTC);
		meta.put("last_modified_date", todayUTC.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
		mcpJson.put("_meta", meta);
		return mcpJson;
	}

	/**
	 * A tool name not already taken in this run, since subpackages may hold
	 * reactors that share a simple name.
	 *
	 * @param functionName name the reactor asks for
	 * @param taken        names already used
	 * @return the name to write
	 */
	private static String uniqueToolName(String functionName, Set<String> taken) {
		String candidate = functionName;
		int counter = 1;
		while (taken.contains(candidate)) {
			candidate = functionName + (++counter);
		}
		return candidate;
	}

	/**
	 * Sets how the tool runs. Supplied metadata overrides what the reactor
	 * declares, which overrides the default.
	 *
	 * @param meta           the tool's metadata being assembled
	 * @param additionalMeta metadata supplied for this reactor
	 * @param reactorName    reactor being described, for logging
	 */
	private static void applyExecutionMode(JSONObject meta, Map<String, Object> additionalMeta, String reactorName) {
		boolean hasMethodMeta = meta.has(MCPUtility.SMSS_MCP_EXECUTION);
		if (!additionalMeta.containsKey(MCPUtility.SMSS_MCP_EXECUTION)) {
			if (!hasMethodMeta) {
				meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
			}
			return;
		}

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
					execModeInput, reactorName);
		}
	}

	/**
	 * Sets how the tool presents itself. Supplied metadata overrides what the
	 * reactor declares.
	 *
	 * @param meta           the tool's metadata being assembled
	 * @param additionalMeta metadata supplied for this reactor
	 * @param reactorName    reactor being described, for logging
	 */
	@SuppressWarnings("unchecked")
	private static void applyUiMetadata(JSONObject meta, Map<String, Object> additionalMeta, String reactorName) {
		Map<String, Object> uiMap = new HashMap<>();
		try {
			uiMap = (Map<String, Object>) additionalMeta.getOrDefault(MCPUtility.SMSS_MCP_UI, new HashMap<>());
		} catch (ClassCastException e) {
			classLogger.error("Invalid type for SMSS_MCP_UI in reactor '{}'; expected a map of key-value pairs.",
					reactorName);
		}

		if (uiMap.isEmpty()) {
			if (!meta.has(MCPUtility.SMSS_MCP_UI)) {
				meta.put(MCPUtility.SMSS_MCP_UI, new JSONObject());
			}
			return;
		}

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
			uiJson.put(MCPUtility.UI_DISPLAY_LOCATION, displayEnum != null ? displayEnum.getValue() : null);
		}
		meta.put(MCPUtility.SMSS_MCP_UI, uiJson);
	}

	/**
	 * Checks if a reactor's package matches any of the requested package prefixes.
	 * Matches the exact package or any sub-package.
	 *
	 * @param reactorPackage package the reactor class lives in
	 * @param packageNames   requested prefixes
	 * @return whether the reactor should be included
	 */
	private static boolean matchesPackage(String reactorPackage, List<String> packageNames) {
		for (String pkg : packageNames) {
			if (reactorPackage.equals(pkg) || reactorPackage.startsWith(pkg + ".")) {
				return true;
			}
		}
		return false;
	}

}
