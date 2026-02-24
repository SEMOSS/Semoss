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
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossMCPException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public final class MCPUtility {

	private static final Logger classLogger = LogManager.getLogger(MCPUtility.class);

	private static final Gson GSON = GsonUtility.getDefaultGson();

	public static final String SMSS_ENGINE_ID = "SMSS_ENGINE_ID";
	public static final String SMSS_ENGINE_NAME = "SMSS_ENGINE_NAME";
	public static final String SMSS_ENGINE_TYPE = "SMSS_ENGINE_TYPE";
	public static final String SMSS_MCP_EXECUTION = "SMSS_MCP_EXECUTION";
	public static final String SMSS_FUNCTION_NAME = "SMSS_FUNCTION_NAME";
	public static final String SMSS_MCP_UI = "SMSS_MCP_UI";
	public static final String UI_RESOURCE_URI = "resourceURI";
	public static final String UI_LOADING_MESSAGE = "loadingMessage";
	public static final String UI_DISPLAY_LOCATION = "displayLocation";

	@Deprecated
	public static final String SMSS_PROJECT_ID = "SMSS_PROJECT_ID";
	@Deprecated
	public static final String SMSS_PROJECT_NAME = "SMSS_PROJECT_NAME";

	public static final String MCP_PY_FILE_NAME = "mcp_driver.py";
	public static final String MCP_NOTEBOOK_NAME = "mcp_driver";

	@Deprecated
	public static final String LEGACY_PY_FILE_NAME = "smss_driver.py";
	@Deprecated
	public static final String LEGACY_MCP_NOTEBOOK_NAME = "smss_driver";

	// Regex pattern for "a" + UUID + "_"
	// UUID format: 8-4-4-4-12 hexadecimal digits
	private static final Pattern UUID_PREFIX_PATTERN = Pattern
			.compile("^a[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}_");

	public enum MCPExecution {
		AUTO("auto"), ASK("ask"), DISABLED("disabled");

		private final String value;

		MCPExecution(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		public static MCPExecution fromValue(String value) {
			for (MCPExecution exec : values()) {
				if (exec.getValue().equalsIgnoreCase(value)) {
					return exec;
				}
			}
			return null;
		}
	}

	public enum MCPDisplayOption {
		INLINE("inline"), SIDEBAR("sidebar"), HIDDEN("hidden");

		private final String value;

		MCPDisplayOption(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		public static MCPDisplayOption fromValue(String value) {
			for (MCPDisplayOption option : values()) {
				if (option.getValue().equalsIgnoreCase(value)) {
					return option;
				}
			}
			return null;
		}
	}

	/**
	 * Run a python mcp tool
	 * 
	 * @param project
	 * @param insight
	 * @param functionName
	 * @param functionProperties
	 * @param paramMap
	 * @return
	 */
	public static String runPythonTool(IEngine engine, Insight insight, String functionName,
			JSONObject functionProperties, Map<String, Object> paramMap) {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());

		// load the path to have access to the file
		String pyFolderLoc = assetsFolder + "/py";
		pyFolderLoc = pyFolderLoc.replace("\\", "/");
		boolean namedMCP = true;
		{
			File mcpDriver = new File(pyFolderLoc + "/" + MCP_PY_FILE_NAME);
			if (!mcpDriver.exists()) {
				namedMCP = false;
			}
		}

		String moduleName = namedMCP ? "mcp_driver" : "smss_driver";

		// clear the cached modules and reimport to get latest file changes
		// @formatter:off
		String loadFreshSmssModule = 
			    "if 'reload_mcp_function' in globals():\n" +
			    "    mcp_driver = reload_mcp_function()\n" +
			    "else:\n" +
			    "    import " + moduleName + " as mcp_driver";
		
		// @formatter:on
		// Copy default path vars from translator globals into the loaded MCP module.
		// These vars are injected into the translator scope, not the module scope.
		// @formatter:off
		String injectDefaultVars = "for _k in ['ROOT', 'APP_ROOT', 'USER_ROOT']:\n" +
		                          "    if _k in globals():\n" +
		                          "        setattr(mcp_driver, _k, globals()[_k])";
		// @formatter:on

		if (!namedMCP) {
			classLogger.warn("Using legacy {} python file name - needs to be updated to {}", LEGACY_PY_FILE_NAME,
					MCP_PY_FILE_NAME);
		}

		// iterate function properties and find if it is string etc.
		Iterator<String> props = functionProperties.keys();
		StringBuilder paramString = new StringBuilder();
		while (props.hasNext()) {
			if (paramString.length() != 0) {
				paramString.append(", ");
			}
			String propName = props.next();
			JSONObject thisProp = ((JSONObject) functionProperties.get(propName));
			String propType = thisProp.getString("type");
			Object propValue = null;

			// get the value
			if (paramMap != null && paramMap.containsKey(propName)) {
				propValue = paramMap.get(propName);
			} else if (thisProp.has("default")) {
				// get the default value
				propValue = thisProp.getString("default");
			} else {
				// PyUtils.determineStringType(propValue) will turn this to None w/o quotes
				// around it
				propValue = null;
			}
			// while we do have the type, the propValue is much better at sending
			// appropriate python syntax
			paramString.append(propName).append("=").append(PyUtils.determineStringType(propValue));
		}

		PyTranslator pyt = null;
		if (engine instanceof IProject) {
			// just in case a SetContext/LoadApp was not called
			insight.setContext(engine.getEngineId());
			insight.setContextProjectName(engine.getEngineName());

			String pyEngine = "user";
			if (engine.getSmssProp().containsKey(Constants.USE_PYTHON)) {
				pyEngine = engine.getSmssProp().get(Constants.USE_PYTHON) + "";
			}
			if (pyEngine.equalsIgnoreCase("project")) {
				pyt = ((IProject) engine).getProjectPyTranslator();
			}
		}
		if (pyt == null) {
			pyt = insight.getPyTranslator();
		}

		String runMethod = "mcp_driver." + functionName + "(" + paramString + ")";
		classLogger.info("Running python tool '{}' from {} engine '{}'", runMethod, engine.getCatalogType(),
				engine.getEngineId());

		// reload the module
		pyt.runScript(insight, loadFreshSmssModule);
		// inject default vars into module scope
		pyt.runScript(insight, injectDefaultVars);
		// run method
		return stringifyMcpResult(pyt.runScript(insight, runMethod));
	}

	/**
	 * Run a pixel mcp tool
	 * 
	 * @param project
	 * @param insight
	 * @param functionName
	 * @param functionProperties
	 * @param paramMap
	 * @return
	 */
	public static String runPixelTool(IEngine engine, Insight insight, String functionName,
			JSONObject functionProperties, Map<String, Object> paramMap) {
		// iterate function properties and find if it is string etc.
		Iterator<String> props = functionProperties.keys();
		StringBuilder paramString = new StringBuilder();
		while (props.hasNext()) {
			String propName = props.next();
			JSONObject thisProp = ((JSONObject) functionProperties.get(propName));
			String propType = thisProp.getString("type");
			Object propValue = null;

			// get the value
			if (paramMap != null && paramMap.containsKey(propName)) {
				propValue = paramMap.get(propName);
			} else if (thisProp.has("default")) {
				// get the default value
				propValue = thisProp.getString("default");
			}
			// if we have a value, add it
			if (propValue != null) {
				// we have confirmed we have a new value to add
				// check if we need to comma separate
				if (paramString.length() != 0) {
					paramString.append(", ");
				}

				paramString.append(propName).append("=");

				// handle json by simple tostring
				if (propValue instanceof JSONObject || propValue instanceof JSONArray) {
					paramString.append(propValue.toString());
				} else {
					// use GSON
					paramString.append(GSON.toJson(propValue));
				}
			}
		}

		if (engine instanceof IProject) {
			// just in case a SetContext/LoadApp was not called
			insight.setContext(engine.getEngineId());
			insight.setContextProjectName(engine.getEngineName());
		}

		String runMethod = functionName + "(" + paramString + ");";
		if (engine != null) {
			classLogger.info("Running pixel tool '{}' from {} engine '{}'", runMethod, engine.getCatalogType(),
					engine.getEngineId());
		} else {
			classLogger.info("Running pixel tool '{}' directly without an engine", runMethod);
		}
		// run pixel
		PixelRunner pixelReturn = insight.runPixel(runMethod);
		NounMetadata result = pixelReturn.getResults().get(0);
		if (result.getOpType().contains(PixelOperationType.ERROR)) {
			throw new SemossMCPException(result.getValue() + "", MCPErrorCode.SERVER_ERROR);
		}
		return stringifyMcpResult(result.getValue());
	}

	/**
	 * Return the tool output in the proper string representation
	 * 
	 * @param value
	 * @return
	 */
	private static String stringifyMcpResult(Object value) {
		// toString method properly handles this already
		if (value instanceof org.json.JSONObject || value instanceof org.json.JSONArray
				|| value instanceof com.google.gson.JsonElement) {
			return value.toString();
		}

		return GSON.toJson(value);
	}

	/**
	 * 
	 * @param engineId
	 * @param jsonToolsMap
	 * @return
	 */
	public static JSONObject appendEngineIdToToolsMethodName(String engineId, JSONObject jsonToolsMap) {
		if (jsonToolsMap == null || !jsonToolsMap.has("tools")) {
			return jsonToolsMap;
		}

		JSONArray toolsArray = jsonToolsMap.getJSONArray("tools");
		for (int i = 0; i < toolsArray.length(); i++) {
			JSONObject toolMap = toolsArray.getJSONObject(i);
			String currentName = toolMap.getString("name");
			toolMap.put("name", "a" + engineId + "_" + currentName);
		}
		return jsonToolsMap;
	}

	/**
	 * 
	 * @param engineId
	 * @param functionName
	 * @return
	 */
	public static String removeEngineIdFromToolsMethodName(String engineId, String functionName) {
		String internalFunctionNamePrefix = "a" + engineId + "_";
		if (functionName.startsWith(internalFunctionNamePrefix)) {
			return functionName.replaceFirst(internalFunctionNamePrefix, "");
		}
		return functionName;
	}

	/**
	 * Parses the "a" + project id UUID + "_" prefix from a string
	 * 
	 * @param input the input string
	 * @return String array [prefix, remainingString] if prefix found, null
	 *         otherwise
	 */
	private static String[] parseEngineIdFromFunctionName(String input) {
		if (input == null) {
			return null;
		}

		Matcher matcher = UUID_PREFIX_PATTERN.matcher(input);
		if (matcher.find()) {
			String prefix = matcher.group();
			// remove the "a" and the "_" after the project id
			prefix = prefix.substring(1, prefix.length() - 1);
			String remaining = input.substring(matcher.end());
			return new String[] { prefix, remaining };
		}

		return null;
	}

	/**
	 * Updates the tool response with information regarding the tool
	 * 
	 * @param response
	 * @return
	 */
	public static void updateToolResponseWithProjectMeta(ResponseMessage response) {
		updateToolResponseWithProjectMeta(response, null);
	}

	/**
	 * Updates the tool response with information regarding the tool. Uses a map for
	 * an intermediary cache during the process in case we are iterating through
	 * numerous response messages
	 * 
	 * @param response
	 * @param mcpToolsJsonCache
	 * @return
	 */
	public static void updateToolResponseWithProjectMeta(ResponseMessage response,
			Map<String, JSONObject> mcpToolsJsonCache) {
		if (mcpToolsJsonCache == null) {
			mcpToolsJsonCache = new HashMap<>();
		}
		List<Map<String, Object>> toolResponses = response.getToolResponses();
		for (int toolResponseIndex = 0; toolResponseIndex < toolResponses.size(); toolResponseIndex++) {
			Map<String, Object> responseToolMap = toolResponses.get(toolResponseIndex);
			// we start the function name with _projectid_ so lets remove that
			String responseProjectIdToolFunctionName = (String) responseToolMap.get("name");
			String[] responseProjectIdToolFunctionNameSplit = parseEngineIdFromFunctionName(
					responseProjectIdToolFunctionName);
			if (responseProjectIdToolFunctionNameSplit == null) {
				// if the tool function doesn't start with _projectid_
				// then this response is already in proper format for the FE
				continue;
			}
			String engineId = responseProjectIdToolFunctionNameSplit[0];
			String origFunctionName = responseProjectIdToolFunctionNameSplit[1];

			// now that we have the projectId
			// lets append some of the mcp metadata back into the response

			JSONObject mcpToolsJson = mcpToolsJsonCache.get(engineId);
			if (mcpToolsJson == null) {
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
					// technically speaking you could have a function start with _
					// but will assume this is in proper format
					continue;
				}
				mcpToolsJson = MCPUtility.getAggregatedTools(engine);
				mcpToolsJsonCache.put(engineId, mcpToolsJson);
			}

			if (mcpToolsJson != null) {
				JSONArray mcpToolsArray = mcpToolsJson.getJSONArray("tools");
				JSONObject mcpTool = null;
				PROJECT_MCP_LOOP: for (int toolIndex = 0; toolIndex < mcpToolsArray.length(); toolIndex++) {
					JSONObject _tool = mcpToolsArray.getJSONObject(toolIndex);
					if (_tool.has("name") && _tool.getString("name").equals(origFunctionName)) {
						mcpTool = _tool;
						break PROJECT_MCP_LOOP;
					}
				}
				responseToolMap.put("_tool_found", true);
				responseToolMap.put("original_name", origFunctionName);

				// add back the title from mcp structure
				if (mcpTool != null && mcpTool.has("title")) {
					responseToolMap.put("title", mcpTool.getString("title"));
				}

				if (mcpTool != null && mcpTool.has("description")) {
					responseToolMap.put("description", mcpTool.getString("description"));
				}

				if (mcpToolsJson.has("_meta")) {
					responseToolMap.put("_meta", mcpToolsJson.getJSONObject("_meta").toMap());
				}

				Map<String, Object> currentMeta = (Map<String, Object>) responseToolMap.get("_meta");
				if (currentMeta == null) {
					currentMeta = new HashMap<>();
					responseToolMap.put("_meta", currentMeta);
				}

				// Add additional MCP metadata
				if (mcpTool != null && mcpTool.has("_meta")) {
					JSONObject toolMeta = mcpTool.getJSONObject("_meta");

					// Add SMSS_MCP_EXECUTION
					String mcpExecution = getValidMcpExecution(toolMeta);
					currentMeta.put(SMSS_MCP_EXECUTION, mcpExecution);

					// Add SMSS_MCP_UI
					JSONObject uiMeta = getValidMcpUI(toolMeta);
					if (uiMeta != null) {
						currentMeta.put(SMSS_MCP_UI, uiMeta.toMap());
					}
				}

			} else {
				responseToolMap.put("_tool_found", false);
			}
		}
	}

	/**
	 * 
	 * @param toolStep
	 */
	public static void updateCOTToolStepWithEngineMeta(Map<String, Object> toolStep) {
		Map<String, JSONObject> mcpToolsJsonCache = new HashMap<>();
		Map<String, Object> toolDetails = (Map<String, Object>) toolStep.get("details");
		if (toolDetails == null) {
			return;
		}
		String responseProjectIdToolFunctionName = (String) toolDetails.get("tool_name");
		String[] responseProjectIdToolFunctionNameSplit = parseEngineIdFromFunctionName(
				responseProjectIdToolFunctionName);
		if (responseProjectIdToolFunctionNameSplit == null) {
			// if the tool function doesn't start with _projectid_
			// then this response is already in proper format for the FE
			return;
		}
		String engineId = responseProjectIdToolFunctionNameSplit[0];
		String origFunctionName = responseProjectIdToolFunctionNameSplit[1];

		// now that we have the projectId
		// lets append some of the mcp metadata back into the response

		JSONObject mcpToolsJson = mcpToolsJsonCache.get(engineId);
		if (mcpToolsJson == null) {
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
				// technically speaking you could have a function start with _
				// but will assume this is in proper format
				return;
			}
			mcpToolsJson = MCPUtility.getAggregatedTools(engine);
			mcpToolsJsonCache.put(engineId, mcpToolsJson);
		}

		if (mcpToolsJson != null) {
			JSONArray mcpToolsArray = mcpToolsJson.getJSONArray("tools");
			JSONObject mcpTool = null;
			PROJECT_MCP_LOOP: for (int toolIndex = 0; toolIndex < mcpToolsArray.length(); toolIndex++) {
				JSONObject _tool = mcpToolsArray.getJSONObject(toolIndex);
				if (_tool.has("name") && _tool.getString("name").equals(origFunctionName)) {
					mcpTool = _tool;
					break PROJECT_MCP_LOOP;
				}
			}

			// add back the title from mcp structure
			if (mcpTool != null && mcpTool.has("title")) {
				toolDetails.put("title", mcpTool.getString("title"));
			}

			if (mcpToolsJson.has("_meta")) {
				toolDetails.put("_meta", mcpToolsJson.get("_meta"));
			}
		}
	}

	/**
	 * Converts camelCase, PascalCase, or snake_case strings to title case with
	 * spaces Useful for pretty version of name -> title in MCP Tool schema
	 * 
	 * @param input
	 * @return
	 */
	public static String formatToTitleCase(String input) {
		if (input == null || input.isEmpty()) {
			return input;
		}

		StringBuilder result = new StringBuilder();
		boolean capitalizeNext = true; // Capitalize the first letter

		for (int i = 0; i < input.length(); i++) {
			char currentChar = input.charAt(i);

			// Handle underscores - replace with space and capitalize next letter
			if (currentChar == '_') {
				result.append(' ');
				capitalizeNext = true;
				continue;
			}

			// Add space before uppercase letters (except the first character)
			if (i > 0 && Character.isUpperCase(currentChar) && result.charAt(result.length() - 1) != ' ') {
				// Check if previous character is lowercase or if next character is lowercase
				// This handles cases like "XMLParser" -> "XML Parser" correctly
				char prevChar = input.charAt(i - 1);
				boolean prevIsLower = Character.isLowerCase(prevChar);
				boolean nextIsLower = (i + 1 < input.length()) && Character.isLowerCase(input.charAt(i + 1));

				if (prevIsLower || nextIsLower) {
					result.append(' ');
					capitalizeNext = true;
				}
			}

			// Apply capitalization logic
			if (capitalizeNext) {
				result.append(Character.toUpperCase(currentChar));
				capitalizeNext = false;
			} else {
				result.append(currentChar);
			}
		}

		return result.toString();
	}

	/**
	 * 
	 * @param project
	 * @return
	 */
	public static JSONObject getAggregatedTools(IEngine engine) {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String pythonJsonFileLoc = assetsFolder + "/mcp/py_mcp.json";
		String pixelJsonFileLoc = assetsFolder + "/mcp/pixel_mcp.json";

		JSONObject toolMap = new JSONObject();
		JSONArray toolsArray = new JSONArray();
		toolsArray.putAll(MCPUtility.getNode(pythonJsonFileLoc, "tools"));
		toolsArray.putAll(MCPUtility.getNode(pixelJsonFileLoc, "tools"));
		toolMap.put("tools", toolsArray);

		// add in meta as well
		JSONObject _meta = new JSONObject();
		_meta.put(MCPUtility.SMSS_PROJECT_ID, engine.getEngineId());
		_meta.put(MCPUtility.SMSS_PROJECT_NAME, engine.getEngineName());
		_meta.put(MCPUtility.SMSS_ENGINE_ID, engine.getEngineId());
		_meta.put(MCPUtility.SMSS_ENGINE_NAME, engine.getEngineName());
		_meta.put(MCPUtility.SMSS_ENGINE_TYPE, engine.getCatalogType().name());
		toolMap.put("_meta", _meta);

		return toolMap;
	}

	/**
	 * Get the current entire json tool generated from a current notebook cell id
	 * 
	 * @param project
	 * @param cellId
	 * @return
	 */
	public static JSONObject findPythonToolWithCellId(IEngine engine, String cellId) {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String pythonJsonFileLoc = assetsFolder + "/mcp/py_mcp.json";

		JSONArray existingTools = MCPUtility.getNode(pythonJsonFileLoc, "tools");
		for (int i = 0; i < existingTools.length(); i++) {
			JSONObject toolObject = existingTools.getJSONObject(i);
			if (!toolObject.has("_meta")) {
				continue;
			}
			JSONObject toolMeta = toolObject.getJSONObject("_meta");
			if (toolMeta.has("notebook_cell_id")) {
				String toolNotebookCellId = toolMeta.get("notebook_cell_id") + "";
				if (toolNotebookCellId.equals(cellId)) {
					return toolObject;
				}
			}
		}

		return null;
	}

	/**
	 * Remove a specific tool (function) from the py_mcp.json
	 * 
	 * @param engine
	 * @param functionName
	 * @return
	 */
	public static boolean removePythonFunctionFromMCPJson(IEngine engine, String functionName) {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String pythonJsonFileLoc = assetsFolder + "/mcp/py_mcp.json";

		JSONObject mcpJson = MCPUtility.readJsonFile(pythonJsonFileLoc);
		if (!mcpJson.has("tools")) {
			return false;
		}
		boolean found = false;
		JSONArray existingTools = mcpJson.getJSONArray("tools");
		for (int i = 0; i < existingTools.length(); i++) {
			JSONObject toolObject = existingTools.getJSONObject(i);
			if (!toolObject.has("name")) {
				continue;
			}
			String toolName = toolObject.getString("name");
			if (toolName.equals(functionName)) {
				// this is what we want to delete
				existingTools.remove(i);
				found = true;
				break;
			}
		}

		if (found) {
			try (FileWriter writer = new FileWriter(pythonJsonFileLoc)) {
				String prettyJson = mcpJson.toString(4);
				writer.write(prettyJson);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException(
						"Unable to write pixel_mcp.json file. Detailed error = " + e.getMessage());
			}
		}

		return found;
	}

	/**
	 * 
	 * @param jsonFileLoc
	 * @return
	 */
	public static JSONObject readJsonFile(String jsonFileLoc) {
		File jsonFile = new File(jsonFileLoc);
		if (jsonFile.exists()) {
			try {
				String jsonTxt = FileUtils.readFileToString(jsonFile, StandardCharsets.UTF_8);
				JSONObject json = new JSONObject(jsonTxt);
				return json;
			} catch (FileNotFoundException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (JSONException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return new JSONObject();
	}

	/**
	 * 
	 * @param jsonFileLoc
	 * @param node
	 * @return
	 */
	public static JSONArray getNode(String jsonFileLoc, String node) {
		File jsonFile = new File(jsonFileLoc);
		if (jsonFile.exists()) {
			try {
				String jsonTxt = FileUtils.readFileToString(jsonFile, StandardCharsets.UTF_8);
				JSONObject json = new JSONObject(jsonTxt);
				if (json.has(node)) {
					JSONArray toolObj = json.getJSONArray(node);
					return toolObj;
				}
			} catch (FileNotFoundException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (JSONException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return new JSONArray();
	}

	/**
	 * Parse the python code to determine the function name
	 * 
	 * @param insight
	 * @param pythonCode
	 * @return
	 */
	public static String getPythonFunctionNameFromCode(Insight insight, String pythonCode) {
		String encodedCode = Base64.getEncoder().encodeToString(pythonCode.getBytes());

		String script = """
				import smssutil
				import base64
				encoded_code = '%s'
				code = base64.b64decode(encoded_code).decode('utf-8')

				try:
				    result = smssutil.get_function_name_from_code(code)
				except SyntaxError as e:
				    f'SYNTAX_ERROR: {str(e)}'
				except Exception as e:
				    f'ERROR: {str(e)}'

				result
				""".formatted(encodedCode);
		String functionName = (String) insight.getPyTranslator().runDirectPy(script);
		return functionName;
	}

	/**
	 * Parse the python code in a file and get all the function names
	 * 
	 * @param insight
	 * @param pythonCode
	 * @return
	 */
	public static List<String> getAllFunctionsFromPyFile(Insight insight, String filePath) {
		String script = """
				import smssutil
				filePath = '''%s'''
				smssutil.get_all_function_names_from_file(filePath)
				""".formatted(filePath.replace("'", "\\'"));
		List<String> functionNames = (List<String>) insight.getPyTranslator().runDirectPy(script);
		return functionNames;
	}

	/**
	 * Parse the python code in a file to find a file and remove it
	 * 
	 * @param insight
	 * @param filePath
	 * @param functionName
	 * @return
	 */
	public static boolean removeExistingFunctionFromPyFile(Insight insight, String filePath, String functionName) {
		String script = """
				import smssutil
				filePath = '''%s'''
				function_name = '%s'
				smssutil.remove_function_from_file(filePath, function_name)
				""".formatted(filePath.replace("\\", "/"), functionName);
		return (boolean) insight.getPyTranslator().runDirectPy(script);
	}

	/**
	 * 
	 * @param toolMeta
	 * @return
	 */
	private static String getValidMcpExecution(JSONObject toolMeta) {
		if (toolMeta == null) {
			return MCPExecution.ASK.getValue(); // default if _meta missing
		}

		Object val = toolMeta.opt(SMSS_MCP_EXECUTION); // could be null, missing, etc
		String valueString = (val == null || JSONObject.NULL.equals(val)) ? null : val.toString();

		MCPExecution exec = MCPExecution.fromValue(valueString); // null if not a valid enum
		return exec != null ? exec.getValue() : MCPExecution.ASK.getValue();
	}

	/**
	 * 
	 * @param toolMeta
	 * @return
	 */
	private static JSONObject getValidMcpUI(JSONObject toolMeta) {
		if (toolMeta == null) {
			return null;
		}

		Object val = toolMeta.opt(SMSS_MCP_UI); // could be null, missing, etc
		if (val == null || JSONObject.NULL.equals(val) || !(val instanceof JSONObject)) {
			return null;
		}

		JSONObject uiJson = (JSONObject) val;

		// Only add known keys
		String resourceURI = null;
		if (uiJson.has(UI_RESOURCE_URI) && !uiJson.isNull(UI_RESOURCE_URI)) {
			resourceURI = uiJson.getString(UI_RESOURCE_URI);
		}

		String loadingMessage = null;
		if (uiJson.has(UI_LOADING_MESSAGE) && !uiJson.isNull(UI_LOADING_MESSAGE)) {
			loadingMessage = uiJson.getString(UI_LOADING_MESSAGE);
		}

		String displayLocation = null;
		if (uiJson.has(UI_DISPLAY_LOCATION) && !uiJson.isNull(UI_DISPLAY_LOCATION)) {
			displayLocation = uiJson.getString(UI_DISPLAY_LOCATION);
		}

		JSONObject validUiJson = new JSONObject();
		if (resourceURI != null) {
			validUiJson.put(UI_RESOURCE_URI, resourceURI);
		}
		if (loadingMessage != null) {
			validUiJson.put(UI_LOADING_MESSAGE, loadingMessage);
		}
		if (displayLocation != null) {
			MCPDisplayOption displayEnum = MCPDisplayOption.fromValue(displayLocation);
			String displayString = (displayEnum != null) ? displayEnum.getValue() : null;
			validUiJson.put(UI_DISPLAY_LOCATION, displayString);
		}

		return validUiJson;
	}

	/**
	 * Add the MCP tag to an existing engine (engine and project)
	 * 
	 * @param engine
	 */
	public static void addMCPTag(IEngine engine) {
		Map<String, Object> metadata = null;
		boolean isProject = engine.getCatalogType() == IEngine.CATALOG_TYPE.PROJECT;
		if (isProject) {
			metadata = SecurityProjectUtils.getAggregateProjectMetadata(engine.getEngineId(), Arrays.asList("tag"),
					false);
		} else {
			metadata = SecurityEngineUtils.getAggregateEngineMetadata(engine.getEngineId(), Arrays.asList("tag"),
					false);
		}
		List<Object> tags = new ArrayList<>();
		if (metadata.containsKey("tag")) {
			Object curTags = metadata.get("tag");
			if (curTags instanceof List) {
				tags.addAll((List) curTags);
			} else {
				tags.add(curTags);
			}
		}

		// we only need to add MCP if it is not already there
		if (!tags.contains("MCP")) {
			tags.add("MCP");
			metadata.put("tag", tags);
			if (isProject) {
				SecurityProjectUtils.updateProjectMetadata(engine.getEngineId(), metadata);
			} else {
				SecurityEngineUtils.updateEngineMetadata(engine.getEngineId(), metadata);
			}
		}
	}

	private MCPUtility() {

	}
}
