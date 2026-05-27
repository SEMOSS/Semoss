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
package prerna.engine.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.api.IMCP;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPErrorCode;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.execptions.SemossMCPException;
import prerna.util.Constants;
import prerna.util.EngineUtility;

public class InternalMCP implements IMCP {

	private static final Logger classLogger = LogManager.getLogger(InternalMCP.class);

	private IEngine engine;
	private String engineId;
	private String engineName;
	private CATALOG_TYPE engineType;

	public InternalMCP(IEngine engine) {
		this.engine = engine;
		this.engineId = engine.getEngineId();
		this.engineName = engine.getEngineName();
		this.engineType = engine.getCatalogType();
	}

	@Override
	public JSONObject initMCP(String protocolVersion) {
		// need to return the protocol version of the client request
		// as part of initialization
		JSONObject resultJson = new JSONObject();
		resultJson.put("protocolVersion", protocolVersion);

		JSONObject serverJson = new JSONObject();
		serverJson.put("name", this.engineName);
		serverJson.put("version", "1.8.0");
		resultJson.put("serverInfo", serverJson);

		JSONObject capabilitiesJson = new JSONObject();
		capabilitiesJson.put("experimental", new JSONObject());

		JSONObject promptJson = new JSONObject();
		promptJson.put("listChanged", false);
		promptJson.put("subscribe", true);
		capabilitiesJson.put("prompts", promptJson);

		JSONObject resourcesJson = new JSONObject();
		resourcesJson.put("listChanged", false);
		resourcesJson.put("subscribe", true);
		capabilitiesJson.put("resources", resourcesJson);

		JSONObject toolsJson = new JSONObject();
		toolsJson.put("listChanged", false);
		toolsJson.put("subscribe", true);
		capabilitiesJson.put("tools", toolsJson);

		resultJson.put("capabilities", capabilitiesJson);
		return resultJson;
	}

	@Override
	public JSONObject getMCPResources() {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(this.engineType, this.engineId,
				this.engineName);
		// we have python and java mcp
		String jsonFileLoc = assetsFolder + "/mcp/py_mcp.json";
		JSONArray pyToolArray = MCPUtility.getNode(jsonFileLoc, "resources");
		jsonFileLoc = assetsFolder + "/mcp/pixel_mcp.json";
		JSONArray javaToolArray = MCPUtility.getNode(jsonFileLoc, "resources");
		pyToolArray.putAll(javaToolArray);

		JSONObject jsonMap = new JSONObject();
		jsonMap.put("resources", pyToolArray);

		return jsonMap;
	}

	@Override
	public JSONObject getMCPResourcesTemplates() {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(this.engineType, this.engineId,
				this.engineName);
		// we have python and java mcp
		String jsonFileLoc = assetsFolder + "/mcp/py_mcp.json";
		JSONArray pyToolArray = MCPUtility.getNode(jsonFileLoc, "resourceTemplates");
		jsonFileLoc = assetsFolder + "/mcp/pixel_mcp.json";
		JSONArray javaToolArray = MCPUtility.getNode(jsonFileLoc, "resourceTemplates");
		pyToolArray.putAll(javaToolArray);

		JSONObject jsonMap = new JSONObject();
		jsonMap.put("resourceTemplates", pyToolArray);

		return jsonMap;
	}

	@Override
	public JSONObject getMCPPrompts() {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(this.engineType, this.engineId,
				this.engineName);
		// we have python and java mcp
		String jsonFileLoc = assetsFolder + "/mcp/py_mcp.json";
		JSONArray pyToolArray = MCPUtility.getNode(jsonFileLoc, "prompts");
		jsonFileLoc = assetsFolder + "/mcp/pixel_mcp.json";
		JSONArray javaToolArray = MCPUtility.getNode(jsonFileLoc, "prompts");
		pyToolArray.putAll(javaToolArray);

		JSONObject jsonMap = new JSONObject();
		jsonMap.put("prompts", pyToolArray);
		return jsonMap;
	}

	@Override
	public JSONObject getMCPTools() {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(this.engineType, this.engineId,
				this.engineName);
		// we have python and java mcp
		String pythonJsonFileLoc = assetsFolder + "/mcp/py_mcp.json";
		String pixelJsonFileLoc = assetsFolder + "/mcp/pixel_mcp.json";

		JSONObject toolMap = new JSONObject();
		JSONArray toolsArray = new JSONArray();
		toolsArray.putAll(MCPUtility.getNode(pythonJsonFileLoc, "tools"));
		toolsArray.putAll(MCPUtility.getNode(pixelJsonFileLoc, "tools"));
		toolMap.put("tools", toolsArray);

		// add in meta as well
		JSONObject _meta = new JSONObject();
		_meta.put(MCPUtility.SMSS_PROJECT_ID, this.engineId);
		_meta.put(MCPUtility.SMSS_PROJECT_NAME, this.engineName);
		_meta.put(MCPUtility.SMSS_ENGINE_ID, this.engineId);
		_meta.put(MCPUtility.SMSS_ENGINE_NAME, this.engineName);
		_meta.put(MCPUtility.SMSS_ENGINE_TYPE, this.engineType.name());
		toolMap.put("_meta", _meta);

		return toolMap;
	}

	@Override
	public Object callTool(String toolName, Map<String, Object> params, Insight insight) {
		if (toolName == null || (toolName = toolName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Tool name must be passed in to execute the mcp tool");
		}
		toolName = MCPUtility.removeEngineIdFromToolsMethodName(engine.getEngineId(), toolName);
		// first need to find the right tool

		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(this.engineType, this.engineId,
				this.engineName);
		assetsFolder = assetsFolder.replace("\\", "/");

		String pythonJsonFileLoc = assetsFolder + "/mcp/py_mcp.json";
		String pixelJsonFileLoc = assetsFolder + "/mcp/pixel_mcp.json";

		JSONObject toolDefinition = getFunction(toolName, pythonJsonFileLoc);
		Object output = null;
		if (toolDefinition != null) {
			// this is a python mcp
			JSONObject toolProperties = ((JSONObject) toolDefinition.get("inputSchema")).getJSONObject("properties");
			// check if we have an aliased name pointing to a function
			// this is so we can have the same tool/function exposed more than once with
			// different parameters exposed as multiple tools
			String functionName = ((JSONObject) toolDefinition.get("_meta")).optString(MCPUtility.SMSS_FUNCTION_NAME);
			functionName = (functionName != null && !functionName.isBlank()) ? functionName : toolName;

			output = MCPUtility.runPythonTool(this.engine, insight, functionName, toolProperties, params);
			return output;
		}

		toolDefinition = getFunction(toolName, pixelJsonFileLoc);
		if (toolDefinition != null) {
			// this is a pixel mcp tool
			JSONObject toolProperties = ((JSONObject) toolDefinition.get("inputSchema")).getJSONObject("properties");
			// check if we have an aliased name pointing to a function
			// this is so we can have the same tool/function exposed more than once with
			// different parameters exposed as multiple tools
			String functionName = ((JSONObject) toolDefinition.get("_meta")).optString(MCPUtility.SMSS_FUNCTION_NAME);
			functionName = (functionName != null && !functionName.isBlank()) ? functionName : toolName;

			output = MCPUtility.runPixelTool(this.engine, insight, functionName, toolProperties, params);
			return output;
		}

		throw new SemossMCPException("Unknown tool: invalid_tool_name", MCPErrorCode.INVALID_PARAMS);
	}

	/**
	 *
	 * @param inputName
	 * @param jsonFileLoc
	 * @return
	 */
	private JSONObject getFunction(String inputName, String jsonFileLoc) {
		File jsonFile = new File(jsonFileLoc);
		if (jsonFile.exists()) {
			try {
				String jsonTxt = FileUtils.readFileToString(jsonFile, StandardCharsets.UTF_8);
				JSONObject json = new JSONObject(jsonTxt);
				// the tools is what has it
				JSONArray toolObj = null;
				if (json.has("tools")) {
					toolObj = json.getJSONArray("tools");
					for (int toolIndex = 0; toolIndex < toolObj.length(); toolIndex++) {
						JSONObject thisTool = toolObj.getJSONObject(toolIndex);
						String toolName = thisTool.getString("name");
						if (toolName.contains(inputName)) {
							// return the full tool
							return thisTool;
						}
					}
				}
			} catch (FileNotFoundException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (JSONException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return null;
	}

}
