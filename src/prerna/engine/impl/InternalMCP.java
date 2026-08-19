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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPErrorCode;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.execptions.SemossMCPException;
import prerna.util.EngineUtility;

public class InternalMCP implements IMCP {

	private static final Logger classLogger = LogManager.getLogger(InternalMCP.class);

	/**
	 * Resolved once at construction. Every tool/resource/prompt read and every
	 * execution is relative to this folder.
	 */
	private final String assetsFolder;

	/**
	 * The backing engine/project. {@code null} for folder-backed MCPs (rooms,
	 * insights) that have no catalog entry.
	 */
	private final IEngine engine;

	private final String engineId;
	private final String engineName;

	/** Value published as {@link MCPUtility#SMSS_ENGINE_TYPE}. */
	private final String engineType;

	/**
	 * Key used to namespace python module state in the shared insight globals. This
	 * is deliberately NOT {@link #engineId}: folder-backed MCPs may share a
	 * published id (every room publishes {@code __room__}) while pointing at
	 * different folders, and a shared key would make concurrent rooms clobber each
	 * other's loaded driver. Engines keep their id so existing aliases are
	 * unchanged.
	 */
	private final String scopeId;

	private InternalMCP(IEngine engine, String assetsFolder, String engineId, String engineName, String engineType) {
		if (assetsFolder == null || assetsFolder.isBlank()) {
			throw new IllegalArgumentException("An assets folder is required to build an MCP");
		}
		this.engine = engine;
		this.assetsFolder = assetsFolder.replace("\\", "/");
		this.engineId = engineId;
		this.engineName = engineName;
		this.engineType = engineType;
		this.scopeId = (engine != null) ? engineId : folderScopeId(this.assetsFolder);
	}

	/**
	 * Builds a stable, identifier-safe namespace key for a folder-backed MCP. The
	 * folder is unique per scope, so hashing it gives uniqueness without asking the
	 * caller to invent an id.
	 *
	 * @param assetsFolder the normalized assets folder
	 * @return a scope key of the form {@code folder_<hex>}
	 */
	private static String folderScopeId(String assetsFolder) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
			byte[] hash = messageDigest.digest(assetsFolder.getBytes(StandardCharsets.UTF_8));
			// convert bytes to hexadecimal
			StringBuilder s = new StringBuilder();
			for (byte b : hash) {
				s.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
			}
			return "folder_" + s.substring(0, 16);
		} catch (NoSuchAlgorithmException e) {
			classLogger.error("Unable to build an MCP scope key for '{}'", assetsFolder, e);
			throw new IllegalStateException("Unable to build an MCP scope key for " + assetsFolder, e);
		}
	}

	/**
	 * Builds an MCP backed by a catalog engine or project. The assets folder is
	 * derived from the engine's catalog type, id, and name.
	 *
	 * @param engine the engine or project to expose as an MCP
	 * @return an MCP that supports both pixel and python tools
	 */
	public static InternalMCP genFromEngine(IEngine engine) {
		if (engine == null) {
			throw new IllegalArgumentException("Engine must not be null");
		}
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		return new InternalMCP(engine, assetsFolder, engine.getEngineId(), engine.getEngineName(),
				engine.getCatalogType().name());
	}

	/**
	 * Builds an MCP backed by an explicit assets folder rather than a catalog
	 * engine. Use this for scopes that own {@code mcp/pixel_mcp.json} without being
	 * registered as an engine, such as a room or insight folder.
	 *
	 * <p>
	 * Both pixel and python tools are supported. Pixel tools run directly in the
	 * caller's insight rather than under a project context, and python tools run
	 * against {@code <assetsFolder>/py} using the insight's python translator.
	 *
	 * <p>
	 * {@code id} does not need to be unique. Several scopes may legitimately
	 * publish the same id (every room publishes {@code __room__}); python module
	 * state is namespaced off the folder instead. See {@link #scopeId}.
	 *
	 * @param assetsFolder absolute path to the folder containing {@code mcp/}
	 * @param id           id published as SMSS_ENGINE_ID / SMSS_PROJECT_ID
	 * @param name         display name published as SMSS_ENGINE_NAME
	 * @param type         value published as SMSS_ENGINE_TYPE
	 * @return an MCP backed by the given folder
	 */
	public static InternalMCP genFromFolder(String assetsFolder, String id, String name, String type) {
		return new InternalMCP(null, assetsFolder, id, name, type);
	}

	/**
	 * Builds the virtual MCP backed by a room's asset folder, published under
	 * {@link MCPUtility#ROOM_MCP_ID}.
	 *
	 * <p>
	 * This is a general purpose per-room toolbox. It serves whatever is in the
	 * folder's {@code mcp/pixel_mcp.json} and {@code mcp/py_mcp.json}, so any
	 * generator can contribute room scoped tools and each tool picks its own
	 * {@code SMSS_MCP_UI}, execution mode, and {@code SMSS_FUNCTION_NAME}.
	 * Playwright playback uses this and tools happen to point their UI at a
	 * {@code system://} app; nothing here requires that, and a tool may omit the UI
	 * block entirely.
	 *
	 * <p>
	 * Where {@code SMSS_ENGINE_ID} normally holds a real engine or project UUID,
	 * {@link MCPUtility#ROOM_MCP_ID} tells the resolver to read the room folder
	 * rather than look the id up in the catalog.
	 *
	 * <p>
	 * Every room publishes that same reserved id, which is fine: python module
	 * state is namespaced off the folder, not the id. See {@link #scopeId}.
	 *
	 * @param roomFolder the room folder containing {@code mcp/}
	 * @return an MCP backed by that folder
	 */
	public static InternalMCP genFromRoomFolder(String roomFolder) {
		if (roomFolder == null || roomFolder.isBlank()) {
			throw new IllegalArgumentException("Room folder must not be blank for a room MCP");
		}
		String normalized = Paths.get(roomFolder).toAbsolutePath().normalize().toString();
		return genFromFolder(normalized, MCPUtility.ROOM_MCP_ID, MCPUtility.ROOM_MCP_NAME, MCPUtility.ROOM_MCP_TYPE);
	}

	private String pyMcpPath() {
		return this.assetsFolder + MCPUtility.PY_MCP_RELATIVE_PATH;
	}

	private String pixelMcpPath() {
		return this.assetsFolder + MCPUtility.PIXEL_MCP_RELATIVE_PATH;
	}

	/**
	 * True when a folder holds MCP tool definitions, without building an MCP for
	 * it.
	 *
	 * @param assetsFolder folder that may contain an {@code mcp/} directory
	 * @return true when a pixel or python definition file is present
	 */
	public static boolean hasDefinitions(String assetsFolder) {
		if (assetsFolder == null || assetsFolder.isBlank()) {
			return false;
		}
		return new File(assetsFolder + MCPUtility.PIXEL_MCP_RELATIVE_PATH).isFile()
				|| new File(assetsFolder + MCPUtility.PY_MCP_RELATIVE_PATH).isFile();
	}

	/**
	 * Reads the given node from both the python and pixel MCP definitions and
	 * merges them into a single array.
	 *
	 * @param nodeName one of tools, resources, resourceTemplates, prompts
	 * @return the merged array, empty when neither file defines the node
	 */
	private JSONArray getMergedNode(String nodeName) {
		JSONArray merged = MCPUtility.getNode(pyMcpPath(), nodeName);
		merged.putAll(MCPUtility.getNode(pixelMcpPath(), nodeName));
		return merged;
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
		// we have python and java mcp
		JSONObject jsonMap = new JSONObject();
		jsonMap.put("resources", getMergedNode("resources"));

		return jsonMap;
	}

	@Override
	public JSONObject getMCPResourcesTemplates() {
		// we have python and java mcp
		JSONObject jsonMap = new JSONObject();
		jsonMap.put("resourceTemplates", getMergedNode("resourceTemplates"));

		return jsonMap;
	}

	@Override
	public JSONObject getMCPPrompts() {
		// we have python and java mcp
		JSONObject jsonMap = new JSONObject();
		jsonMap.put("prompts", getMergedNode("prompts"));
		return jsonMap;
	}

	@Override
	public JSONObject getMCPTools() {
		// we have python and java mcp
		JSONObject toolMap = new JSONObject();
		toolMap.put("tools", getMergedNode("tools"));

		// add in meta as well
		JSONObject _meta = new JSONObject();
		_meta.put(MCPUtility.SMSS_ENGINE_ID, this.engineId);
		_meta.put(MCPUtility.SMSS_ENGINE_NAME, this.engineName);
		_meta.put(MCPUtility.SMSS_ENGINE_TYPE, this.engineType);
		toolMap.put("_meta", _meta);

		return toolMap;
	}

	@Override
	public Object callTool(String toolName, Map<String, Object> params, Insight insight) {
		if (toolName == null || (toolName = toolName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Tool name must be passed in to execute the mcp tool");
		}
		toolName = MCPUtility.removeEngineIdFromToolsMethodName(this.engineId, toolName);
		// first need to find the right tool

		JSONObject toolDefinition = getFunction(toolName, pyMcpPath());
		Object output = null;
		if (toolDefinition != null) {
			// this is a python mcp
			output = MCPUtility.runPythonTool(this.engine, this.assetsFolder, this.scopeId, insight,
					resolveFunctionName(toolDefinition, toolName), getToolProperties(toolDefinition), params);
			return output;
		}

		toolDefinition = getFunction(toolName, pixelMcpPath());
		if (toolDefinition != null) {
			// this is a pixel mcp tool
			output = MCPUtility.runPixelTool(this.engine, insight, resolveFunctionName(toolDefinition, toolName),
					getToolProperties(toolDefinition), params);
			return output;
		}

		throw new SemossMCPException("Unknown tool '" + toolName + "' in mcp definitions under " + this.assetsFolder
				+ describeNearMiss(toolName), MCPErrorCode.INVALID_PARAMS);
	}

	/**
	 * Builds a diagnostic suffix for the unknown-tool error when the requested name
	 * looks like a defined name that was cut short.
	 *
	 * <p>
	 * Deliberately reports rather than repairs. A name that is a strict prefix of a
	 * defined tool almost always means the provider truncated it to fit a tool name
	 * limit and the reversal in {@code Room#resolveOriginalToolName} did not run -
	 * usually because that room's lookup map was cold. Naming the candidate makes
	 * that diagnosable without silently running a tool the caller did not ask for.
	 *
	 * <p>
	 * Only walked on the failure path, so it costs nothing in normal operation.
	 *
	 * @param toolName the name that failed to resolve
	 * @return a message suffix, empty when no candidate looks related
	 */
	private String describeNearMiss(String toolName) {
		if (toolName == null || toolName.isBlank()) {
			return "";
		}
		for (String file : new String[] { pyMcpPath(), pixelMcpPath() }) {
			JSONArray tools = MCPUtility.getNode(file, "tools");
			for (int i = 0; i < tools.length(); i++) {
				JSONObject tool = tools.optJSONObject(i);
				String defined = tool == null ? null : tool.optString("name", null);
				if (defined != null && defined.length() > toolName.length() && defined.startsWith(toolName)) {
					return ". '" + defined + "' starts with the requested name, so this looks like a tool name"
							+ " truncated to a provider limit that was not resolved back to its original"
							+ " (see Room#resolveOriginalToolName)";
				}
			}
		}
		return "";
	}

	/**
	 * Extracts {@code inputSchema.properties}, defaulting to an empty object when
	 * the tool declares no parameters or omits the schema entirely.
	 *
	 * @param toolDefinition the tool definition
	 * @return the parameter schema, never null
	 */
	private static JSONObject getToolProperties(JSONObject toolDefinition) {
		JSONObject inputSchema = toolDefinition.optJSONObject("inputSchema");
		JSONObject properties = inputSchema != null ? inputSchema.optJSONObject("properties") : null;
		return properties != null ? properties : new JSONObject();
	}

	/**
	 * Resolves the function to invoke. Tools may alias a function via
	 * SMSS_FUNCTION_NAME so the same function can be exposed more than once with
	 * different parameters as multiple tools.
	 *
	 * @param toolDefinition the tool definition
	 * @param toolName       the requested tool name, used when there is no alias
	 * @return the function name to invoke
	 */
	private static String resolveFunctionName(JSONObject toolDefinition, String toolName) {
		JSONObject toolMeta = toolDefinition.optJSONObject("_meta");
		String functionName = toolMeta != null ? toolMeta.optString(MCPUtility.SMSS_FUNCTION_NAME) : null;
		return (functionName != null && !functionName.isBlank()) ? functionName : toolName;
	}

	/**
	 * Finds a tool definition by exact name.
	 *
	 * <p>
	 * Matching is deliberately exact. Name aliasing is reversed upstream by
	 * {@code Room#resolveOriginalToolName}, so callers arriving through
	 * {@code RunMCPToolReactor} or {@code AgentToolDecisionHandler} already hold
	 * the real name. If a name reaches here that matches nothing, something
	 * upstream is wrong and this reports it rather than picking the closest
	 * candidate: an approximate match would run a different recording than the user
	 * approved, which is worse than failing.
	 *
	 * <p>
	 * The failure worth recognizing is a truncated name. For providers that cap
	 * tool name length (OpenAI and Azure at 64 chars),
	 * {@code MCPUtility.appendEngineIdToToolsMethodName} shortens names with
	 * {@code currentName.substring(0, availableChars)}, and stripping the engine id
	 * prefix does not put those characters back. {@link #describeNearMiss(String)}
	 * detects that shape and names it in the error so the cause is obvious.
	 *
	 * @param inputName   the requested tool name, already stripped of its engine id
	 *                    prefix
	 * @param jsonFileLoc the mcp definition file to search
	 * @return the matching tool definition, or null when no name matches exactly
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
						if (toolName.equals(inputName)) {
							// return the full tool
							return thisTool;
						}
					}
				}
			} catch (JSONException e) {
				classLogger.error("Unable to parse mcp definitions at '{}'", jsonFileLoc, e);
			} catch (IOException e) {
				classLogger.error("Unable to read mcp definitions at '{}'", jsonFileLoc, e);
			}
		}
		return null;
	}

}
