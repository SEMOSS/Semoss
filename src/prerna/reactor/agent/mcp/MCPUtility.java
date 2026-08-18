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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.StreamingOutput;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IMCP;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.InternalMCP;
import prerna.engine.impl.MCPFactory;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelStreamUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossMCPException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.AbstractTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
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
	public static final String SMSS_ORIGINAL_TOOL_NAME = "SMSS_ORIGINAL_TOOL_NAME";
	public static final String SMSS_MCP_UI = "SMSS_MCP_UI";

	/**
	 * Records which generator produced a tool. A generator replaces tools carrying
	 * its own id; a tool without this key is owned by nobody and always survives.
	 */
	public static final String SMSS_MCP_GENERATOR = "SMSS_MCP_GENERATOR";

	public static final String UI_RESOURCE_URI = "resourceURI";
	public static final String UI_LOADING_MESSAGE = "loadingMessage";
	public static final String UI_DISPLAY_LOCATION = "displayLocation";
	public static final String UI_AUTO_OPEN = "autoOpen";

	/**
	 * @deprecated Use {@link #SMSS_ENGINE_ID}, which is set for every engine type
	 *             and is what the tool routing paths read first. Do not add new
	 *             reads of this key.
	 */
	@Deprecated
	public static final String SMSS_PROJECT_ID = "SMSS_PROJECT_ID";

	/**
	 * @deprecated Use {@link #SMSS_ENGINE_NAME}. Same reasoning as
	 *             {@link #SMSS_PROJECT_ID}.
	 */
	@Deprecated
	public static final String SMSS_PROJECT_NAME = "SMSS_PROJECT_NAME";

	/**
	 * Reserved id standing in where an engine or project UUID would normally go. An
	 * MCP id is otherwise always a real catalog entry; this fixed, deliberately
	 * unmistakable value instead means "there is no engine - read the tools from
	 * the room's own asset folder". Used both in {@code room.options.mcp} and as
	 * the {@link #SMSS_ENGINE_ID} on the tools themselves.
	 *
	 * <p>
	 * Persisted in room OPTIONS and in the LLM-facing tool name prefix.
	 */
	public static final String ROOM_MCP_ID = "__room__";

	/** Display name shown in the room's MCP list. */
	public static final String ROOM_MCP_NAME = "Room Tools";

	/**
	 * Type reported for the room's own toolbox, published as
	 * {@link #SMSS_ENGINE_TYPE} on its tools. Not an {@code IEngine.CATALOG_TYPE}
	 * value: a room has no engine or project behind it, only its folder.
	 *
	 * <p>
	 * Safe to change, unlike {@link #ROOM_MCP_ID}. Nothing dispatches on it: the
	 * room options form tests for "VECTOR", the tool sidebar tests for "PROJECT".
	 */
	public static final String ROOM_MCP_TYPE = "ROOM";

	/** Pixel tool definition path relative to an engine, project, or room asset folder. */
	public static final String PIXEL_MCP_RELATIVE_PATH = "/mcp/pixel_mcp.json";

	public static final String MCP_PY_FILE_NAME = "mcp_driver.py";
	public static final String MCP_NOTEBOOK_NAME = "mcp_driver";

	@Deprecated
	public static final String LEGACY_PY_FILE_NAME = "smss_driver.py";
	@Deprecated
	public static final String LEGACY_MCP_NOTEBOOK_NAME = "smss_driver";

	// Regex pattern for "a" + UUID + "_"
	// UUID format: 8-4-4-4-12 hexadecimal digits. Separators may be "-" (legacy
	// names persisted before LLM-name sanitization) or "_" (sanitized names).
	private static final Pattern UUID_PREFIX_PATTERN = Pattern.compile(
			"^a([0-9a-fA-F]{8})[-_]([0-9a-fA-F]{4})[-_]([0-9a-fA-F]{4})[-_]([0-9a-fA-F]{4})[-_]([0-9a-fA-F]{12})_");

	// Default maximum tool name length (matches OpenAI's 64-char limit)
	public static final int DEFAULT_MAX_TOOL_NAME_LENGTH = 64;

	// SMSS property key to override tool name length per engine instance
	public static final String MAX_TOOL_NAME_CHAR = "MAX_TOOL_NAME_CHAR";

	// Per-provider tool name length limits keyed by ModelTypeEnum.name()
	private static final Map<String, Integer> PROVIDER_TOOL_NAME_LIMITS = Map.of(ModelTypeEnum.OPEN_AI.name(), 64,
			ModelTypeEnum.AZURE_OPEN_AI.name(), 64
	// other providers default to Integer.MAX_VALUE (no truncation)
	);

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
	 * Executes a Python MCP tool function from the engine's {@code assets/py}
	 * directory.
	 * <p>
	 * The engine asset folder path is resolved once and forwarded explicitly to the
	 * Python process via {@link PyTranslator#runScriptWithExplicitAssetPaths}, so
	 * the shared {@code Insight} context fields ({@code contextProjectId} /
	 * {@code contextProjectName}) are never written. This prevents the race
	 * condition where concurrent threads for different engines on the same insight
	 * would overwrite each other's context before the Python call completes.
	 * <p>
	 * On the Python side, the generated script loads the driver file under a
	 * per-engine namespaced key in {@code insight_globals} (e.g.
	 * {@code __smss_mcp_<engineId>__}) rather than the bare name
	 * {@code mcp_driver}. All temporary variables live in function-local scope so
	 * they never pollute the shared globals dict. File modification-time checking
	 * ensures the module is reloaded automatically when the source file changes.
	 *
	 * @param engine             the engine backing this tool, or {@code null} for a
	 *                           folder-backed scope (room/insight) that has no
	 *                           catalog entry. Only used for project-scoped Python
	 *                           translator selection and chroot symlinking.
	 * @param assetsFolder       the folder whose {@code py} subfolder contains the
	 *                           driver
	 * @param scopeId            id used to namespace the module alias and the
	 *                           generated function def. Must be unique per scope
	 *                           (engine id, project id, or room id)
	 * @param insight            the calling insight (provides the Python translator
	 *                           and globals store)
	 * @param functionName       the Python function to invoke inside the driver
	 * @param functionProperties JSON schema of the function's parameters (type /
	 *                           default)
	 * @param paramMap           runtime argument values keyed by parameter name
	 * @return the stringified result of the Python function call
	 */
	public static String runPythonTool(IEngine engine, String assetsFolder, String scopeId, Insight insight,
			String functionName, JSONObject functionProperties, Map<String, Object> paramMap) {
		if (assetsFolder == null || assetsFolder.isBlank()) {
			throw new IllegalArgumentException("An assets folder is required to run a python tool");
		}
		if (scopeId == null || scopeId.isBlank()) {
			throw new IllegalArgumentException("A scope id is required to run a python tool");
		}

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

		if (!namedMCP) {
			classLogger.warn("Using legacy {} python file name - needs to be updated to {}", LEGACY_PY_FILE_NAME,
					MCP_PY_FILE_NAME);
		}

		// Build the parameter string from functionProperties + paramMap
		Iterator<String> props = functionProperties.keys();
		StringBuilder paramString = new StringBuilder();
		while (props.hasNext()) {
			if (paramString.length() != 0) {
				paramString.append(", ");
			}
			String propName = props.next();
			JSONObject thisProp = ((JSONObject) functionProperties.get(propName));
			Object propValue = null;
			if (paramMap != null && paramMap.containsKey(propName)) {
				propValue = paramMap.get(propName);
			} else if (thisProp.has("default")) {
				propValue = thisProp.getString("default");
			}
			paramString.append(propName).append("=").append(PyUtils.determineStringType(propValue));
		}

		PyTranslator pyt = null;
		if (engine instanceof IProject) {
			String pyEngine = "user";
			if (engine.getSmssProp().containsKey(Constants.USE_PYTHON)) {
				pyEngine = engine.getSmssProp().get(Constants.USE_PYTHON) + "";
			}
			if (pyEngine.equalsIgnoreCase("project")) {
				pyt = ((IProject) engine).getProjectPyTranslator();
			}

			// dont forget to mount the project into the symlink folder if chroot is enabled
			// so that the python process can access the files
			User user = insight.getUser();
			if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
				user.getUserSymlinkHelper().symlinkProject(user, engine.getEngineId());
			}
		}
		if (pyt == null) {
			pyt = insight.getPyTranslator();
		}

		// Use a scope-namespaced alias so concurrent calls for different scopes
		// writing to the same insight_globals never overwrite each other's mcp_driver
		// reference. The module is loaded from an explicit file path so sys.modules is
		// never mutated and third-party packages (e.g. torch) are not affected.
		String safeScopeId = scopeId.replaceAll("[^A-Za-z0-9_]", "_");
		String modAlias = "__smss_mcp_" + safeScopeId + "__";
		String modMtimeKey = modAlias + "_mtime__";
		String funcDefName = "__smss_run_" + safeScopeId + "__";
		String mcpFilePath = pyFolderLoc + "/" + (namedMCP ? MCP_PY_FILE_NAME : LEGACY_PY_FILE_NAME);
		// All temp vars (_f, _mt, _spec, _mod, _drv, ...) live inside the function's
		// local scope and never touch insight_globals. Only globals()[modAlias] and
		// globals()[modMtimeKey] are written - both are per-engine keys - so
		// concurrent threads for different engines on the same insight cannot overwrite
		// each other's state. funcDefName is also per-engine so the def itself doesn't
		// collide.
		String runScript = """
				def <funcDefName>():
				    import importlib.util as _ilu, os as _os, hashlib as _hl, sys as _sys
				    _f = r'<mcpFilePath>'
				    _mt = _os.path.getmtime(_f) if _os.path.exists(_f) else None
				    if globals().get('<modAlias>') is None or globals().get('<modMtimeKey>') != _mt:
				        _pfx = '_smss_' + _hl.md5(r'<pyFolderLoc>'.encode()).hexdigest()[:12] + '_'
				        for _k in [k for k in _sys.modules if k.startswith(_pfx)]:
				            del _sys.modules[_k]
				        _spec = _ilu.spec_from_file_location('mcp_driver', _f)
				        _mod = _ilu.module_from_spec(_spec)
				        _spec.loader.exec_module(_mod)
				        globals()['<modAlias>'] = _mod
				        globals()['<modMtimeKey>'] = _mt
				    _drv = globals()['<modAlias>']

				<legacyVarCodeSnipped>

				    return _drv.<functionName>(<paramString>)
				<funcDefName>()
				""";
		// @formatter:off 
		final String PY_INDENT = "    ";
		final String legacyVarCodeSnippet = String.join(
				"\n",
				PY_INDENT + "for _k in ['ROOT', 'APP_ROOT', 'USER_ROOT']:",
				PY_INDENT + PY_INDENT + "_v = smss_get_runtime_var(_k)",
				PY_INDENT + PY_INDENT + "if _v is not None:",
				PY_INDENT + PY_INDENT + PY_INDENT + "setattr(_drv, _k, _v)"
				);
		runScript = runScript
				/*
				 * Will delete the below replace. Only here for backwards compatibility using
				 * incorrect syntax to access ROOT, APP_ROOT, USER_ROOT as storing in globals
				 * can cause race conditions
				 */
				.replace("<legacyVarCodeSnipped>", legacyVarCodeSnippet)
				.replace("<funcDefName>", funcDefName)
				.replace("<mcpFilePath>", mcpFilePath)
				.replace("<modAlias>", modAlias)
				.replace("<modMtimeKey>", modMtimeKey)
				.replace("<pyFolderLoc>", pyFolderLoc)
				.replace("<functionName>", functionName)
				.replace("<paramString>", paramString.toString());
		// @formatter:on
		if (engine != null) {
			classLogger.info("Running python tool '{}.{}({})' from {} engine '{}'", modAlias, functionName, paramString,
					engine.getCatalogType(), engine.getEngineId());
		} else {
			classLogger.info("Running python tool '{}.{}({})' from folder scope '{}'", modAlias, functionName,
					paramString, scopeId);
		}

		return stringifyMcpResult(
				pyt.runScriptWithExplicitAssetPaths(insight, runScript, assetsFolder, new String[] { pyFolderLoc }));
	}

	/**
	 * Convenience overload that derives the assets folder and scope id from the
	 * engine. Mirrors
	 * {@link #runPixelTool(IEngine, Insight, String, JSONObject, Map)}.
	 *
	 * @param engine             the engine whose {@code assets/py} folder contains
	 *                           the driver
	 * @param insight            the calling insight
	 * @param functionName       the Python function to invoke inside the driver
	 * @param functionProperties JSON schema of the function's parameters
	 * @param paramMap           runtime argument values keyed by parameter name
	 * @return the stringified result of the Python function call
	 */
	public static String runPythonTool(IEngine engine, Insight insight, String functionName,
			JSONObject functionProperties, Map<String, Object> paramMap) {
		if (engine == null) {
			throw new IllegalArgumentException("Engine must not be null - use the assetsFolder overload instead");
		}
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		return runPythonTool(engine, assetsFolder, engine.getEngineId(), insight, functionName, functionProperties,
				paramMap);
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

		String runMethod = functionName + "(" + paramString + ");";
		if (engine != null) {
			classLogger.info("Running pixel tool '{}' from {} engine '{}'", runMethod, engine.getCatalogType(),
					engine.getEngineId());
		} else {
			classLogger.info("Running pixel tool '{}' directly without an engine", runMethod);
		}
		// run pixel - use scoped context when engine is a project so concurrent tool
		// calls for different engines on the same insight don't overwrite each other's
		// contextProjectId
		PixelRunner pixelReturn = (engine instanceof IProject)
				? insight.runPixelWithContext(engine.getEngineId(), engine.getEngineName(), runMethod)
				: insight.runPixel(runMethod);
		NounMetadata result = pixelReturn.getResults().get(0);
		if (result.getOpType().contains(PixelOperationType.ERROR)) {
			throw new SemossMCPException(result.getValue() + "", MCPErrorCode.SERVER_ERROR);
		}
		if (result.getNounType() == PixelDataType.PIXEL_RUNNER) {
			PixelRunner runner = (PixelRunner) ((Map<String, Object>) result.getValue()).get("runner");
			return stringifyMcpResult(runner);
		} else if (result.getNounType() == PixelDataType.FORMATTED_DATA_SET) {
			Object value = result.getValue();
			if (value instanceof ITask) {
				// if we have a task
				// iterate through it to return the data
				try (ITask task = (ITask) value) {
					if (task instanceof ConstantDataTask) {
						return stringifyMcpResult(((ConstantDataTask) task).getOutputData());
					}

					classLogger.debug("Start flushing task = {}", task.getId());
					Map<String, Object> dataMap = new HashMap<>();
					// first merge the metadata
					dataMap.putAll(task.getMetaMap());

					int numCollect = task.getNumCollect();
					boolean collectAll = numCollect == -1;
					String formatType = task.getFormatter().getFormatType();

					if (formatType.equals("TABLE")) {
						// right now, only grid will work
						String[] headers = null;
						String[] rawHeaders = null;
						int count = 0;

						// try to at least provide the headers
						List<Map<String, Object>> headerInfo = task.getHeaderInfo();
						if (headerInfo != null) {
							headers = new String[headerInfo.size()];
							rawHeaders = new String[headerInfo.size()];
							for (int i = 0; i < headers.length; i++) {
								headers[i] = headerInfo.get(i).get("alias") + "";
								rawHeaders[i] = headerInfo.get(i).get("header") + "";
							}
						}
						dataMap.put("headers", headers);
						dataMap.put("rawHeaders", rawHeaders);
						List<Object[]> values = new ArrayList<>();
						while (task.hasNext() && (collectAll || count < numCollect)) {
							IHeadersDataRow row = task.next();
							values.add(row.getValues());
							count++;
						}
						dataMap.put("values", values);
					} else {
						// just let the formatter handle the output of this data
						dataMap.put("data", ((AbstractTask) task).getData());
					}
					classLogger.debug("Done flushing sending task = {}", task.getId());

					Map<String, Object> retObj = new HashMap<>();
					retObj.put("output", dataMap);
					return stringifyMcpResult(retObj);
				} catch (Exception e) {
					throw new SemossMCPException(e.getMessage(), MCPErrorCode.TOOL_EXECUTION_FAILED);
				}
			}
		}

		// all other situations, just return the value
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
		} else if (value instanceof PixelRunner) {
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
				// The StreamingOutput writes its data to the provided OutputStream
				StreamingOutput streamingOutput = PixelStreamUtility.collectPixelData((PixelRunner) value, null);
				streamingOutput.write(baos);
				// Convert the captured bytes to a String using UTF-8 encoding
				return baos.toString(StandardCharsets.UTF_8.name());
			} catch (WebApplicationException | IOException e) {
				classLogger.error("The pixel ran but an error occurred streaming the pixel results", e.getMessage());
				return "An error occurred streaming the pixel execution output: " + e.getMessage();
			}
		}

		return GSON.toJson(value);
	}

	/**
	 * Returns the first 8 hex characters of the engineId (using a UUID string -
	 * dashes removed; unless platform project/engine which usually do not).
	 */
	public static String computeShortEngineId(String engineId) {
		String retId = engineId;
		if (retId.contains("-")) {
			retId = retId.replace("-", "");
		}
		if (retId.length() > 8) {
			retId = retId.substring(0, 8);
		}
		return retId;
	}

	/**
	 * Returns the maximum tool name length for the given model engine. Checks
	 * MAX_TOOL_NAME_CHAR in the engine's SMSS first, then falls back to the
	 * per-provider default.
	 */
	public static int getMaxToolNameLength(IModelEngine modelEngine) {
		if (modelEngine == null) {
			return DEFAULT_MAX_TOOL_NAME_LENGTH;
		}
		java.util.Properties smssProp = modelEngine.getSmssProp();
		if (smssProp != null) {
			String smssValue = smssProp.getProperty(MAX_TOOL_NAME_CHAR);
			if (smssValue != null && !smssValue.isBlank()) {
				try {
					return Integer.parseInt(smssValue.trim());
				} catch (NumberFormatException e) {
					classLogger.warn("Invalid {} value '{}' in SMSS for engine {} - falling back to provider default",
							MAX_TOOL_NAME_CHAR, smssValue, modelEngine.getEngineId());
				}
			}
		}
		ModelTypeEnum modelType = modelEngine.getModelType();
		return getMaxToolNameLength(modelType != null ? modelType.name() : null);
	}

	/**
	 * Returns the maximum tool name length for the given model type string. OPEN_AI
	 * and AZURE_OPEN_AI return 64; all others return Integer.MAX_VALUE (no
	 * truncation). A null type falls back to DEFAULT_MAX_TOOL_NAME_LENGTH in case
	 * it is OPEN_AI.
	 */
	public static int getMaxToolNameLength(String modelType) {
		if (modelType == null) {
			return DEFAULT_MAX_TOOL_NAME_LENGTH;
		}
		return PROVIDER_TOOL_NAME_LIMITS.getOrDefault(modelType, Integer.MAX_VALUE);
	}

	/**
	 * Appends engine ID prefix to each tool name with no length limit (preserves
	 * full UUID prefix).
	 */
	public static JSONObject appendEngineIdToToolsMethodName(String engineId, JSONObject jsonToolsMap) {
		return appendEngineIdToToolsMethodName(engineId, jsonToolsMap, Integer.MAX_VALUE);
	}

	/**
	 * Appends engine ID prefix to each tool name, truncating to maxLength.
	 * Non-length-limited providers (Integer.MAX_VALUE) use the full UUID prefix and
	 * skip truncation.
	 */
	public static JSONObject appendEngineIdToToolsMethodName(String engineId, JSONObject jsonToolsMap, int maxLength) {
		return appendEngineIdToToolsMethodName(engineId, jsonToolsMap, maxLength, false);
	}

	/**
	 * Appends engine ID prefix to each tool name, truncating to maxLength. When
	 * sanitizeForLLM is true the injected prefix is restricted to [a-zA-Z0-9_]
	 * (see {@link #requiresLLMNameSanitization}); other providers keep the raw
	 * engine id so existing LLM-facing names are unchanged.
	 */
	public static JSONObject appendEngineIdToToolsMethodName(String engineId, JSONObject jsonToolsMap, int maxLength,
			boolean sanitizeForLLM) {
		if (jsonToolsMap == null || !jsonToolsMap.has("tools")) {
			return jsonToolsMap;
		}

		JSONArray toolsArray = jsonToolsMap.getJSONArray("tools");
		String idForPrefix = sanitizeForLLM ? sanitizeEngineIdForLLMName(engineId) : engineId;
		String shortIdForPrefix = sanitizeForLLM ? sanitizeEngineIdForLLMName(computeShortEngineId(engineId))
				: computeShortEngineId(engineId);

		if (maxLength == Integer.MAX_VALUE) {
			// No length limit: use the full UUID prefix (preserves existing behavior)
			String fullPrefix = "a" + idForPrefix + "_";
			for (int i = 0; i < toolsArray.length(); i++) {
				JSONObject toolMap = toolsArray.getJSONObject(i);
				String currentName = toolMap.getString("name");
				toolMap.put("name", fullPrefix + currentName);
			}
			return jsonToolsMap;
		}

		// Length-limited provider: prefer full UUID prefix when it fits, otherwise
		// fall back to short 8-hex prefix and truncate if still needed.
		String fullPrefix = "a" + idForPrefix + "_";
		String shortPrefix = "a" + shortIdForPrefix + "_";

		for (int i = 0; i < toolsArray.length(); i++) {
			JSONObject toolMap = toolsArray.getJSONObject(i);
			String currentName = toolMap.getString("name");
			String llmName;
			if (fullPrefix.length() + currentName.length() <= maxLength) {
				llmName = fullPrefix + currentName;
			} else {
				int availableChars = maxLength - shortPrefix.length();
				if (availableChars < 0) {
					availableChars = 0;
				}
				String truncated = currentName.length() > availableChars ? currentName.substring(0, availableChars)
						: currentName;
				llmName = shortPrefix + truncated;
			}
			toolMap.put("name", llmName);
		}
		return jsonToolsMap;
	}

	/**
	 * LLM-facing tool names must stay within [a-zA-Z0-9_]. Amazon Nova (Bedrock)
	 * deterministically fails with "Model produced invalid sequence as part of
	 * ToolUse" whenever a tool name contains a hyphen (e.g. from a hyphenated
	 * engine id), and underscores are accepted by every provider. Only the
	 * SEMOSS-injected prefix is sanitized; the author's tool name is appended
	 * untouched.
	 */
	public static String sanitizeEngineIdForLLMName(String engineId) {
		return engineId == null ? null : engineId.replaceAll("[^a-zA-Z0-9_]", "_");
	}

	/**
	 * Whether LLM-facing tool names must be sanitized for the given model engine.
	 * Only Bedrock engines opt in: Amazon Nova is the model family that rejects
	 * hyphenated tool names, and scoping the rename here keeps every other
	 * provider's tool names byte-identical to their historical form.
	 */
	public static boolean requiresLLMNameSanitization(IModelEngine modelEngine) {
		return modelEngine != null && ModelTypeEnum.BEDROCK == modelEngine.getModelType();
	}

	/**
	 * Strips the engine ID prefix from a tool function name. Tries the short prefix
	 * (a{8hex}_) first, then the sanitized full prefix, then falls back to the
	 * legacy raw full prefix (names persisted before LLM-name sanitization).
	 */
	public static String removeEngineIdFromToolsMethodName(String engineId, String functionName) {
		String shortPrefix = "a" + sanitizeEngineIdForLLMName(computeShortEngineId(engineId)) + "_";
		if (functionName.startsWith(shortPrefix)) {
			return functionName.substring(shortPrefix.length());
		}
		String sanitizedFullPrefix = "a" + sanitizeEngineIdForLLMName(engineId) + "_";
		if (functionName.startsWith(sanitizedFullPrefix)) {
			return functionName.substring(sanitizedFullPrefix.length());
		}
		String fullPrefix = "a" + engineId + "_";
		if (functionName.startsWith(fullPrefix)) {
			return functionName.substring(fullPrefix.length());
		}
		return functionName;
	}

	/**
	 * Parses the legacy "a{UUID}_" prefix from a function name. Returns [engineId,
	 * functionName] or null if no match.
	 */
	public static String[] parseEngineIdFromFunctionName(String input) {
		if (input == null) {
			return null;
		}

		Matcher matcher = UUID_PREFIX_PATTERN.matcher(input);
		if (matcher.find()) {
			// rebuild the canonical hyphenated UUID regardless of which separator
			// ("-" legacy, "_" sanitized) appeared in the LLM-facing name
			String prefix = matcher.group(1) + "-" + matcher.group(2) + "-" + matcher.group(3) + "-"
					+ matcher.group(4) + "-" + matcher.group(5);
			String remaining = input.substring(matcher.end());
			return new String[] { prefix, remaining };
		}

		return null;
	}

	/**
	 * Updates the tool response with engine/tool metadata.
	 */
	public static void updateToolResponseWithProjectMeta(ResponseMessage response) {
		updateToolResponseWithProjectMeta(response, null, null);
	}

	/**
	 * Updates the tool response with engine/tool metadata. Accepts a cache map to
	 * avoid repeated engine lookups when iterating over multiple messages.
	 */
	public static void updateToolResponseWithProjectMeta(ResponseMessage response,
			Map<String, JSONObject> mcpToolsJsonCache) {
		updateToolResponseWithProjectMeta(response, mcpToolsJsonCache, null);
	}

	/**
	 * Updates the tool response with engine/tool metadata. Uses llmNameToToolJson
	 * for direct lookup (short-prefix names) when provided, falling back to
	 * UUID-regex parsing for legacy full-UUID-prefix names.
	 */
	@SuppressWarnings("unchecked")
	public static void updateToolResponseWithProjectMeta(ResponseMessage response,
			Map<String, JSONObject> mcpToolsJsonCache, Map<String, Map<String, Object>> llmNameToToolJson) {
		if (mcpToolsJsonCache == null) {
			mcpToolsJsonCache = new HashMap<>();
		}
		List<Map<String, Object>> toolResponses = response.getToolResponses();
		for (int toolResponseIndex = 0; toolResponseIndex < toolResponses.size(); toolResponseIndex++) {
			Map<String, Object> responseToolMap = toolResponses.get(toolResponseIndex);
			String llmFacingName = (String) responseToolMap.get("name");

			if (hasText(llmFacingName) && !hasText(responseToolMap.get("title"))) {
				responseToolMap.put("title", llmFacingName);
			}

			if (llmNameToToolJson != null && llmNameToToolJson.containsKey(llmFacingName)) {
				Map<String, Object> toolEntry = llmNameToToolJson.get(llmFacingName);
				Object rawMeta = toolEntry.get("_meta");
				Map<String, Object> enrichedMeta = (rawMeta instanceof Map) ? (Map<String, Object>) rawMeta
						: new HashMap<>();

				String origFunctionName = (String) enrichedMeta.get(SMSS_FUNCTION_NAME);
				if (origFunctionName == null) {
					origFunctionName = llmFacingName;
				}

				responseToolMap.put("_tool_found", true);
				responseToolMap.put("original_name", origFunctionName);

				Object declaredTitle = toolEntry.get("title");
				if (hasText(declaredTitle)) {
					responseToolMap.put("title", declaredTitle);
				} else {
					// No declared title, so label the tool with the name it declared before
					// the engine-id prefix was appended (and possibly truncated) for the LLM
					Object originalToolName = enrichedMeta.get(SMSS_ORIGINAL_TOOL_NAME);
					responseToolMap.put("title", hasText(originalToolName) ? originalToolName : origFunctionName);
				}
				if (toolEntry.containsKey("description")) {
					responseToolMap.put("description", toolEntry.get("description"));
				}
				applyMissingArgumentDefaults(responseToolMap, toolEntry);

				Map<String, Object> currentMeta = new HashMap<>(enrichedMeta);
				currentMeta.put(SMSS_MCP_EXECUTION, getValidMcpExecution(enrichedMeta));
				JSONObject uiMeta = getValidMcpUI(enrichedMeta);
				if (uiMeta != null) {
					currentMeta.put(SMSS_MCP_UI, uiMeta.toMap());
				}
				responseToolMap.put("_meta", currentMeta);
				continue;
			}

			// Legacy fallback: UUID-regex parsing for old full-UUID-prefix names
			String[] responseProjectIdToolFunctionNameSplit = parseEngineIdFromFunctionName(llmFacingName);
			if (responseProjectIdToolFunctionNameSplit == null) {
				continue;
			}
			String engineId = responseProjectIdToolFunctionNameSplit[0];
			String origFunctionName = responseProjectIdToolFunctionNameSplit[1];

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

				if (mcpTool != null && hasText(mcpTool.opt("title"))) {
					responseToolMap.put("title", mcpTool.getString("title"));
				} else {
					// origFunctionName is already the name with the engine-id prefix removed
					responseToolMap.put("title", origFunctionName);
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

				if (mcpTool != null && mcpTool.has("_meta")) {
					Map<String, Object> toolMetaMap = mcpTool.getJSONObject("_meta").toMap();
					currentMeta.put(SMSS_MCP_EXECUTION, getValidMcpExecution(toolMetaMap));
					JSONObject uiMeta = getValidMcpUI(toolMetaMap);
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
	 * Whether the value is a non-blank string. Tool metadata arrives from JSON, so
	 * a key can be present while holding null, a blank string, or a non-string.
	 */
	private static boolean hasText(Object value) {
		return value instanceof String && !((String) value).isBlank();
	}

	/**
	 * Materializes schema defaults that a model omitted from a tool call. JSON
	 * Schema's {@code default} is only an annotation, so not every provider echoes
	 * pinned routing inputs such as recording_file and project_id. Applying only
	 * missing top-level values preserves every value the model/user supplied.
	 */
	@SuppressWarnings("unchecked")
	private static void applyMissingArgumentDefaults(Map<String, Object> responseToolMap,
			Map<String, Object> toolEntry) {
		Object rawSchema = toolEntry.get("inputSchema");
		if (!(rawSchema instanceof Map)) {
			return;
		}
		Object rawProperties = ((Map<String, Object>) rawSchema).get("properties");
		if (!(rawProperties instanceof Map)) {
			return;
		}

		Object rawArguments = responseToolMap.get("arguments");
		Map<String, Object> arguments = rawArguments instanceof Map ? (Map<String, Object>) rawArguments
				: new HashMap<>();
		for (Map.Entry<String, Object> propertyEntry : ((Map<String, Object>) rawProperties).entrySet()) {
			if (arguments.containsKey(propertyEntry.getKey()) || !(propertyEntry.getValue() instanceof Map)) {
				continue;
			}
			Map<String, Object> property = (Map<String, Object>) propertyEntry.getValue();
			if (property.containsKey("default")) {
				arguments.put(propertyEntry.getKey(), property.get("default"));
			} else if (property.containsKey("const")) {
				arguments.put(propertyEntry.getKey(), property.get("const"));
			}
		}
		responseToolMap.put("arguments", arguments);
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
		return MCPFactory.build(engine).getMCPTools();
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
				classLogger.error("Unable to write mcp definitions to '{}'", pythonJsonFileLoc, e);
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
			} catch (JSONException e) {
				classLogger.error("Unable to parse json file '{}'", jsonFileLoc, e);
			} catch (IOException e) {
				classLogger.error("Unable to read json file '{}'", jsonFileLoc, e);
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
			} catch (JSONException e) {
				classLogger.error("Unable to parse node '{}' from json file '{}'", node, jsonFileLoc, e);
			} catch (IOException e) {
				classLogger.error("Unable to read json file '{}'", jsonFileLoc, e);
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
	private static String getValidMcpExecution(Map<String, Object> toolMeta) {
		if (toolMeta == null) {
			return MCPExecution.ASK.getValue();
		}
		Object val = toolMeta.get(SMSS_MCP_EXECUTION);
		String valueString = (val == null) ? null : val.toString();
		MCPExecution exec = MCPExecution.fromValue(valueString);
		return exec != null ? exec.getValue() : MCPExecution.ASK.getValue();
	}

	/**
	 *
	 * @param toolMeta
	 * @return
	 */
	private static JSONObject getValidMcpUI(Map<String, Object> toolMeta) {
		if (toolMeta == null) {
			return null;
		}
		Object val = toolMeta.get(SMSS_MCP_UI);
		if (val == null) {
			return null;
		}
		JSONObject uiJson;
		if (val instanceof JSONObject) {
			uiJson = (JSONObject) val;
		} else if (val instanceof Map) {
			uiJson = new JSONObject((Map<?, ?>) val);
		} else {
			return null;
		}

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

		if (uiJson.has(UI_AUTO_OPEN) && !uiJson.isNull(UI_AUTO_OPEN)) {
			validUiJson.put(UI_AUTO_OPEN, uiJson.getBoolean(UI_AUTO_OPEN));
		}

		return validUiJson;
	}

	/**
	 * Stamps {@link #SMSS_MCP_GENERATOR} onto every tool in the array. Call it on
	 * the finished array so a generator with more than one build path marks all of
	 * them.
	 *
	 * @param tools       the generated tools, modified in place
	 * @param generatorId the generator, conventionally the reactor name without the
	 *                    "Reactor" suffix
	 */
	public static void stampGenerator(JSONArray tools, String generatorId) {
		if (tools == null) {
			return;
		}
		for (int i = 0; i < tools.length(); i++) {
			JSONObject tool = tools.optJSONObject(i);
			if (tool == null) {
				continue;
			}
			JSONObject meta = tool.optJSONObject("_meta");
			if (meta == null) {
				meta = new JSONObject();
				tool.put("_meta", meta);
			}
			meta.put(SMSS_MCP_GENERATOR, generatorId);
		}
	}

	/**
	 * Reads an existing {@code *_mcp.json}. A malformed file is logged and treated
	 * as absent, since nothing can load it in that state.
	 *
	 * @param filePath full path to the definition file
	 * @return the parsed file, or null when it is missing or unusable
	 */
	public static JSONObject readMcpJson(String filePath) {
		File existing = new File(filePath);
		if (!existing.isFile()) {
			return null;
		}
		try {
			return new JSONObject(FileUtils.readFileToString(existing, StandardCharsets.UTF_8));
		} catch (Exception e) {
			classLogger.warn("Existing {} could not be parsed and will be replaced: {}", filePath, e.getMessage());
			return null;
		}
	}

	/**
	 * Merges generated tools into the tools already in a definition file.
	 *
	 * <p>
	 * A generated tool replaces any existing tool of the same name. A tool stamped
	 * with {@code generatorId} that was not regenerated is dropped when
	 * {@code completeRegeneration} is set. Everything else is kept.
	 *
	 * @param existingMcpJson      the parsed existing file, or null when there is
	 *                             none
	 * @param generated            the generated tools, already stamped by
	 *                             {@link #stampGenerator}, in their given order
	 * @param generatorId          the generator whose own output may be replaced
	 * @param completeRegeneration true when {@code generated} is this generator's
	 *                             entire output. Pass false for a run scoped to a
	 *                             subset, which would otherwise prune the
	 *                             generator's other tools.
	 * @return generated tools followed by the surviving existing tools
	 */
	public static JSONArray mergeGeneratedTools(JSONObject existingMcpJson, JSONArray generated, String generatorId,
			boolean completeRegeneration) {
		JSONArray merged = new JSONArray();
		Set<String> generatedNames = new HashSet<>();
		for (int i = 0; i < generated.length(); i++) {
			JSONObject tool = generated.getJSONObject(i);
			merged.put(tool);
			String name = tool.optString("name", null);
			if (name != null) {
				generatedNames.add(name);
			}
		}

		JSONArray existing = existingMcpJson != null ? existingMcpJson.optJSONArray("tools") : null;
		if (existing == null) {
			return merged;
		}
		for (int i = 0; i < existing.length(); i++) {
			JSONObject tool = existing.optJSONObject(i);
			if (tool == null) {
				continue;
			}
			if (generatedNames.contains(tool.optString("name", null))) {
				continue;
			}
			JSONObject meta = tool.optJSONObject("_meta");
			if (completeRegeneration && meta != null && generatorId.equals(meta.optString(SMSS_MCP_GENERATOR, null))) {
				continue;
			}
			merged.put(tool);
		}
		return merged;
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

	/**
	 * Resolves the engine or project, enforces user access, normalizes the tool
	 * name, and executes the tool. Shared by RunMCPToolReactor (direct calls) and
	 * AgentToolDecisionHandler (HITL approve/edit).
	 */
	public static Object executeTool(String engineId, String toolName, Map<String, Object> paramMap, Insight insight) {
		// A room's toolbox is backed by its own asset folder, so there is no engine or
		// project to resolve.
		if (ROOM_MCP_ID.equals(engineId)) {
			if (insight == null) {
				throw new IllegalArgumentException("Caller insight is required to execute a room MCP tool");
			}
			InternalMCP roomMcp = InternalMCP.genFromRoomFolder(insight.getInsightFolder());
			String cleaned = removeEngineIdFromToolsMethodName(engineId, toolName);
			return roomMcp.callTool(cleaned, paramMap, insight);
		}

		IEngine engine = null;
		try {
			engine = Utility.getEngine(engineId);
		} catch (Exception ex) {
			// fall through to the project lookup
		}
		if (engine == null) {
			engine = Utility.getProject(engineId);
		}
		checkEngineAccess(engine, insight.getUser());

		if (toolName == null || (toolName = toolName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Tool name must be passed in to execute the mcp tool");
		}
		toolName = removeEngineIdFromToolsMethodName(engine.getEngineId(), toolName);

		IMCP mcp = MCPFactory.build(engine);
		return mcp.callTool(toolName, paramMap, insight);
	}

	/**
	 * Typed adapter for agent callers. The established direct execution path and
	 * its exception behavior remain unchanged.
	 */
	public static ToolExecutionResult executeToolResult(String engineId, String toolName, Map<String, Object> paramMap,
			Insight insight) {
		try {
			return ToolExecutionResult.success(executeTool(engineId, toolName, paramMap, insight));
		} catch (RuntimeException e) {
			String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			return ToolExecutionResult.error(message, message);
		}
	}

	// mirrors AbstractReactor.checkEngineEditSecurity for non-reactor callers
	private static void checkEngineAccess(IEngine engine, User user) {
		if (engine == null) {
			throw new NullPointerException("Engine/Project is null");
		}
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			AbstractReactor.throwAnonymousUserError();
		}
		if (engine instanceof IProject) {
			if (!SecurityProjectUtils.userCanViewProject(user, engine.getEngineId())) {
				throw new IllegalArgumentException(
						"Project " + engine.getEngineId() + " does not exist or user does not have access");
			}
		} else {
			if (!SecurityEngineUtils.userCanViewEngine(user, engine.getEngineId())) {
				throw new IllegalArgumentException(
						"Engine " + engine.getEngineId() + " does not exist or user does not have access");
			}
		}
	}

	private MCPUtility() {

	}
}
