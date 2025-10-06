package prerna.reactor.agent.mcp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossMCPException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public final class MCPUtility {

	private static final Logger classLogger = LogManager.getLogger(MCPUtility.class);

	public static final String SMSS_PROJECT_ID = "SMSS_PROJECT_ID";
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
	public static String runPythonTool(IProject project, Insight insight, String functionName,
			JSONObject functionProperties, Map<String, Object> paramMap) {
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectId());

		// load the path to have access to the file
		String pyFolderLoc = projectAssetFolder + "/py";
		boolean namedMCP = true;
		{
			File mcpDriver = new File(pyFolderLoc + "/" + MCP_PY_FILE_NAME);
			if (!mcpDriver.exists()) {
				namedMCP = false;
			}
		}
		String sysImport = "import sys";
		String getpath = "sys.path";
		pyFolderLoc = pyFolderLoc.replace("\\", "/");
		String setpath = "sys.path.insert(0,'" + pyFolderLoc + "')";
		String importSmssIfNeeded = null;
		if (namedMCP) {
			importSmssIfNeeded = "if 'smss' not in globals():\n" + "    import mcp_driver as smss";
		} else {
			classLogger.warn("Using legacy {} python file name - needs to be updated to {}", LEGACY_PY_FILE_NAME,
					MCP_PY_FILE_NAME);
			importSmssIfNeeded = "if 'smss' not in globals():\n" + "    import smss_driver as smss";
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
				propValue = "None";
			}
			// while we do have the type, the propValue is much better at sending
			// appropriate python syntax
			paramString.append(propName).append("=").append(PyUtils.determineStringType(propValue));
		}

		PyTranslator pyt = project.getProjectPyTranslator();

		String runMethod = "smss." + functionName + "(" + paramString + ")";
		classLogger.info("Running python tool '" + runMethod + "' from project " + project.getProjectId());
		String curPath = pyt.runScript(insight, sysImport, getpath) + "";
		curPath = curPath.replace("\\", "/");
		if (!curPath.contains(pyFolderLoc)) {
			pyt.runScript(insight, setpath);
		}

		// previous insight execution
//	    // Always import smss if needed
//	    insight.getPyTranslator().runScript(importSmssIfNeeded);
//	    // run method
//	    return insight.getPyTranslator().runScript(runMethod)+"";

		// Running via project py
		// Always import smss if needed
		pyt.runScript(insight, importSmssIfNeeded);
		// run method
		return pyt.runScript(insight, runMethod) + "";
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
	public static String runPixelTool(IProject project, Insight insight, String functionName,
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

				// handle scalar and arrays
				if (propValue instanceof List || propValue instanceof JSONArray) {
					// handle arrays/lists
					paramString.append(formatArrayValue(propValue, propType));
				} else {
					// handle single values
					if (propType.toUpperCase().contains("STR") && !propValue.toString().equals("None")) {
						paramString.append("'").append(propValue).append("'");
					} else {
						paramString.append(propValue);
					}
				}
			}
		}

		String runMethod = functionName + "(" + paramString + ");";
		if (project != null) {
			classLogger.info("Running pixel tool '" + runMethod + "' from project " + project.getProjectId());
		} else {
			classLogger.info("Running pixel tool '" + runMethod + "' directly without a project");
		}
		// run pixel
		PixelRunner pixelReturn = insight.runPixel(runMethod);
		NounMetadata result = pixelReturn.getResults().get(0);
		if (result.getOpType().contains(PixelOperationType.ERROR)) {
			throw new SemossMCPException(result.getValue() + "", MCPErrorCode.SERVER_ERROR);
		}
		return result.getValue() + "";
	}

	/**
	 * Format array values for pixel execution
	 * 
	 * @param arrayValue - the array value (List or JSONArray)
	 * @param propType   - the property type
	 * @return formatted string representation
	 */
	private static String formatArrayValue(Object arrayValue, String propType) {
		StringBuilder arrayString = new StringBuilder("[");

		if (arrayValue instanceof List) {
			List<?> list = (List<?>) arrayValue;
			for (int i = 0; i < list.size(); i++) {
				if (i > 0) {
					arrayString.append(", ");
				}

				Object item = list.get(i);
				if (propType.toUpperCase().contains("STR") && item != null && !item.toString().equals("None")) {
					arrayString.append("'").append(item).append("'");
				} else {
					arrayString.append(item);
				}
			}
		} else if (arrayValue instanceof JSONArray) {
			JSONArray jsonArray = (JSONArray) arrayValue;
			for (int i = 0; i < jsonArray.length(); i++) {
				if (i > 0) {
					arrayString.append(", ");
				}

				Object item = jsonArray.get(i);
				if (propType.toUpperCase().contains("STR") && item != null && !item.toString().equals("None")) {
					arrayString.append("'").append(item).append("'");
				} else {
					arrayString.append(item);
				}
			}
		}

		arrayString.append("]");
		return arrayString.toString();
	}

	/**
	 * 
	 * @param projectId
	 * @param jsonToolsMap
	 * @return
	 */
	public static JSONObject appendProjectIdToTooslMethodName(String projectId, JSONObject jsonToolsMap) {
		if (jsonToolsMap == null || !jsonToolsMap.has("tools")) {
			return jsonToolsMap;
		}

		JSONArray toolsArray = jsonToolsMap.getJSONArray("tools");
		for (int i = 0; i < toolsArray.length(); i++) {
			JSONObject toolMap = toolsArray.getJSONObject(i);
			String currentName = toolMap.getString("name");
			toolMap.put("name", "a" + projectId + "_" + currentName);
		}
		return jsonToolsMap;
	}

	/**
	 * 
	 * @param projectId
	 * @param functionName
	 * @return
	 */
	public static String removeProjectIdFromToolsMethodName(String projectId, String functionName) {
		String internalFunctionNamePrefix = "a" + projectId + "_";
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
	public static String[] parseProjectIdFromFunctionName(String input) {
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
	 * Appends a parameter for the SMSS_PROJECT_ID for each tool
	 * 
	 * @param projectId
	 * @param jsonToolsMap
	 * @return
	 */
	public static JSONObject appendProjectIdToToolsArgs(String projectId, JSONObject jsonToolsMap) {
		if (jsonToolsMap == null || !jsonToolsMap.has("tools")) {
			return jsonToolsMap;
		}

		JSONArray toolsArray = jsonToolsMap.getJSONArray("tools");
		for (int i = 0; i < toolsArray.length(); i++) {
			JSONObject toolMap = toolsArray.getJSONObject(i);
			if (!toolMap.has("inputSchema")) {
				toolMap.put("inputSchema", new JSONObject());
			}

			JSONObject inputSchema = toolMap.getJSONObject("inputSchema");
			if (!inputSchema.has("properties")) {
				inputSchema.put("properties", new JSONObject());
			}

			JSONObject properties = inputSchema.getJSONObject("properties");

			// add an enum with a single value for this field
			JSONObject smssProjectId = new JSONObject();
			smssProjectId.put("type", "string");
			smssProjectId.put("enum", new JSONArray());
			smssProjectId.getJSONArray("enum").put(projectId);
			smssProjectId.put("description",
					"This is a required id field. Always return the enum's single value of '" + projectId + "'");
			properties.put(SMSS_PROJECT_ID, smssProjectId);

			// now add in the required
			if (!inputSchema.has("required")) {
				inputSchema.put("required", new JSONArray());
			}
			inputSchema.getJSONArray("required").put(SMSS_PROJECT_ID);
		}
		return jsonToolsMap;
	}

	/**
	 * 
	 * @param response
	 */
	public static void updateToolResponseWithProjectMeta(ResponseMessage response) {
		Map<String, JSONObject> mcpToolsJsonCache = new HashMap<>();
		List<Map<String, Object>> toolResponses = response.getToolResponses();
		for (int toolResponseIndex = 0; toolResponseIndex < toolResponses.size(); toolResponseIndex++) {
			Map<String, Object> responseToolMap = toolResponses.get(toolResponseIndex);
			// we start the function name with _projectid_ so lets remove that
			String responseProjectIdToolFunctionName = (String) responseToolMap.get("name");
			String[] responseProjectIdToolFunctionNameSplit = parseProjectIdFromFunctionName(
					responseProjectIdToolFunctionName);
			if (responseProjectIdToolFunctionNameSplit == null) {
				// if the tool function doesn't start with _projectid_
				// then this response is already in proper format for the FE
				continue;
			}
			String projectId = responseProjectIdToolFunctionNameSplit[0];
			String origFunctionName = responseProjectIdToolFunctionNameSplit[1];

			// now that we have the projectId
			// lets append some of the mcp metadata back into the response

			JSONObject mcpToolsJson = mcpToolsJsonCache.get(projectId);
			if (mcpToolsJson == null) {
				IProject project = Utility.getProject(projectId);
				if (project == null) {
					// technically speaking you could have a function start with _
					// but will assume this is in proper format
					continue;
				}
				mcpToolsJson = MCPUtility.getAggregatedTools(project);
				mcpToolsJsonCache.put(projectId, mcpToolsJson);
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
					responseToolMap.put("title", mcpTool.getString("title"));
				}

				if (mcpToolsJson.has("_meta")) {
					responseToolMap.put("_meta", mcpToolsJson.get("_meta"));
				}
			}
		}
	}

	/**
	 * 
	 * @param toolStep
	 */
	public static void updateCOTToolStepWithProjectMeta(Map<String, Object> toolStep) {
		Map<String, JSONObject> mcpToolsJsonCache = new HashMap<>();

		String responseProjectIdToolFunctionName = (String) toolStep.get("tool_name");
		String[] responseProjectIdToolFunctionNameSplit = parseProjectIdFromFunctionName(
				responseProjectIdToolFunctionName);
		if (responseProjectIdToolFunctionNameSplit == null) {
			// if the tool function doesn't start with _projectid_
			// then this response is already in proper format for the FE
			return;
		}
		String projectId = responseProjectIdToolFunctionNameSplit[0];
		String origFunctionName = responseProjectIdToolFunctionNameSplit[1];

		// now that we have the projectId
		// lets append some of the mcp metadata back into the response

		JSONObject mcpToolsJson = mcpToolsJsonCache.get(projectId);
		if (mcpToolsJson == null) {
			IProject project = Utility.getProject(projectId);
			if (project == null) {
				// technically speaking you could have a function start with _
				// but will assume this is in proper format
				return;
			}
			mcpToolsJson = MCPUtility.getAggregatedTools(project);
			mcpToolsJsonCache.put(projectId, mcpToolsJson);
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
				toolStep.put("title", mcpTool.getString("title"));
			}

			if (mcpToolsJson.has("_meta")) {
				toolStep.put("_meta", mcpToolsJson.get("_meta"));
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
	public static JSONObject getAggregatedTools(IProject project) {
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectId());
		String pythonJsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		String pixelJsonFileLoc = projectAssetFolder + "/mcp/pixel_mcp.json";

		JSONObject toolMap = new JSONObject();
		JSONArray toolsArray = new JSONArray();
		toolsArray.putAll(MCPUtility.getNode(pythonJsonFileLoc, "tools"));
		toolsArray.putAll(MCPUtility.getNode(pixelJsonFileLoc, "tools"));
		toolMap.put("tools", toolsArray);

		// add in meta as well
		JSONObject _meta = new JSONObject();
		_meta.put(MCPUtility.SMSS_PROJECT_ID, project.getProjectId());
		_meta.put(MCPUtility.SMSS_PROJECT_NAME, project.getProjectName());
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
	public static JSONObject findPythonToolWithCellId(IProject project, String cellId) {
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectId());
		String pythonJsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";

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
		String script = """
				import smssutil
				code = '''%s'''
				smssutil.get_function_name_from_code(code)
				""".formatted(pythonCode.replace("'", "\\'"));
		String functionName = (String) insight.getPyTranslator().runDirectPy(script);
		return functionName;
	}

	/**
	 * Parse the python code in a file to find a file and remove it
	 * 
	 * @param insight
	 * @param filePath
	 * @param functionName
	 */
	public static void removeExistingFunctionFromPyFile(Insight insight, String filePath, String functionName) {
		String script = """
				import smssutil
				filePath = '''%s'''
				function_name = '%s'
				smssutil.remove_function_from_file(filePath, function_name)
				""".formatted(filePath.replace("\\", "/"), functionName);
		insight.getPyTranslator().runDirectPy(script);
	}

	private MCPUtility() {

	}
}
