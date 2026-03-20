package prerna.reactor.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GenerateDescriptionForMcpReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateDescriptionForMcpReactor.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public GenerateDescriptionForMcpReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() + "," + ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.MODEL.getKey(), "toolName" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = insight.getUser();
		String engineId = this.keyValue.get(this.keysToGet[0].split(",")[0]);
		if (engineId == null || engineId.isEmpty()) {
			engineId = insight.getContextProjectId();
			if (engineId == null || engineId.isEmpty()) {
				engineId = insight.getProjectId();
			}
		}
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the engine/project id or set the app context");
		}

		// get engine
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
			throw new NullPointerException("Unknown engine or project with id " + engineId);
		}
		String modelEngineId = keyValue.get(ReactorKeysEnum.MODEL.getKey());
		String toolName = keyValue.get("toolName");

		if (modelEngineId == null || (modelEngineId = modelEngineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Model engineId is required");
		}

		try {
			IEngine.CATALOG_TYPE engineType = engine.getCatalogType();

			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}

			if (engineType == CATALOG_TYPE.PROJECT) {
				if (!SecurityProjectUtils.userCanViewProject(user, engineId)) {
					throw new IllegalArgumentException(
							"Project " + engineId + " does not exist or user does not have access");
				}
			} else {
				if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
					throw new IllegalArgumentException(
							"Engine " + engineId + " does not exist or user does not have access");
				}
			}
			String engineName = engine.getEngineName();

			Map<String, Object> mcpJson = readMcpJsonFile(engineId, engineType, engineName);

			List<Map<String, Object>> tools = extractTools(mcpJson);
			Map<String, Object> targetTool = findToolByName(tools, toolName);

			// Read Python driver file if it exists
			String pythonCode = readPythonDriverFile(engineId, engineType, engineName, toolName);

			// Build context-aware prompt with proper JSON stringification
			String prompt = buildDescriptionPrompt(targetTool, pythonCode);

			IModelEngine modelEngine = Utility.getModel(modelEngineId);

			Map<String, Object> params = new HashMap<>();
			params.put("temperature", 0.3);
			params.put("max_completion_tokens", 2000);

			Map<String, Object> response = modelEngine.ask(prompt, null, insight, params).toMap();

			Map<String, Object> generated = parseGeneratedDescriptions(response.get("response"), targetTool);

			return new NounMetadata(generated, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);

		} catch (Exception e) {
			classLogger.error("MCP description generation failed", e);
			return new NounMetadata("Failed to generate MCP description: " + e.getMessage(), PixelDataType.CONST_STRING,
					PixelOperationType.ERROR);
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> parseGeneratedDescriptions(Object llmResponse, Map<String, Object> originalTool)
			throws Exception {

		if (llmResponse == null) {
			throw new Exception("LLM returned null response");
		}

		Map<String, Object> generated;

		// Handle different response types
		if (llmResponse instanceof Map<?, ?>) {
			generated = (Map<String, Object>) llmResponse;
		} else {
			try {
				String responseStr = llmResponse.toString().trim();

				int jsonStart = responseStr.indexOf("{");
				int jsonEnd = responseStr.lastIndexOf("}") + 1;

				if (jsonStart >= 0 && jsonEnd > jsonStart) {
					String jsonStr = responseStr.substring(jsonStart, jsonEnd);
					generated = MAPPER.readValue(jsonStr, Map.class);
				} else {
					throw new Exception("No JSON found in LLM response: " + responseStr);
				}
			} catch (Exception e) {
				classLogger.error("Failed to parse LLM response as JSON", e);
				throw e;
			}
		}

		Map<String, Object> result = new LinkedHashMap<>();

		result.put("tool_name", originalTool.get("name"));

		Map<String, Object> functionDesc = new LinkedHashMap<>();
		functionDesc.put("old", originalTool.get("description"));
		functionDesc.put("new", generated.get("function_description"));

		result.put("function_description", functionDesc);

		// Build parameter list with old and new descriptions
		List<Map<String, Object>> parametersWithOldAndNew = new ArrayList<>();

		// Get original parameters
		List<Map<String, Object>> originalParams = new ArrayList<>();
		Object inputSchemaObj = originalTool.get("inputSchema");
		if (inputSchemaObj != null && inputSchemaObj instanceof Map<?, ?>) {
			Map<String, Object> inputSchema = (Map<String, Object>) inputSchemaObj;
			Map<String, Object> properties = (Map<String, Object>) inputSchema.get("properties");

			if (properties != null) {
				for (Map.Entry<String, Object> entry : properties.entrySet()) {
					Map<String, Object> param = new LinkedHashMap<>();
					param.put("name", entry.getKey());
					Map<String, Object> paramDetails = (Map<String, Object>) entry.getValue();
					param.put("old_description", paramDetails.get("description"));
					originalParams.add(param);
				}
			}
		}

		// Get generated parameters
		Object generatedParamsObj = generated.get("parameters");
		Map<String, String> generatedParamMap = new HashMap<>();

		if (generatedParamsObj instanceof List<?>) {
			List<Map<String, Object>> generatedParams = (List<Map<String, Object>>) generatedParamsObj;
			for (Map<String, Object> param : generatedParams) {
				String paramName = (String) param.get("name");
				String description = (String) param.get("description");
				if (paramName != null && description != null) {
					generatedParamMap.put(paramName, description);
				}
			}
		}

		// Combine old and new descriptions
		for (Map<String, Object> originalParam : originalParams) {
			Map<String, Object> paramResult = new LinkedHashMap<>();
			String paramName = (String) originalParam.get("name");

			paramResult.put("name", paramName);
			paramResult.put("old", originalParam.get("old_description"));
			paramResult.put("new", generatedParamMap.getOrDefault(paramName, ""));

			parametersWithOldAndNew.add(paramResult);
		}

		result.put("parameters", parametersWithOldAndNew);

		return result;
	}

	private String buildDescriptionPrompt(Map<String, Object> targetTool, String pythonCode)
			throws JsonProcessingException {

		StringBuilder prompt = new StringBuilder();

		// Role Definition
		prompt.append(
				"""
						### ROLE
						You are a senior technical documentation specialist for MCP (Model Context Protocol) tools.
						Your objective is to create clear, comprehensive, and actionable tool descriptions that help users understand what the tool does and when to use it.

						""");

		// Context Section
		prompt.append("### CONTEXT\n");
		prompt.append(String.format("- **Tool Name**: %s\n", targetTool.get("name")));
		prompt.append(String.format("- **Current Description**: %s\n\n", targetTool.get("description")));

		// Add Python code context if available
		if (pythonCode != null && !pythonCode.trim().isEmpty()) {
			prompt.append("### PYTHON IMPLEMENTATION CODE\n");
			prompt.append("Below is the actual Python implementation of this tool. Analyze it to understand:\n");
			prompt.append("- What the function actually does\n");
			prompt.append("- What parameters it accepts and how they are used\n");
			prompt.append("- What operations, transformations, or API calls it performs\n");
			prompt.append("- What business logic or workflows it implements\n\n");
			prompt.append("``python\n");
			prompt.append(pythonCode);
			prompt.append("\n```\n\n");
		}

		// Input Schema Context
		Object inputSchemaObj = targetTool.get("inputSchema");
		if (inputSchemaObj != null && inputSchemaObj instanceof Map<?, ?>) {
			@SuppressWarnings("unchecked")
			Map<String, Object> inputSchema = (Map<String, Object>) inputSchemaObj;

			prompt.append("### INPUT PARAMETERS\n");

			@SuppressWarnings("unchecked")
			Map<String, Object> properties = (Map<String, Object>) inputSchema.get("properties");

			if (properties != null && !properties.isEmpty()) {
				prompt.append("The tool accepts the following parameters:\n\n");

				for (Map.Entry<String, Object> entry : properties.entrySet()) {
					String paramName = entry.getKey();
					@SuppressWarnings("unchecked")
					Map<String, Object> paramDetails = (Map<String, Object>) entry.getValue();

					prompt.append(String.format("- **%s**", paramName));

					if (paramDetails.containsKey("type")) {
						prompt.append(String.format(" (type: %s)", paramDetails.get("type")));
					}

					if (paramDetails.containsKey("description")) {
						prompt.append(String.format(": %s", paramDetails.get("description")));
					}

					if (paramDetails.containsKey("required") && Boolean.TRUE.equals(paramDetails.get("required"))) {
						prompt.append(" [REQUIRED]");
					}

					prompt.append("\n");
				}
			} else {
				prompt.append("No parameter schema information provided.\n");
			}
		}

		// Guidelines
		prompt.append(
				"""

						### INSTRUCTIONS
						Generate concise, specific descriptions following these rules:

						1. **Function Description** (2-4 sentences MAX):
						   - Start with: "This tool [specific action verb]..." (e.g., "retrieves", "calculates", "transforms", "validates")
						   - Describe ONLY what the code actually does - no marketing fluff
						   - Mention specific APIs, services, or data sources if present in code
						   - State the return value/output format
						   - AVOID: Generic terms like "enables", "allows users to", "suitable for", "powerful", "robust"
						   - AVOID: Listing hypothetical use cases unless explicitly coded


						2. **Parameter Descriptions** (1 sentence each):
						   - Format: "[Parameter name] specifies [what it controls]. [Constraints/format if relevant]."
						   - AVOID: "This parameter allows users to...", "Used for the purpose of..."
						   - Include type info and defaults from function signature

						3. **STRICT PROHIBITIONS**:
						   - NO buzzwords: "empower", "enable", "seamless", "robust", "comprehensive"
						   - NO hypothetical use cases: "can be used for", "ideal for", "perfect for"
						   - NO redundancy: Don't repeat the function name or obvious facts
						   - NO length padding: Every word must add information

						""");

		// Output Format
		prompt.append(
				"""
						### OUTPUT FORMAT
						Return ONLY a valid JSON object. No conversational text or markdown explanation outside the JSON block.

						CRITICAL LENGTH CONSTRAINTS:
						- function_description: MAX 4 sentences, ideally 2-3
						- Each parameter description: MAX 2 sentences, ideally 1
						- Total response should be concise - if you're writing more than 150 words, you're being too verbose

						```json
						{
						  "function_description": "Concise 2-4 sentence description focusing on actual code behavior",
						  "parameters": [
						    {
						      "name": "parameterName",
						      "description": "Direct 1-sentence description of what this parameter controls"
						    }
						  ]
						}
						```

						If context is insufficient, still return valid JSON using the same keys; put the limitation reason in an "explanation" field.

						REMEMBER: Be direct, specific, and brief. Describe what the code DOES, not what it could be used for.
						""");

		return prompt.toString();
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> extractTools(Map<String, Object> mcpJson) {

		Object toolsObj = mcpJson.get("tools");

		if (toolsObj == null) {
			throw new IllegalStateException("'tools' key not found in MCP JSON");
		}

		if (!(toolsObj instanceof List)) {
			throw new IllegalStateException("'tools' is not a valid array");
		}

		return (List<Map<String, Object>>) toolsObj;
	}

	/**
	 * Read MCP configuration from JSON file
	 * 
	 * @param engineName
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> readMcpJsonFile(String engineId, IEngine.CATALOG_TYPE engineType, String engineName)
			throws IOException {

		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId, engineName);

		if (assetsFolder == null || assetsFolder.trim().isEmpty()) {
			throw new IOException("Unable to resolve engine assets folder for engine: " + engineId);
		}

		File mcpDir = new File(assetsFolder, "mcp");

		if (!mcpDir.exists() || !mcpDir.isDirectory()) {
			throw new IOException("MCP directory not found: " + mcpDir.getAbsolutePath());
		}

		File pixelFile = new File(mcpDir, "pixel_mcp.json");
		File pyFile = new File(mcpDir, "py_mcp.json");

		File mcpFile = null;

		if (pixelFile.exists() && pixelFile.isFile()) {
			mcpFile = pixelFile;
		} else if (pyFile.exists() && pyFile.isFile()) {
			mcpFile = pyFile;
		} else {
			throw new IOException(
					"No MCP configuration file found (pixel_mcp.json or py_mcp.json) in: " + mcpDir.getAbsolutePath());
		}

		try (InputStream is = new FileInputStream(mcpFile)) {
			return MAPPER.readValue(is, Map.class);
		}
	}

	/**
	 * Read Python driver file (mcp_driver.py) and extract the specific function
	 * code
	 * 
	 * @param engineId
	 * @param engineType
	 * @param engineName
	 * @param toolName
	 * @return Python function code or null if not found
	 */
	private String readPythonDriverFile(String engineId, IEngine.CATALOG_TYPE engineType, String engineName,
			String toolName) {

		try {
			String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId, engineName);

			if (assetsFolder == null || assetsFolder.trim().isEmpty()) {
				classLogger.warn("Unable to resolve engine assets folder for engine: " + engineId);
				return null;
			}

			File pyDir = new File(assetsFolder, "py");

			if (!pyDir.exists() || !pyDir.isDirectory()) {
				classLogger.debug("Py directory not found: " + pyDir.getAbsolutePath());
				return null;
			}

			File pythonDriverFile = new File(pyDir, "mcp_driver.py");

			if (!pythonDriverFile.exists() || !pythonDriverFile.isFile()) {
				classLogger.debug("Python driver file not found: " + pythonDriverFile.getAbsolutePath());
				return null;
			}

			// Read the entire Python file
			String pythonCode = new String(Files.readAllBytes(pythonDriverFile.toPath()), StandardCharsets.UTF_8);

			// Extract the specific function matching toolName
			String extractedFunction = extractPythonFunction(pythonCode, toolName);

			if (extractedFunction != null && !extractedFunction.trim().isEmpty()) {
				classLogger.info("Successfully extracted Python function '" + toolName + "' from mcp_driver.py");
				return extractedFunction;
			} else {
				classLogger.warn("Function '" + toolName + "' not found in mcp_driver.py");
				return null;
			}

		} catch (Exception e) {
			classLogger.warn("Error reading Python driver file", e);
			return null;
		}
	}

	/**
	 * Extract a specific function from Python code by name
	 * 
	 * @param pythonCode   Full Python code
	 * @param functionName Name of the function to extract
	 * @return The function code or null if not found
	 */
	private String extractPythonFunction(String pythonCode, String functionName) {
		if (pythonCode == null || pythonCode.trim().isEmpty()) {
			return null;
		}

		// Pattern to match Python function definition
		// Matches: def function_name(params): with optional return type
		Pattern pattern = Pattern
				.compile("(^def\\s+" + Pattern.quote(functionName) + "\\s*\\([^)]*\\)\\s*->\\s*[^:]*:.*)|(^def\\s+"
						+ Pattern.quote(functionName) + "\\s*\\([^)]*\\)\\s*:)", Pattern.MULTILINE);

		Matcher matcher = pattern.matcher(pythonCode);

		if (!matcher.find()) {
			return null;
		}

		int startIdx = matcher.start();
		String firstLine = matcher.group(0);

		int baseIndent = firstLine.length() - firstLine.replaceFirst("^\\s*", "").length();

		StringBuilder functionCode = new StringBuilder();
		functionCode.append(firstLine).append("\n");

		// Continue reading lines until we hit another top-level definition
		int currentPos = matcher.end();

		while (currentPos < pythonCode.length()) {
			// Find next newline
			int nextNewline = pythonCode.indexOf('\n', currentPos);

			if (nextNewline == -1) {
				// End of file - add remaining content
				String remainingLine = pythonCode.substring(currentPos).trim();
				if (!remainingLine.isEmpty()) {
					functionCode.append(pythonCode.substring(currentPos));
				}
				break;
			}

			String line = pythonCode.substring(currentPos, nextNewline);

			// Check if line is empty or just whitespace - always include blank lines within
			// function
			if (line.trim().isEmpty()) {
				functionCode.append(line).append("\n");
				currentPos = nextNewline + 1;
				continue;
			}

			int currentIndent = line.length() - line.replaceFirst("^\\s*", "").length();

			String trimmedLine = line.trim();
			if (currentIndent <= baseIndent && (trimmedLine.startsWith("def ") || trimmedLine.startsWith("class ")
					|| trimmedLine.startsWith("import ") || trimmedLine.startsWith("from "))) {
				break;
			}

			if (currentIndent <= baseIndent && !line.substring(baseIndent).trim().isEmpty()) {
				break;
			}

			functionCode.append(line).append("\n");
			currentPos = nextNewline + 1;
		}

		String result = functionCode.toString().replaceAll("\\n+$", "").trim();
		return result;
	}

	/**
	 * Find specific tool by name in MCP JSON tools array
	 */
	private Map<String, Object> findToolByName(List<Map<String, Object>> tools, String toolName) {

		if (tools == null || tools.isEmpty()) {
			throw new IllegalArgumentException("Tools list is empty");
		}

		for (Map<String, Object> tool : tools) {

			Object nameObj = tool.get("name");

			if (nameObj != null && toolName.equals(nameObj.toString())) {
				return tool;
			}
		}

		throw new IllegalArgumentException("Tool not found: " + toolName);
	}

	/**
	 * Get list of strings from reactor keys
	 */
	@Override
	protected List<String> getListString(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> stringInputs = grs.getNounsOfType(PixelDataType.CONST_STRING);
			if (stringInputs != null && !stringInputs.isEmpty()) {
				return stringInputs.stream().map(n -> (String) n.getValue()).collect(Collectors.toList());
			}
		}
		return null;
	}

}
