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
package prerna.project.impl.notebook.v1_0_0_alpha;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.project.impl.notebook.INotebookHelper;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.NotebookExecution;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.gson.GsonUtility;

public class NotebookHelper implements INotebookHelper {

	private static final Logger classLogger = LogManager.getLogger(NotebookHelper.class);

	private JsonObject blocksFileJson = null;

	@Override
	public JsonElement getBlocksFileJson() {
		return this.blocksFileJson;
	}

	@Override
	public void setBlocksFileJson(JsonElement blocksFileJson) {
		try {
			this.blocksFileJson = blocksFileJson.getAsJsonObject();
		} catch (IllegalStateException e) {
			classLogger.error("Failed to parse blocks file json as a JsonObject: {}", e.getMessage(), e);
			throw new IllegalArgumentException("The json is not of the valid format for this version.", e);
		}
	}

	@Override
	public NotebookExecution executeNotebook(Insight insight, Map<String, String> inputReplacements) {
		Gson gson = GsonUtility.getDefaultGson();

		PixelRunner runner = new PixelRunner();

		Set<String> outputVariables = new HashSet<>();
		Map<String, Object> outputVariableMap = new HashMap<>();

		// grab all the values for replacement
		Map<String, String> idToVariable = new HashMap<>();
		Map<String, String> replacements = new HashMap<>();
		JsonObject variables = blocksFileJson.getAsJsonObject("variables");
		for (String varName : variables.keySet()) {
			JsonObject varMap = variables.get(varName).getAsJsonObject();
			if (varMap.has("value")) {
				replacements.put(varName, varMap.get("value").getAsString());
			} else {
				String cellType = varMap.get("type").getAsString();
				if (cellType.equalsIgnoreCase("cell")) {
					String cellId = varMap.get("cellId").getAsString();
					idToVariable.put(cellId, varName);
				} else {
					String pointer = varMap.get("to").getAsString();
					idToVariable.put(pointer, varName);
				}
			}

			boolean isOutput = false;
			if (varMap.has("isOutput")) {
				isOutput = varMap.get("isOutput").getAsBoolean();
			}
			if (isOutput) {
				outputVariables.add(varName);
			}
		}
		// add user defined replacements
		if (inputReplacements != null) {
			replacements.putAll(inputReplacements);
		}

		// determine the order of execution
		// we will use the executionOrder
		// otherwise, lets hope the order is correct from the notebook
		// keyset list
		Collection<String> notebookNames = null;
		JsonObject blocksNotebookMap = getNotebooksJson();
		JsonArray executionOrder = blocksFileJson.getAsJsonArray("executionOrder");
		if (executionOrder == null || executionOrder.isEmpty()) {
			notebookNames = blocksNotebookMap.keySet();
		} else {
			notebookNames = new ArrayList<>();
			for (JsonElement ele : executionOrder) {
				notebookNames.add(ele.getAsString());
			}
		}

		for (String notebookName : notebookNames) {
			// these are from the blocks json
			JsonObject blocksNotebook = blocksNotebookMap.getAsJsonObject(notebookName);
			String notebookId = blocksNotebook.get("id").getAsString();

			// store the final output for the notebook
			Object lastResultValue = null;
			String lastResultReplacement = null;

			// loop through all the cells in the notebook
			List<JsonElement> blocksCells = blocksNotebook.getAsJsonArray("cells").asList();
			for (JsonElement blocksCellEle : blocksCells) {
				JsonObject blocksCellObj = blocksCellEle.getAsJsonObject();
				String cellId = blocksCellObj.get("id").getAsString();

				JsonObject blocksParam = blocksCellObj.getAsJsonObject("parameters");

				String blockType = blocksParam.get("type").getAsString();
				String blockValue = blocksParam.get("code").getAsString();

				String pixel = null;
				if (blockType.equalsIgnoreCase("py")) {
					pixel = "Py(\"<encode>" + blockValue + "</encode>\");";
				} else if (blockType.equals("r")) {
					pixel = "R(\"<encode>" + blockValue + "</encode>\");";
				} else {
					// you are pixel
					pixel = blockValue;
				}

				String finalPixel = performReplacements(pixel, replacements);
				insight.runPixel(runner, finalPixel);

				List<NounMetadata> allResults = runner.getResults();
				NounMetadata lastResult = allResults.get(allResults.size() - 1);
				lastResultValue = lastResult.getValue();

				// we want to keep this logic to match the FE replacement logic
				List<PixelOperationType> opTypes = lastResult.getOpType();
				if (opTypes.contains(PixelOperationType.CODE_EXECUTION)
						|| opTypes.contains(PixelOperationType.VECTOR)) {
					lastResultValue = ((List) lastResult.getValue()).get(0);
					if (lastResultValue instanceof NounMetadata) {
						lastResultValue = ((NounMetadata) lastResultValue).getValue();
					}
				}

				lastResultReplacement = gson.toJson(lastResultValue);

				// store cellId to value
				if (idToVariable.containsKey(cellId)) {
					String pointer = idToVariable.get(cellId);
					replacements.put(pointer, lastResultReplacement);
					replacements.put(pointer + ".value", lastResultReplacement);
					if (outputVariables.contains(pointer)) {
						outputVariableMap.put(pointer, lastResultValue);
					}
				}
			}
			// store notebookId to last cell value
			if (idToVariable.containsKey(notebookId)) {
				String pointer = idToVariable.get(notebookId);
				replacements.put(pointer, lastResultReplacement);
				replacements.put(pointer + ".value", lastResultReplacement);
				if (outputVariables.contains(pointer)) {
					outputVariableMap.put(pointer, lastResultValue);
				}
			}
		}

		NotebookExecution execution = new NotebookExecution();
		execution.setRunner(runner);
		execution.setVariableOutput(outputVariableMap);
		return execution;
	}

	/**
	 * Return the map of notebooks from the blocks file. Prefers the
	 * {@code notebooks} key; falls back to the legacy {@code queries} key when
	 * {@code notebooks} is missing or empty. Always returns a non-null
	 * {@link JsonObject} — an empty object when neither key is present — so
	 * callers can iterate {@code keySet()} without a null check.
	 *
	 * @return the notebooks map, or an empty object if neither key is present
	 */
	private JsonObject getNotebooksJson() {
		JsonObject notebooks = blocksFileJson.getAsJsonObject("notebooks");
		if (notebooks != null && notebooks.size() > 0) {
			return notebooks;
		}
		JsonObject queries = blocksFileJson.getAsJsonObject("queries");
		if (queries != null) {
			return queries;
		}
		return new JsonObject();
	}

	/**
	 *
	 * @param pixel
	 * @param replacements
	 * @return
	 */
	private String performReplacements(String pixel, Map<String, String> replacements) {
		for (String replaceKey : replacements.keySet()) {
			pixel = pixel.replace("{{" + replaceKey + "}}", replacements.get(replaceKey));
		}
		return pixel;
	}

	@Override
	public Map<String, String> getBlocksEngineDependencies() {
		Map<String, String> engineMap = new HashMap<>();

		Set<String> validTypes = new HashSet<>(Arrays.asList("model", "database", "vector", "storage", "function"));

		JsonObject variables = blocksFileJson.getAsJsonObject("variables");
		for (String varName : variables.keySet()) {
			JsonObject varMap = variables.get(varName).getAsJsonObject();
			if (varMap.has("type")) {
				String type = varMap.get("type").getAsString();
				if (validTypes.contains(type)) {
					String value = INotebookHelper.UNDEFINED_VALUE;
					if (varMap.has("value")) {
						value = varMap.get("value").getAsString();
					}
					engineMap.put(varName, value);
				}
			}
		}

		return engineMap;
	}

	@Override
	public Map<String, String> getNotebookVariables() {
		Map<String, String> variableMap = new HashMap<>();

		JsonObject variables = blocksFileJson.getAsJsonObject("variables");
		for (String varName : variables.keySet()) {
			JsonObject varMap = variables.get(varName).getAsJsonObject();
			String value = INotebookHelper.UNDEFINED_VALUE;
			if (varMap.has("value")) {
				value = varMap.get("value").getAsString();
			}
			variableMap.put(varName, value);
		}

		return variableMap;
	}

	/**
	 * 
	 * @param mcpNotebookJson
	 * @param model
	 * @param insight
	 * @param cellId
	 * @return
	 */
	private List<PythonFunction> generatePythonFunctionsFromNotebook(JsonObject mcpNotebookJson, IModelEngine model,
			Insight insight, String cellId) {
		List<PythonFunction> functions = new ArrayList<>();

		JsonObject variables = blocksFileJson.getAsJsonObject("variables");
		Set<String> variableList = variables.keySet();

		JsonArray cells = mcpNotebookJson.getAsJsonArray("cells");
		for (int i = 0; i < cells.size(); i++) {
			JsonObject cell = cells.get(i).getAsJsonObject();
			String thisCellId = cell.get("id").getAsString();
			// are we filtering to a specific cell?
			// null cellId = process all cells
			if (cellId == null || thisCellId.equals(cellId)) {
				String widgetType = cell.get("widget").getAsString();
				if (widgetType.equals("code")) {

					PythonFunction function = new PythonFunction();
					function.setNotebookCellId(thisCellId);
					function.setMethodName("mcp_" + thisCellId);

					JsonObject parameters = cell.getAsJsonObject("parameters");
					String type = parameters.get("type").getAsString();
					String code = parameters.get("code").getAsString();

					String transformedCode = code;
					// Regular expression to match {{parameter_name}}
					Pattern pattern = Pattern.compile("\\{\\{([^}]+)\\}\\}");
					Matcher matcher = pattern.matcher(code);

					List<String> codeParameters = new ArrayList<>();
					List<String> definedCodeParameters = new ArrayList<>();
					while (matcher.find()) {
						// Extract the parameter name (group 1 contains the content inside {{}})
						String codeParameter = matcher.group(1).trim();
						codeParameters.add(codeParameter);
					}

					for (String codeParameter : codeParameters) {
						if (variableList.contains(codeParameter)) {
							JsonObject variableMap = variables.getAsJsonObject(codeParameter);
							if (variableMap.has("value")) {
								definedCodeParameters.add(codeParameter);
								transformedCode = transformedCode.replace("{{" + codeParameter + "}}",
										variableMap.get("value").getAsString());
							}
						}
					}

					if (transformedCode.contains("'${i}'")) {
						codeParameters.add("insight_id");
						transformedCode = transformedCode.replace("'${i}'", "insight_id");
					}

					codeParameters.removeAll(definedCodeParameters);
					function.setInputs(codeParameters);
					if (type.equals("py")) {
						function.setCode(transformedCode);
					} else if (type.equals("pixel")) {
						String pythonRunPixel = """
								from semoss import Insight
								insight = Insight()
								""";
						pythonRunPixel += "\ninsight.run_pixel(\"\"\"" + transformedCode.replace("\"", "\\\"")
								+ "\"\"\")";
						function.setCode(pythonRunPixel);
					}

					functions.add(function);
				}
			}
		}
		return functions;
	}

	/**
	 * 
	 * @param functions
	 * @param model
	 * @param insight
	 * @param filePath
	 * @param append
	 * @return
	 */
	private Map<String, String> writeFunctionsToFile(List<PythonFunction> functions, IModelEngine model,
			Insight insight, File file, boolean append) {
		Map<String, String> cellIdToFunctionName = new HashMap<>();

		try (FileWriter writer = new FileWriter(file, append)) {
			writer.write("\n\n");
			for (PythonFunction function : functions) {
				if (function.getCode() != null) {
					writer.write("# Auto generated method from cell id = '" + function.getNotebookCellId() + "' on "
							+ ZonedDateTime.now(ZoneId.of("UTC")) + "\n");
					if (model == null) {
						writer.write(function.createPythonFunctionSyntax());

						cellIdToFunctionName.put(function.getMethodName(), function.getNotebookCellId());
					} else {
						AskModelEngineResponse llmResponse = model.ask(
								PythonFunction.defaultLLMImprovePrompt() + function.createPythonFunctionSyntax(), null,
								insight, new HashMap<>());
						String llmStringResponse = llmResponse.getStringResponse();
						// because the LLM sometimes still adds stuff
						// lets parse out the code block
						String response = extractPythonCodeBlock(llmStringResponse);
						if (response != null) {
							// sometimes the response comes back with double encoding ...
							// TODO: investigate this more
							if (!response.contains("\n") && response.contains("\\n")) {
								response = response.replace("\\n", "\n");
							}
							writer.write(response);

							cellIdToFunctionName.put(MCPUtility.getPythonFunctionNameFromCode(insight, response),
									function.getNotebookCellId());
						} else {
							classLogger.warn(
									"Unable to properly get python markdown from LLM. Defaulting to base function code");
							writer.write(function.createPythonFunctionSyntax());

							cellIdToFunctionName.put(function.getMethodName(), function.getNotebookCellId());
						}
					}
					writer.write("\n\n");
				}
			}
		} catch (IOException e) {
			classLogger.error("Failed to write generated python functions to file '{}': {}", file.getAbsolutePath(),
					e.getMessage(), e);
		}
		return cellIdToFunctionName;
	}

	@Override
	public Map<String, String> transformNotebookToMcpDriver(String filePath, IModelEngine model, Insight insight) {
		// we will go through every cell in the mcp_driver notebook
		// and turn that into a function
		JsonObject notebooks = getNotebooksJson();
		JsonObject mcpNotebookJson = notebooks.getAsJsonObject(MCPUtility.MCP_NOTEBOOK_NAME);
		if (mcpNotebookJson == null) {
			// try legacy name
			mcpNotebookJson = notebooks.getAsJsonObject(MCPUtility.LEGACY_MCP_NOTEBOOK_NAME);
			classLogger.warn("Using legacy {} notebook name - needs to be updated to {}",
					MCPUtility.LEGACY_MCP_NOTEBOOK_NAME, MCPUtility.MCP_NOTEBOOK_NAME);
			if (mcpNotebookJson == null) {
				return null;
			}
		}

		List<PythonFunction> functions = generatePythonFunctionsFromNotebook(mcpNotebookJson, model, insight, null);

		// new file
		File f = new File(filePath);
		if (f.exists()) {
			f.delete();
		}
		f.getParentFile().mkdirs();

		// false = we are not appending
		return writeFunctionsToFile(functions, model, insight, f, false);
	}

	@Override
	public Map<String, String> transformNotebookCellToMcpDriver(String filePath, IModelEngine model, Insight insight,
			String cellId) {
		// we will go through every cell in the mcp_driver notebook
		// and turn that into a function
		JsonObject notebooks = getNotebooksJson();
		JsonObject mcpNotebookJson = notebooks.getAsJsonObject(MCPUtility.MCP_NOTEBOOK_NAME);
		if (mcpNotebookJson == null) {
			// try legacy name
			mcpNotebookJson = notebooks.getAsJsonObject(MCPUtility.LEGACY_MCP_NOTEBOOK_NAME);
			classLogger.warn("Using legacy {} notebook name - needs to be updated to {}",
					MCPUtility.LEGACY_MCP_NOTEBOOK_NAME, MCPUtility.MCP_NOTEBOOK_NAME);
			if (mcpNotebookJson == null) {
				return null;
			}
		}

		List<PythonFunction> functions = generatePythonFunctionsFromNotebook(mcpNotebookJson, model, insight, cellId);
		// if you are giving a specific cell
		// then we will not write/make a new file
		File f = new File(filePath);
		// but make the dirs in case it doesn't exist
		f.getParentFile().mkdirs();

		// true = we are appending
		return writeFunctionsToFile(functions, model, insight, f, true);
	}

	/**
	 * 
	 */
	class PythonFunction {

		String notebookCellId;
		String methodName;
		List<String> inputs;
		String code;

		public PythonFunction() {

		}

		public String getNotebookCellId() {
			return notebookCellId;
		}

		public void setNotebookCellId(String notebookCellId) {
			this.notebookCellId = notebookCellId;
		}

		public String getMethodName() {
			return methodName;
		}

		public void setMethodName(String methodName) {
			this.methodName = methodName;
		}

		public List<String> getInputs() {
			return inputs;
		}

		public void setInputs(List<String> inputs) {
			this.inputs = inputs;
		}

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}

		private String sanitizeName(String name) {
			return name.replace("-", "_").replace(".", "_");
		}

		public String createPythonFunctionSyntax() {
			if (code == null) {
				return "";
			}

			final String TAB = "\t";
			String parameters = null;

			Map<String, String> sanitizedInputs = new HashMap<>();
			if (inputs != null && !inputs.isEmpty()) {
				List<String> sanitizedNames = new ArrayList<>();
				for (String input : inputs) {
					String sanitized = sanitizeName(input);
					sanitizedInputs.put(input, sanitized);
					sanitizedNames.add(sanitized);
				}
				parameters = "(" + String.join(", ", sanitizedNames) + ")";
			} else {
				parameters = "()";
			}

			String finalCode = code;
			for (Map.Entry<String, String> entry : sanitizedInputs.entrySet()) {
				finalCode = finalCode.replace("{{" + entry.getKey() + "}}", entry.getValue());
			}

			// since we wrap within a function
			// add an additional indent to all lines of code
			String indentedCode = finalCode.replaceAll("\\R", "\n" + TAB);

			String[] lines = indentedCode.split("\n");
			// find the last non-empty line and prepend "return " to it
			for (int i = lines.length - 1; i >= 0; i--) {
				String line = lines[i];
				if (!line.trim().isEmpty()) {
					// find where the actual content starts
					int contentStart = 0;
					while (contentStart < line.length() && Character.isWhitespace(line.charAt(contentStart))) {
						contentStart++;
					}

					// split the indent and the content start
					String indent = line.substring(0, contentStart);
					String content = line.substring(contentStart);
					// modify the line after the indents to start with "return "
					lines[i] = indent + "return " + content;
					break;
				}
			}

			String modifiedCode = String.join("\n", lines);
			return "def " + methodName + parameters + ":" + "\n" + TAB + modifiedCode + "\n";
		}

		public static String defaultLLMImprovePrompt() {
			String prompt = """
					You are a python coding assistant. Inspect the provided python function.
					Please break out the inputs into variables in case they are within string inputs.
					Please provide a Google docstring and input types for the function.
					Ensure that the function has a return.
					Give the function a meaningful name that is under 20 characters long.
					Only reply with the code in markdown and make sure the syntax is executable with proper spacing:
					""";
			return prompt;
		}
	}

	/**
	 * 
	 * @param str
	 * @return
	 */
	public static String extractPythonCodeBlock(String str) {
		if (str == null) {
			return null;
		}

		// Pattern handles various formats:
		// ```python, ```Python, ``` python (with space), etc.
		Pattern pattern = Pattern.compile("```\\s*[Pp]ython\\s*\\n(.*?)\\n```", Pattern.DOTALL);
		Matcher matcher = pattern.matcher(str);

		if (matcher.find()) {
			return matcher.group(1).trim();
		}

		return null;
	}
}
