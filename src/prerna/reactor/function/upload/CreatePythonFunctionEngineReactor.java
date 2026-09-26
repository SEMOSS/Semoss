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
package prerna.reactor.function.upload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.engine.AbstractEngineFileReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreatePythonFunctionEngineReactor extends AbstractEngineFileReactor {

	private static final Logger classLogger = LogManager.getLogger(CreatePythonFunctionEngineReactor.class);

	public CreatePythonFunctionEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.FUNCTION_DETAILS.getKey(),
				ReactorKeysEnum.CONTENT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		validateUserAndEngineAccess(user);
		// validate we have not restricted this to only admins
		if (AbstractSecurityUtils.adminOnlyFunctionAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();

		String functionEngineName = getStringFromKeyOrCurRowStringValue(ReactorKeysEnum.FUNCTION.getKey());
		if (functionEngineName == null || (functionEngineName = functionEngineName.trim()).isEmpty()) {
			throw new NullPointerException("Must define the name of the new function engine");
		}
		// Generate unique engine ID and formatted name
		String functionName = toUpperCamelCase(functionEngineName);
		// if function name is not valid, throw error
		if (!Utility.validateName(functionName)) {
			// error and redirect to try again
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		Map<String, Object> functionDetails = getMapFromKeyOrCurRow(ReactorKeysEnum.FUNCTION_DETAILS.getKey());
		if (functionDetails == null) {
			throw new NullPointerException("Must define the properties for the new function engine");
		}
		String functionTypeStr = (String) functionDetails.get(IFunctionEngine.FUNCTION_TYPE);
		String pythonFileName = (String) functionDetails.get(IFunctionEngine.PYTHON_FILE_NAME);
		if (functionTypeStr == null || (functionTypeStr = functionTypeStr.trim()).isEmpty()) {
			functionTypeStr = FunctionTypeEnum.LOCAL_PYTHON.getFunctionName();
		}
		if (pythonFileName == null || (pythonFileName = pythonFileName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the Python file name");
		}
		if (pythonFileName.contains("/") || pythonFileName.contains("\\") || !pythonFileName.endsWith(".py")) {
			throw new IllegalArgumentException("The Python file name must be a .py file name without a path");
		}
		FunctionTypeEnum functionType = null;
		try {
			functionType = FunctionTypeEnum.getEnumFromName(functionTypeStr);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid function type " + functionTypeStr);
		}
		if (functionType != FunctionTypeEnum.LOCAL_PYTHON) {
			throw new IllegalArgumentException("CreatePythonFunctionEngine requires the LOCAL_PYTHON function type");
		}

		String functionId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IFunctionEngine function = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.FUNCTION, user, functionName, functionId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.FUNCTION,
					functionId, functionName);
			// create main.py file with provided content
			createPythonFile(specificEngineFolder, pythonFileName, functionDetails);
			String functionClass = functionType.getFunctionClass();
			function = (IFunctionEngine) Class.forName(functionClass).newInstance();
			tempSmss = UploadUtilities.createTemporaryFunctionSmss(functionId, functionName, functionClass,
					functionDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to
			// reload again
			UploadUtilities.addEngineToDIHelperToIgnoreEngineWatchers(functionId, tempSmss.getAbsolutePath());
			function.open(tempSmss.getAbsolutePath());
			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			function.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.addEngineToDIHelper(functionId, functionName, function, smssFile);
			SecurityEngineUtils.addEngine(functionId, false, user);

			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addEngineOwner(functionId, user.getAccessToken(ap).getId());
			}

			ClusterUtil.pushEngine(functionId);
		} catch (Exception e) {
			classLogger.error("Unable to create Python function engine '{}' ({})", functionName, functionId, e);
			UploadUtilities.cleanUpCreateNewError(function, functionId, tempSmss, smssFile, specificEngineFolder);
			return new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		}

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), functionId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * 
	 * @param input
	 * @return
	 */
	private static String toUpperCamelCase(String input) {
		// Split the input string by spaces or non-alphabetic characters
		String[] words = input.split("\\s+|_+|\\W+");

		StringBuilder result = new StringBuilder();

		for (String word : words) {
			// Capitalize the first letter and append the rest of the word in lowercase
			result.append(word.substring(0, 1).toUpperCase());
			result.append(word.substring(1).toLowerCase());
		}

		return result.toString();
	}

	/**
	 * 
	 * @param specificEngineFolder
	 * @param pythonFileName
	 * @param functionDetails
	 * @throws IOException
	 */
	private void createPythonFile(File specificEngineFolder, String pythonFileName, Map<String, Object> functionDetails)
			throws IOException {
		File mainPy = new File(specificEngineFolder, pythonFileName);
		String fileContent = this.keyValue.get(ReactorKeysEnum.CONTENT.getKey());

		if (mainPy.exists()) {
			classLogger.warn("Python file '{}' already exists in engine folder {}", pythonFileName,
					specificEngineFolder.getAbsolutePath());
			return;
		}
		// UI has passed the full .py content
		if (fileContent != null && !fileContent.trim().isEmpty()) {
			String unescapedScript = StringEscapeUtils.unescapeJava(fileContent);
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(mainPy))) {
				writer.write(unescapedScript);
				classLogger.info("Saved uploaded Python file to {}", mainPy.getAbsolutePath());
			} catch (IOException e) {
				classLogger.error("Unable to save uploaded Python file to {}", mainPy.getAbsolutePath(), e);
				throw e;
			}
			return;
		}
		String generatedPython = buildPythonStarter(functionDetails);

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(mainPy))) {
			writer.write(generatedPython);
			classLogger.info("Created Python file at {}", mainPy.getAbsolutePath());
		} catch (IOException e) {
			classLogger.error("Unable to create Python file at {}", mainPy.getAbsolutePath(), e);
			throw e;
		}
	}

	static String buildPythonStarter(Map<String, Object> functionDetails) {
		List<String> requiredParams = parseRequiredParameters(
				functionDetails.get(IFunctionEngine.REQUIRED_PARAMETER_KEY));
		List<Map<String, String>> allParams = parseParameters(functionDetails.get(IFunctionEngine.PARAMETER_KEY));
		String functionName = String.valueOf(functionDetails.get(IFunctionEngine.NAME_KEY));
		validatePythonIdentifier(functionName, "function name");

		Map<String, Map<String, String>> paramsByName = new LinkedHashMap<>();
		for (Map<String, String> param : allParams) {
			String name = param.get("parameterName");
			validatePythonIdentifier(name, "parameter name");
			paramsByName.put(name, param);
		}

		Set<String> requiredNames = new LinkedHashSet<>(requiredParams);
		List<String> orderedNames = new ArrayList<>(requiredNames);
		for (String name : paramsByName.keySet()) {
			if (!requiredNames.contains(name)) {
				orderedNames.add(name);
			}
		}

		StringBuilder builder = new StringBuilder("def ").append(functionName).append("(\n");
		for (String name : orderedNames) {
			validatePythonIdentifier(name, "parameter name");
			Map<String, String> metadata = paramsByName.getOrDefault(name, Map.of());
			builder.append("    ").append(name).append(": ").append(toPythonType(metadata.get("parameterType")));
			if (!requiredNames.contains(name)) {
				builder.append(" = None");
			}
			builder.append(",\n");
		}
		builder.append("):\n    \"\"\"\n    Args:\n");
		for (String name : orderedNames) {
			Map<String, String> metadata = paramsByName.getOrDefault(name, Map.of());
			builder.append("        ").append(name).append(" (").append(toPythonType(metadata.get("parameterType")))
					.append("): ").append(metadata.getOrDefault("parameterDescription", "")).append("\n");
		}
		builder.append("    \"\"\"\n");
		for (String name : orderedNames) {
			builder.append("    print(\"").append(name).append(" - \", ").append(name).append(")\n");
		}
		return builder.toString();
	}

	private static String toPythonType(String type) {
		if (type == null) {
			return "str";
		}
		return switch (type.toLowerCase()) {
		case "integer" -> "int";
		case "number" -> "float";
		case "boolean" -> "bool";
		case "object" -> "dict";
		case "array" -> "list";
		default -> "str";
		};
	}

	private static void validatePythonIdentifier(String value, String field) {
		if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
			throw new IllegalArgumentException("The Python " + field + " must be a valid identifier");
		}
	}

	static List<String> parseRequiredParameters(Object value) {
		return parseList(value, new TypeToken<List<String>>() {
		}, IFunctionEngine.REQUIRED_PARAMETER_KEY);
	}

	static List<Map<String, String>> parseParameters(Object value) {
		return parseList(value, new TypeToken<List<Map<String, String>>>() {
		}, IFunctionEngine.PARAMETER_KEY);
	}

	private static <T> List<T> parseList(Object value, TypeToken<List<T>> type, String key) {
		if (value == null) {
			return List.of();
		}
		try {
			List<T> parsed = value instanceof String ? GSON.fromJson((String) value, type.getType())
					: GSON.fromJson(GSON.toJson(value), type.getType());
			return parsed == null ? List.of() : parsed;
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(key + " must be a JSON array", e);
		}
	}

	@Override
	public String getReactorDescription() {
		return """
				Create and register a LOCAL_PYTHON function engine. The reactor writes the supplied Python source
				to the engine assets folder, or generates a starter function from the function metadata when no
				source content is supplied.
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.FUNCTION.getKey().equals(key)) {
			return "The catalog name for the new Python function engine. It is normalized to UpperCamelCase for the engine name.";
		} else if (ReactorKeysEnum.FUNCTION_DETAILS.getKey().equals(key)) {
			return """
					The function-engine SMSS properties. The map must define FUNCTION_TYPE as LOCAL_PYTHON,
					PYTHON_FILE_NAME, FUNCTION_NAME, and FUNCTION_DESCRIPTION. FUNCTION_PARAMETERS and
					FUNCTION_REQUIRED_PARAMETERS are used when generating starter source.
					""";
		} else if (ReactorKeysEnum.CONTENT.getKey().equals(key)) {
			return """
					Optional complete Python source for the configured file. When omitted, the reactor generates a
					starter function with the configured function name, required parameters, parameter documentation,
					and placeholder print statements.
					""";
		}

		return super.getDescriptionForKey(key);
	}
}
