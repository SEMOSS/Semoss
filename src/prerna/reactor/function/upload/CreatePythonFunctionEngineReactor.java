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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import prerna.engine.impl.function.FunctionParameter;
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
			throw new IllegalArgumentException("Must define the function type");
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
		// No uploaded content, auto-generate a starter Python file.
		List<String> requiredParams = GSON.fromJson(
				asJsonList(functionDetails.get(IFunctionEngine.REQUIRED_PARAMETER_KEY)), new TypeToken<List<String>>() {
				}.getType());
		List<FunctionParameter> allParams = GSON.fromJson(
				asJsonList(functionDetails.get(IFunctionEngine.PARAMETER_KEY)),
				new TypeToken<List<FunctionParameter>>() {
				}.getType());
		Map<String, String> paramDescriptions = allParams.stream().collect(Collectors.toMap(
				FunctionParameter::getParameterName,
				parameter -> parameter.getParameterDescription() == null ? "" : parameter.getParameterDescription(),
				(first, replacement) -> replacement));

		Object configuredFunctionName = functionDetails.get(IFunctionEngine.NAME_KEY);
		if (configuredFunctionName == null || configuredFunctionName.toString().isBlank()) {
			throw new IllegalArgumentException("Must define the Python function name");
		}
		String pythonFunctionName = configuredFunctionName.toString().trim();
		validatePythonIdentifier(pythonFunctionName, "function name");
		for (String requiredParam : requiredParams) {
			validatePythonIdentifier(requiredParam, "required parameter");
		}
		String functionParameters = requiredParams.stream().map(param -> "    " + param + ": str,")
				.collect(Collectors.joining("\n"));
		String parameterDocumentation = requiredParams.stream()
				.map(param -> "        " + param + " (str): " + paramDescriptions.getOrDefault(param, ""))
				.collect(Collectors.joining("\n"));
		String printStatements = requiredParams.stream().map(param -> "    print(\"" + param + " - \", " + param + ")")
				.collect(Collectors.joining("\n"));
		String generatedPython = """
				def %s(
				%s
				):
				    \"""
				    Args:
				%s
				    \"""
				%s
				""".formatted(pythonFunctionName, functionParameters, parameterDocumentation, printStatements);

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(mainPy))) {
			writer.write(generatedPython);
			classLogger.info("Created Python file at {}", mainPy.getAbsolutePath());
		} catch (IOException e) {
			classLogger.error("Unable to create Python file at {}", mainPy.getAbsolutePath(), e);
			throw e;
		}
	}

	private static String asJsonList(Object value) {
		if (value == null || value.toString().isBlank()) {
			return "[]";
		}
		return value instanceof String stringValue ? stringValue : GSON.toJson(value);
	}

	private static void validatePythonIdentifier(String value, String field) {
		if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
			throw new IllegalArgumentException("The Python " + field + " must be a valid identifier");
		}
	}
}
