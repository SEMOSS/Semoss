package prerna.reactor.function.upload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.engine.AbstractEngineFileReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreatePythonFunctionEngineReactor extends AbstractEngineFileReactor {

	private static final Logger classLogger = LogManager.getLogger(CreatePythonFunctionEngineReactor.class);

	public CreatePythonFunctionEngineReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.FUNCTION.getKey(),
				ReactorKeysEnum.FUNCTION_DETAILS.getKey(),
				ReactorKeysEnum.CONTENT.getKey()
		};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		validateUserAndEngineAccess(user);
		organizeKeys();

		String functionEngineName = getFunctionName();
		// Generate unique engine ID and formatted name
		String functionName = toUpperCamelCase(functionEngineName);
		//if function name is not valid, throw error
		if (!Utility.validateName(functionName)) {
			//error and redirect to try again
			throw new IllegalArgumentException("Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		//String functionName = getFunctionName();
		Map<String, Object> functionDetails = getFunctionDetails();
		String functionTypeStr = (String) functionDetails.get(IFunctionEngine.FUNCTION_TYPE);
		String pythonFileName = (String) functionDetails.get(IFunctionEngine.PYTHON_FILE_NAME);
		if(functionTypeStr == null || (functionTypeStr=functionTypeStr.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the function type");
		}
		FunctionTypeEnum functionType = null;
		try {
			functionType = FunctionTypeEnum.getEnumFromName(functionTypeStr);
		} catch(Exception e) {
			throw new IllegalArgumentException("Invalid function type " + functionTypeStr);
		}

		String functionId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IFunctionEngine function = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.FUNCTION, user, functionName, functionId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.FUNCTION, functionId, functionName);
			//create  main.py file with provided content
			createPythonFile(specificEngineFolder, pythonFileName, functionDetails);
			String functionClass = functionType.getFunctionClass();
			function = (IFunctionEngine) Class.forName(functionClass).newInstance();
			tempSmss = UploadUtilities.createTemporaryFunctionSmss(functionId, functionName, functionClass, functionDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to reload again
			DIHelper.getInstance().setEngineProperty(functionId + "_" + Constants.STORE, tempSmss.getAbsolutePath());

			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			function.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.updateDIHelper(functionId, functionName, function, smssFile);
			SecurityEngineUtils.addEngine(functionId, false, user);

			// even if no security, just add user as database owner
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityEngineUtils.addEngineOwner(functionId, user.getAccessToken(ap).getId());
				}
			}

			ClusterUtil.pushEngine(functionId);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			cleanUpCreateNewError(function, functionId, tempSmss, smssFile, specificEngineFolder);
			return new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		}

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), functionId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 * 
	 * @param function
	 * @param storageId
	 * @param tempSmss
	 * @param smssFile
	 * @param specificEngineFolder
	 */
	private void cleanUpCreateNewError(IFunctionEngine function, String storageId, File tempSmss, File smssFile, File specificEngineFolder) {
		try {
			// close the function so we can delete it
			if (function != null) {
				function.close();
			}

			// delete the .temp file
			if (tempSmss != null && tempSmss.exists()) {
				FileUtils.forceDelete(tempSmss);
			}
			// delete the .smss file
			if (smssFile != null && smssFile.exists()) {
				FileUtils.forceDelete(smssFile);
			}
			if (specificEngineFolder != null && specificEngineFolder.exists()) {
				FileUtils.forceDelete(specificEngineFolder);
			}

			UploadUtilities.removeEngineFromDIHelper(storageId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
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
	 * @return
	 */
	private String getFunctionName() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FUNCTION.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<String> strValues = grs.getAllStrValues();
			if(strValues != null && !strValues.isEmpty()) {
				return strValues.get(0).trim();
			}
		}

		List<String> strValues = this.curRow.getAllStrValues();
		if(strValues != null && !strValues.isEmpty()) {
			return strValues.get(0).trim();
		}

		throw new NullPointerException("Must define the name of the new function engine");
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getFunctionDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FUNCTION_DETAILS.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if(mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}

		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, Object>) mapNouns.get(0).getValue();
		}

		throw new NullPointerException("Must define the properties for the new function engine");
	}

	/**
	 * 
	 * @param specificEngineFolder
	 * @param pythonFileName
	 * @param functionDetails
	 * @throws IOException
	 */
	private void createPythonFile(File specificEngineFolder, String pythonFileName, Map<String, Object> functionDetails) throws IOException {
		File mainPy = new File(specificEngineFolder, pythonFileName);
		String fileContent = this.keyValue.get(ReactorKeysEnum.CONTENT.getKey());

		if (mainPy.exists()) {
			classLogger.warn(pythonFileName + " already exists in " + specificEngineFolder.getAbsolutePath());
			return;
		}
		//UI has passed the full .py content
		if (fileContent != null && !fileContent.trim().isEmpty()) {
			String unescapedScript = StringEscapeUtils.unescapeJava(fileContent);
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(mainPy))) {
				writer.write(unescapedScript);
				classLogger.info("Uploaded .py file saved to: " + mainPy.getAbsolutePath());
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw e;
			}
			return;
		}
		// No uploaded file, auto-generate main.py
		List<String> requiredParams = (List<String>) functionDetails.get("FUNCTION_REQUIRED_PARAMETERS");
		List<Map<String, String>> allParams = (List<Map<String, String>>) functionDetails.get("FUNCTION_PARAMETERS");

		Map<String, String> paramDescriptions = new HashMap<>();
		for (Map<String, String> param : allParams) {
			paramDescriptions.put(param.get("parameterName"), param.get("parameterDescription"));
		}

		StringBuilder builder = new StringBuilder();

		// Function signature
		builder.append("def main(\n");
		for (String param : requiredParams) {
			builder.append("    ").append(param).append(": str,\n");
		}
		builder.append("):\n");

		builder.append("    \"\"\"\n");
		builder.append("    Args:\n");
		for (String param : requiredParams) {
			builder.append("        ")
			.append(param)
			.append(" (str): ")
			.append(paramDescriptions.getOrDefault(param, ""))
			.append("\n");
		}
		builder.append("    \"\"\"\n");

		// Print statements
		for (String param : requiredParams) {
			builder.append("    print(\"").append(param).append(" - \", ").append(param).append(")\n");
		}

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(mainPy))) {
			writer.write(builder.toString());
			classLogger.info("main.py file created  " + mainPy.getAbsolutePath());
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
}
