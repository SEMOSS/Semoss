package prerna.reactor.function.upload;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AddFileUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreateRestFunctionEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateRestFunctionEngineReactor.class);

	public static final String FILE_PATHS_KEY = "filePaths";
	public static final String FILE_PATH_KEY = "filePath";
	public static final String DOUBLE_BACKWARD_SLASH = "\\\\";
	public static final String NEW_LINE = "\n";
	public static final String SMSS_KEY_SERVICE_ACCOUNT_FILE = "SERVICE_ACCOUNT_FILE";
	public static final String SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS = "SERVICE_ACCOUNT_CREDENTIALS";
	private static final String FILE_NAME_SUFFIX = "_service_account_file";

	public CreateRestFunctionEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.FUNCTION_DETAILS.getKey(),
				ReactorKeysEnum.FILE_NAME.getKey(), FILE_PATHS_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a function engine",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		// throw error is user doesn't have rights to publish new databases
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyFunctionAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();

		String functionName = getFunctionName();
		// if function name is not valid, throw error
		if (!Utility.validateName(functionName)) {
			// error and redirect to try again
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		// String functionName = getFunctionName();
		Map<String, Object> functionDetails = getFunctionDetails();
		String functionTypeStr = (String) functionDetails.get(IFunctionEngine.FUNCTION_TYPE);
		if (functionTypeStr == null || (functionTypeStr = functionTypeStr.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the function type");
		}
		FunctionTypeEnum functionType = null;
		try {
			functionType = FunctionTypeEnum.getEnumFromName(functionTypeStr);
		} catch (Exception e) {
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
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.FUNCTION,
					functionId, functionName);

			if (functionType == FunctionTypeEnum.LOCAL_PYTHON) {
				moveFilesToEngineFolder(specificEngineFolder);
			}

			String filePaths = this.keyValue.get(FILE_PATHS_KEY);
			if (StringUtils.isNotEmpty(filePaths)) {
				List<Map<String, String>> responseMap = AddFileUtils.addFileToModel(this.store, functionId,
						functionName, this.insight, IEngine.CATALOG_TYPE.FUNCTION, FILE_NAME_SUFFIX);
				String filePath = Utility.normalizePath(responseMap.get(0).get(FILE_PATH_KEY));
				String fileContent = FileUtils.readFileToString(new File(filePath), StandardCharsets.UTF_8)
						.replaceAll(NEW_LINE, "")
						.replaceAll(DOUBLE_BACKWARD_SLASH, DOUBLE_BACKWARD_SLASH + DOUBLE_BACKWARD_SLASH);
				functionDetails.put(SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS, fileContent);
			}

			String functionClass = functionType.getFunctionClass();
			function = (IFunctionEngine) Class.forName(functionClass).newInstance();
			tempSmss = UploadUtilities.createTemporaryFunctionSmss(functionId, functionName, functionClass,
					functionDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to
			// reload again
			DIHelper.getInstance().setEngineProperty(functionId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
			function.open(tempSmss.getAbsolutePath());

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
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			cleanUpCreateNewError(function, functionId, tempSmss, smssFile, specificEngineFolder);
			return new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		}

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), functionId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * Delete all the corresponding files that are generated from the upload the
	 * failed
	 */
	private void cleanUpCreateNewError(IFunctionEngine function, String storageId, File tempSmss, File smssFile,
			File specificEngineFolder) {
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
	 * @return
	 */
	private String getFunctionName() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FUNCTION.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<String> strValues = grs.getAllStrValues();
			if (strValues != null && !strValues.isEmpty()) {
				return strValues.get(0).trim();
			}
		}

		List<String> strValues = this.curRow.getAllStrValues();
		if (strValues != null && !strValues.isEmpty()) {
			return strValues.get(0).trim();
		}

		throw new NullPointerException("Must define the name of the new function engine");
	}

	/**
	 * @return
	 */
	private Map<String, Object> getFunctionDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FUNCTION_DETAILS.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if (mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}

		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, Object>) mapNouns.get(0).getValue();
		}

		throw new NullPointerException("Must define the properties for the new function engine");
	}

	private void moveFilesToEngineFolder(File specificEngineFolder) throws IOException {
		String insightFolder = this.insight.getInsightFolder();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FILE_NAME.getKey());
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				File file = new File(insightFolder + File.separator + grs.get(i).toString());
				if (file.exists()) {
					FileUtils.moveFileToDirectory(file, specificEngineFolder, false);
				}
			}
		}
	}
}
