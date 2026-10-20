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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
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
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class CreateFunctionEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateFunctionEngineReactor.class);

	public CreateFunctionEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.FUNCTION_DETAILS.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
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

		String functionName = getStringFromKeyOrCurRowStringValue(ReactorKeysEnum.FUNCTION.getKey());
		if (functionName == null || (functionName = functionName.trim()).isEmpty()) {
			throw new NullPointerException("Must define the name of the new function engine");
		}
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
		File specificEngineAssetsFolder = null;
		IFunctionEngine function = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.FUNCTION, user, functionName, functionId);
			specificEngineAssetsFolder = UploadUtilities
					.generateSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.FUNCTION, functionId, functionName);

			if (functionType == FunctionTypeEnum.LOCAL_PYTHON) {
				moveFilesToEngineFolder(specificEngineAssetsFolder);
			}

			String functionClass = functionType.getFunctionClass();
			function = (IFunctionEngine) Class.forName(functionClass).getDeclaredConstructor().newInstance();
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

			// Initialize git and commit initial engine files
			try {
				String versionFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.FUNCTION,
						functionId, functionName);
				GitRepoUtils.init(versionFolder);
				GitRepoUtils.addAllFiles(versionFolder, false);
				AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
				GitRepoUtils.commitAddedFiles(versionFolder, "initial: created function engine " + functionName,
						accessToken.getUsername(), accessToken.getEmail());
			} catch (Exception e) {
				classLogger.warn("Unable to initialize git for function engine {}", functionId, e);
			}

			ClusterUtil.pushEngine(functionId);
		} catch (Exception e) {
			classLogger.error("Unable to create function engine '{}' ({})", functionName, functionId, e);
			UploadUtilities.cleanUpCreateNewError(function, functionId, tempSmss, smssFile, specificEngineAssetsFolder);
			return new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		}

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), functionId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * 
	 * @param specificEngineFolder
	 * @throws IOException
	 */
	private void moveFilesToEngineFolder(File specificEngineFolder) throws IOException {
		String insightFolder = this.insight.getInsightFolder();

		// see if added as key
		GenRowStruct grs = getGenRowStruct(ReactorKeysEnum.FILE_PATH.getKey());
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

	@Override
	public String getReactorDescription() {
		return """
				Create and register a function engine from the supplied function metadata. The reactor validates the
				engine type, creates its assets and SMSS configuration, assigns the caller as an owner, initializes
				version control, and synchronizes the engine to the cluster.
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.FUNCTION.getKey().equals(key)) {
			return "The catalog name for the new function engine. The name must be unique and contain only supported engine-name characters.";
		} else if (ReactorKeysEnum.FUNCTION_DETAILS.getKey().equals(key)) {
			return """
					The function-engine SMSS properties. The map must include FUNCTION_TYPE and the configuration
					required by that engine type, such as FUNCTION_NAME, FUNCTION_DESCRIPTION, parameters,
					credentials, connection settings, or provider-specific options.
					""";
		} else if (ReactorKeysEnum.FILE_PATH.getKey().equals(key)) {
			return """
					Optional path to a file in the current insight folder. For LOCAL_PYTHON engines, each supplied
					file is moved into the new engine assets folder.
					""";
		}

		return super.getDescriptionForKey(key);
	}
}
