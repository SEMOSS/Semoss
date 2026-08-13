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
package prerna.reactor.model.upload;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Constants;
import prerna.util.PythonVariableValidator;
import prerna.util.Settings;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreateModelEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateModelEngineReactor.class);

	public CreateModelEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MODEL.getKey(), ReactorKeysEnum.MODEL_DETAILS.getKey(),
				ReactorKeysEnum.GLOBAL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a model engine", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
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

		if (AbstractSecurityUtils.adminOnlyModelAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();

		String modelName = getModelName();
		// if model name is not valid, throw error
		if (!Utility.validateName(modelName)) {
			// error and redirect to try again
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		// String modelName = getModelName();
		Map<String, Object> modelMetadata = SecurityModelMetadataUtils.normalizeModelDetails(getModelDetails());
		String modelDescription = (String) modelMetadata.remove(Constants.DESCR);
		Map<String, Object> modelDetails = SecurityModelMetadataUtils.getModelEngineProperties(modelMetadata);
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey()) + "");

		NounMetadata warning = null;
		if (global) {
			if (AbstractSecurityUtils.adminOnlyEngineSetPublic(IEngine.CATALOG_TYPE.MODEL)
					&& !SecurityAdminUtils.userIsAdmin(user)) {
				warning = NounMetadata.getWarningNounMessage(
						"Public access can only be enabled by administrators. This item will be created as private.");
				global = false;
			}
		}

		String modelTypeStr = (String) modelDetails.get(IModelEngine.MODEL_TYPE);
		if (modelTypeStr == null || (modelTypeStr = modelTypeStr.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the model type");
		}
		ModelTypeEnum modelType = null;
		try {
			modelType = ModelTypeEnum.getEnumFromName(modelTypeStr);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid model type " + modelTypeStr);
		}

		if (modelDetails.containsKey(Settings.VAR_NAME)) {
			// need to validate this is alphanumeric underscore and does not start with a
			// number
			String varName = (String) modelDetails.get(Settings.VAR_NAME);
			if (!PythonVariableValidator.isValidPythonVariableName(varName)) {
				throw new IllegalArgumentException("The variable '" + varName
						+ "' is not a valid variable name. It must be alphanumeric underscore, cannot start with a digit, and cannot be a reserved word.");
			}
		}

		String modelId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IModelEngine model = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.MODEL, user, modelName, modelId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.MODEL, modelId,
					modelName);

			String modelClass = modelType.getModelClass();
			model = (IModelEngine) Class.forName(modelClass).newInstance();
			tempSmss = UploadUtilities.createTemporaryModelSmss(modelId, modelName, modelClass, modelDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to
			// reload again
			UploadUtilities.addEngineToDIHelperToIgnoreEngineWatchers(modelId, tempSmss.getAbsolutePath());
			model.open(tempSmss.getAbsolutePath());

			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			model.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.addEngineToDIHelper(modelId, modelName, model, smssFile);
			SecurityEngineUtils.addEngine(modelId, global, user);
			SecurityModelMetadataUtils.upsertModelMetadata(modelId, modelMetadata);
			if (modelDescription != null) {
				SecurityEngineUtils.updateEngineMetadata(modelId, Map.of(Constants.DESCRIPTION, modelDescription));
			}

			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addEngineOwner(modelId, user.getAccessToken(ap).getId());
			}

			ClusterUtil.pushEngine(modelId);
			UserAuditTrailUtils.recordEngineLifecycle(user, "MODEL_CREATE", "MODEL", modelId, modelName,
					Map.of("global", global, "modelType", modelType.getModelName()));
			} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			UploadUtilities.cleanUpCreateNewError(model, modelId, tempSmss, smssFile, specificEngineFolder);
		}

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), modelId);
		NounMetadata retNoun = new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP,
				PixelOperationType.MARKET_PLACE_ADDITION);
		if (warning != null) {
			retNoun.addAdditionalReturn(warning);
		}
		return retNoun;

	}

	/**
	 * 
	 * @return
	 */
	private String getModelName() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MODEL.getKey());
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

		throw new NullPointerException("Must define the name of the new model engine");
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getModelDetails() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MODEL_DETAILS.getKey());
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

		throw new NullPointerException("Must define the properties for the new model engine");
	}

}
