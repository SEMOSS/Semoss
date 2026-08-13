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
package prerna.reactor.guardrail.upload;

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
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreateGuardrailEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateGuardrailEngineReactor.class);

	public CreateGuardrailEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.GUARDRAIL.getKey(), ReactorKeysEnum.GUARDRAIL_DETAILS.getKey(),
				ReactorKeysEnum.GLOBAL.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a guardrail engine",
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

		if (AbstractSecurityUtils.adminOnlyGuardrailAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();

		String guardrailName = getGuardrailName();
		// if guardrail name is not valid throw error
		if (!Utility.validateName(guardrailName)) {
			// error and redirect to try again
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		Map<String, Object> guardrailDetails = getGuardrailDetails();
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey()) + "");

		NounMetadata warning = null;
		if (global) {
			if (AbstractSecurityUtils.adminOnlyEngineSetPublic(IEngine.CATALOG_TYPE.GUARDRAIL)
					&& !SecurityAdminUtils.userIsAdmin(user)) {
				warning = NounMetadata.getWarningNounMessage(
						"Public access can only be enabled by administrators. This item will be created as private.");
				global = false;
			}
		}

		String guardrailTypeStr = (String) guardrailDetails.get(IGuardrailReactorFunctionEngine.GUARDRAIL_TYPE);
		if (guardrailTypeStr == null || (guardrailTypeStr = guardrailTypeStr.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the guardrail type");
		}
		GuardrailTypeEnum guardrailType = null;
		try {
			guardrailType = GuardrailTypeEnum.getEnumFromName(guardrailTypeStr);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid guardrail type " + guardrailTypeStr);
		}

		String guardrailId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IGuardrailReactorFunctionEngine guardrail = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.GUARDRAIL, user, guardrailName, guardrailId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.GUARDRAIL,
					guardrailId, guardrailName);

			String guardrailClass = guardrailType.getGuardrailClass();
			guardrail = (IGuardrailReactorFunctionEngine) Class.forName(guardrailClass).getDeclaredConstructor()
					.newInstance();
			tempSmss = UploadUtilities.createTemporaryGuardrailSmss(guardrailId, guardrailName, guardrailClass,
					guardrailDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to
			// reload again
			UploadUtilities.addEngineToDIHelperToIgnoreEngineWatchers(guardrailId, tempSmss.getAbsolutePath());
			guardrail.open(tempSmss.getAbsolutePath());

			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			guardrail.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.addEngineToDIHelper(guardrailId, guardrailName, guardrail, smssFile);
			SecurityEngineUtils.addEngine(guardrailId, global, user);

			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addEngineOwner(guardrailId, user.getAccessToken(ap).getId());
			}

			ClusterUtil.pushEngine(guardrailId);
			UserAuditTrailUtils.recordEngineLifecycle(user, "GUARDRAIL_CREATE", "GUARDRAIL", guardrailId,
					guardrailName, Map.of("global", global, "guardrailType", guardrailType.getGuardrailName()));
		} catch (Exception e) {
			classLogger.error(
					"Failed to create guardrail engine '{}' with id '{}' and type '{}': {}",
					guardrailName, guardrailId, guardrailTypeStr, e.getMessage(), e);
			UploadUtilities.cleanUpCreateNewError(guardrail, guardrailId, tempSmss, smssFile, specificEngineFolder);
			throw new IllegalArgumentException("Failed to create guardrail engine. Error: " + e.getMessage());
		}

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), guardrailId);
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
	private String getGuardrailName() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.GUARDRAIL.getKey());
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

		throw new NullPointerException("Must define the name of the new guardrail engine");
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getGuardrailDetails() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.GUARDRAIL_DETAILS.getKey());
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

		throw new NullPointerException("Must define the properties for the new guardrail engine");
	}

	@Override
	public String getReactorDescription() {
		return "Create a new guardrail engine";
	}

}
