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
package prerna.reactor.vector.upload;

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
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.vector.FaissDatabaseEngine;
import prerna.engine.impl.vector.OpenSearchRestVectorDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreateVectorDatabaseEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateVectorDatabaseEngineReactor.class);

	public CreateVectorDatabaseEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONNECTION_DETAILS.getKey(),
				ReactorKeysEnum.GLOBAL.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
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

		if (AbstractSecurityUtils.adminOnlyVectorAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();

		String vectorDbName = getVectorDatabaseName();
		// if vector db name is not valid throw error
		if (!Utility.validateName(vectorDbName)) {
			// error and redirect to try again
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		// String vectorDbName = getVectorDatabaseName();
		Map<String, Object> vectorDbDetails = getVectorDatabaseDetails();
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey()) + "");

		NounMetadata warning = null;
		if (global) {
			if (AbstractSecurityUtils.adminOnlyEngineSetPublic(IEngine.CATALOG_TYPE.VECTOR)
					&& !SecurityAdminUtils.userIsAdmin(user)) {
				warning = NounMetadata.getWarningNounMessage(
						"Public access can only be enabled by administrators. This item will be created as private.");
				global = false;
			}
		}

		String vectorDbTypeStr = (String) vectorDbDetails.get(IVectorDatabaseEngine.VECTOR_TYPE);
		if (vectorDbTypeStr == null || (vectorDbTypeStr = vectorDbTypeStr.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the vector db type");
		}

		VectorDatabaseTypeEnum vectorDbType = null;
		try {
			vectorDbType = VectorDatabaseTypeEnum.getEnumFromName(vectorDbTypeStr);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid vector db type " + vectorDbTypeStr);
		}
		// TODO
		// IF IT IS TYPE PROXY THEN I DONT NEED THE EMBEDDER ENGINE NAME
		if (vectorDbType != VectorDatabaseTypeEnum.PROXY
				&& !vectorDbDetails.containsKey(Constants.EMBEDDER_ENGINE_NAME)) {
			String embedderEngineId = (String) vectorDbDetails.getOrDefault(Constants.EMBEDDER_ENGINE_ID, null);
			if (embedderEngineId == null) {
				throw new IllegalArgumentException("EMBEDDER_ENGINE_ID must be defined for FAISS database");
			}

			IModelEngine embeddingModel = Utility.getModel(embedderEngineId);
			if (embeddingModel == null) {
				throw new IllegalArgumentException("EMBEDDER_ENGINE_ID " + embeddingModel + " could not be found");
			}
			String embeddingModelAlias = embeddingModel.getSmssProp().getProperty(Constants.ENGINE_ALIAS);
			vectorDbDetails.put(Constants.EMBEDDER_ENGINE_NAME, embeddingModelAlias);
		}

		if (vectorDbType == VectorDatabaseTypeEnum.FAISS
				&& !vectorDbDetails.containsKey(FaissDatabaseEngine.ENABLE_HYBRID_SEARCH)) {
			vectorDbDetails.put(FaissDatabaseEngine.ENABLE_HYBRID_SEARCH, true);
		}

		if (!vectorDbDetails.containsKey(Constants.INDEX_CLASSES)) {
			vectorDbDetails.put(Constants.INDEX_CLASSES, "default");
		}

		if (vectorDbType == VectorDatabaseTypeEnum.OPEN_SEARCH) {
			if (vectorDbDetails.get(Constants.USERNAME) == null) {
				throw new IllegalArgumentException(Constants.USERNAME + " is not provided.");
			}
			if (vectorDbDetails.get(Constants.PASSWORD) == null) {
				throw new IllegalArgumentException(Constants.PASSWORD + " is not provided.");
			}
			if (vectorDbDetails.get(Constants.HOSTNAME) == null) {
				throw new IllegalArgumentException(Constants.HOSTNAME + " is not provided.");
			}
			if (vectorDbDetails.get(OpenSearchRestVectorDatabaseEngine.INDEX_NAME) == null) {
				throw new IllegalArgumentException(OpenSearchRestVectorDatabaseEngine.INDEX_NAME + " is not provided.");
			}
		}
		if (vectorDbType == VectorDatabaseTypeEnum.WEAVIATE) {
			if (vectorDbDetails.get(Constants.API_KEY) == null) {
				throw new IllegalArgumentException(Constants.API_KEY + " is not provided.");
			}
			if (vectorDbDetails.get(Constants.HOSTNAME) == null) {
				throw new IllegalArgumentException(Constants.HOSTNAME + " is not provided.");
			}
		}

		// vectorDbDetails.put(Constants.PIPELINE,"PIPELINE.json");

		String vectorDbId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IVectorDatabaseEngine vectorDb = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.VECTOR, user, vectorDbName, vectorDbId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.VECTOR, vectorDbId,
					vectorDbName);

			String vectorDbClass = vectorDbType.getVectorDatabaseClass();
			vectorDb = (IVectorDatabaseEngine) Class.forName(vectorDbClass).getDeclaredConstructor().newInstance();
			tempSmss = UploadUtilities.createTemporaryVectorDatabaseSmss(vectorDbId, vectorDbName, vectorDbClass,
					vectorDbDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to
			// reload again
			UploadUtilities.addEngineToDIHelperToIgnoreEngineWatchers(vectorDbId, tempSmss.getAbsolutePath());
			vectorDb.open(tempSmss.getAbsolutePath());

			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			vectorDb.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.addEngineToDIHelper(vectorDbId, vectorDbName, vectorDb, smssFile);
			SecurityEngineUtils.addEngine(vectorDbId, global, user);

			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addEngineOwner(vectorDbId, user.getAccessToken(ap).getId());
			}

			ClusterUtil.pushEngine(vectorDbId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			UploadUtilities.cleanUpCreateNewError(vectorDb, vectorDbId, tempSmss, smssFile, specificEngineFolder);
			return new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		}

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), vectorDbId);
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
	private String getVectorDatabaseName() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.DATABASE.getKey());
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

		throw new NullPointerException("Must define the name of the new vector database engine");
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getVectorDatabaseDetails() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
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

		throw new NullPointerException("Must define the properties for the new vector database engine");
	}

}
