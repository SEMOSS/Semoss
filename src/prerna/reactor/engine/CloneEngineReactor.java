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
package prerna.reactor.engine;

import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.StorageTypeEnum;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.EngineCloneUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Clones an engine of any type into a new private engine owned by the user,
 * with a copy of every asset the source keeps on the server. See
 * {@link EngineCloneUtility} for what is copied. The user must own the source
 * engine, or be an admin, and be allowed to add an engine of its type.
 */
public class CloneEngineReactor extends AbstractReactor {

	private static final String CLASS_NAME = CloneEngineReactor.class.getName();

	/** Appended to the source display name when no name is given for the clone. */
	private static final String DEFAULT_NAME_SUFFIX = " Copy";
	/** Audit detail key holding the id of the engine that was cloned. */
	private static final String CLONED_FROM_DETAIL = "clonedFrom";

	public CloneEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.NAME.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);

		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to clone an engine",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(user)) {
			throwUserNotPublisherError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin && !SecurityEngineUtils.userIsOwner(user, engineId)) {
			throw new IllegalArgumentException("Engine " + engineId
					+ " does not exist or user does not have permissions to clone the engine. User must be the owner to perform this function.");
		}

		// the clone is a new engine, so the rules for adding one apply
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) typeAndSubtype[0];
		if (AbstractSecurityUtils.adminOnlyEngineAdd(engineType) && !isAdmin) {
			throwFunctionalityOnlyExposedForAdminsError();
		}
		if (IEngine.CATALOG_TYPE.STORAGE == engineType && !isAdmin
				&& StorageTypeEnum.LOCAL_FILE_SYSTEM.getStorageName().equalsIgnoreCase((String) typeAndSubtype[1])) {
			throw new IllegalArgumentException("Only an admin can create a local file system storage engine");
		}

		String cloneName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		if (cloneName == null || cloneName.trim().isEmpty()) {
			IEngine source = Utility.getEngine(engineId);
			if (source == null) {
				throw new IllegalArgumentException("Engine " + engineId + " does not exist or could not be loaded");
			}
			String sourceName = source.getDisplayName();
			if (sourceName == null || sourceName.trim().isEmpty()) {
				sourceName = source.getEngineName();
			}
			// a display name can hold the folder separators a clone name cannot
			cloneName = sourceName.replace('/', '-').replace('\\', '-') + DEFAULT_NAME_SUFFIX;
		}
		cloneName = cloneName.trim();
		// the name is part of the engine folder name
		if (cloneName.contains("/") || cloneName.contains("\\")) {
			throw new IllegalArgumentException("The name of the clone cannot contain / or \\");
		}

		logger.info("Cloning engine {} as {}", Utility.cleanLogString(engineId), Utility.cleanLogString(cloneName));
		String cloneId = EngineCloneUtility.cloneEngine(user, engineId, cloneName, logger);
		logger.info("Cloned engine {} into {}", Utility.cleanLogString(engineId), cloneId);

		UserAuditTrailUtils.recordEngineLifecycle(user, AuditEventType.ENGINE_CREATE,
				AuditTargetType.ofEngine(engineType), cloneId, cloneName, Map.of(CLONED_FROM_DETAIL, engineId));

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(user, cloneId);
		NounMetadata noun = new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP,
				PixelOperationType.MARKET_PLACE_ADDITION);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully cloned the engine as " + cloneName));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return """
				Clone an engine into a new private engine owned by the user. The clone gets a new id and a copy \
				of everything the engine keeps on the server: its folder (database file, OWL, vector indexes, \
				python files, virtual environment, version history), its smss file and secrets, its metadata, \
				and its image. Connection settings are copied as they are, so a clone of an engine backed by an \
				external system uses the same external data. The user must own the engine.""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the engine to clone";
		} else if (key.equals(ReactorKeysEnum.NAME.getKey())) {
			return "The name of the clone. Defaults to the engine name followed by \"" + DEFAULT_NAME_SUFFIX.trim()
					+ "\"";
		}
		return super.getDescriptionForKey(key);
	}

}
