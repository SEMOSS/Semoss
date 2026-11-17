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
package prerna.auth.utils.reactors.admin;

import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.EngineCloneUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Clones an engine of any type for an admin, whatever permission the admin
 * holds on it, into a new private engine owned by the admin, with a copy of
 * every asset the source keeps on the server. See {@link EngineCloneUtility}
 * for what is copied. An admin adds an engine of any type, whether or not the
 * platform limits publishing to publishers. The clone is recorded in the audit
 * trail as a new engine. This is the admin counterpart to
 * {@link prerna.reactor.engine.CloneEngineReactor}, which clones an engine its
 * user owns.
 */
public class AdminCloneEngineReactor extends AbstractReactor {

	private static final String CLASS_NAME = AdminCloneEngineReactor.class.getName();

	private static final String ENGINE_ID_KEY = "engineId";

	/** Appended to the source display name when no name is given for the clone. */
	private static final String DEFAULT_NAME_SUFFIX = " Copy";
	/** Audit detail key holding the id of the engine that was cloned. */
	private static final String CLONED_FROM_DETAIL = "clonedFrom";

	/**
	 * Creates the reactor with the required id of the engine to clone and the
	 * optional name of the clone.
	 */
	public AdminCloneEngineReactor() {
		this.keysToGet = new String[] { ENGINE_ID_KEY + "," + ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.CATALOG_NAME.getKey() + "," + ReactorKeysEnum.NAME.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	/**
	 * Clones the engine, once the user is known to be an admin, under the name
	 * given or the source display name followed by the default suffix.
	 *
	 * @return the catalog details of the clone, with a success message
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);

		String engineId = this.keyValue.get(ENGINE_ID_KEY);
		IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(engineId);

		String cloneName = this.keyValue.get(ReactorKeysEnum.CATALOG_NAME.getKey());
		if (cloneName == null || cloneName.trim().isEmpty()) {
			IEngine source = Utility.getEngine(engineId);
			if (source == null) {
				throw new IllegalArgumentException("Engine " + engineId + " does not exist or could not be loaded");
			}
			String sourceName = source.getDisplayName();
			if (sourceName == null || sourceName.trim().isEmpty()) {
				sourceName = source.getEngineName();
			}
			cloneName = sourceName.replace('/', '-').replace('\\', '-') + DEFAULT_NAME_SUFFIX;
		}
		cloneName = cloneName.trim();
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

	/**
	 * Describes the reactor, its keys and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Clones an engine into a new private engine owned by the caller, regardless of the \
				caller's permissions on the engine. The calling user must be an application admin or the reactor \
				throws "User must be an admin to perform this function".

				This is the admin counterpart to CloneEngine, which clones an engine the caller owns.

				The clone gets a new id and a copy of everything the engine keeps on the server: its folder \
				(database file, OWL, vector indexes, python files, virtual environment, version history), its smss \
				file and secrets, its metadata, and its image. Connection settings are copied as they are, so a \
				clone of an engine backed by an external system uses the same external data. The clone is recorded \
				in the audit trail.

				Inputs:
				  engineId    (required) - the id of the engine to clone.
				  catalogName (optional) - the name of the clone; defaults to the engine name followed by "Copy".

				Returns a map (MARKET_PLACE_ADDITION/UPLOAD_RETURN_MAP) with the catalog details of the clone.
				""").key(ENGINE_ID_KEY, "The id of the engine to clone")
				.key(ReactorKeysEnum.CATALOG_NAME, "The name of the clone. Defaults to the engine name followed by \""
						+ DEFAULT_NAME_SUFFIX.trim() + "\"")
				.output(Map.class, "The catalog details of the clone");
	}

}
