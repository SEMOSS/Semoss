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
package prerna.reactor.database.upload;

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
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public abstract class AbstractDatabaseUploadFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractDatabaseUploadFileReactor.class);

	/**
	 * Every reactor that extends this needs to define its own inputs However, every
	 * one needs to have the following in the keysToGet array:
	 * UploadUtility.DATABASE UploadInputUtility.FILE_PATH
	 * UploadInputUtility.ADD_TO_EXISTING
	 * 
	 */

	// we need to define some variables that are stored at the class level
	// so that we can properly account for cleanup if errors occur
	protected transient Logger logger;
	protected transient String databaseId;
	protected transient String databaseName;
	protected transient IDatabaseEngine database;
	protected transient File databaseFolder;
	protected transient File tempSmss;
	protected transient File smssFile;

	protected transient boolean error = false;

	@Override
	public NounMetadata execute() {
		this.logger = getLogger(this.getClass().getName());

		organizeKeys();
		String databaseIdOrName = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()));
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		if (!new File(filePath).exists()) {
			throw new IllegalArgumentException("Could not find the specified file to use for importing");
		}
		final boolean existing = UploadInputUtility.getExisting(this.store);
		// check security
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create or update a database",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}

		// throw error is user doesn't have rights to publish new databases
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyDatabaseAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		if (existing) {
			// check if input is alias since we are adding ot existing
			databaseIdOrName = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseIdOrName);
			if (!SecurityEngineUtils.userCanEditEngine(user, databaseIdOrName)) {
				NounMetadata noun = new NounMetadata(
						"User does not have sufficient priviledges to create or update a database",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}

			try {
				this.databaseId = databaseIdOrName;
				this.databaseName = MasterDatabaseUtility.getDatabaseAliasForId(this.databaseId);
				// get existing database
				this.logger.info("Get existing database");
				this.database = Utility.getDatabase(databaseId);
				if (this.database == null) {
					throw new IllegalArgumentException(
							"Couldn't find the database " + databaseId + " to append data into");
				}
				this.logger.info("Done");
				addToExistingDatabase(filePath);
				// sync metadata
				this.logger.info("Process database metadata to allow for traversing across databases");
				UploadUtilities.updateMetadata(this.database.getEngineId(), user);
				this.logger.info("Complete");
				this.logger.info("Delete OWL position map");
				File owlF = this.database.getOwlPositionFile();
				if (owlF.exists()) {
					owlF.delete();
				}
				this.logger.info("Complete");
			} catch (Exception e) {
				classLogger.error("Error occurred while adding data to existing database '{}': {}", databaseIdOrName,
						e.getMessage(), e);
				this.error = true;
				if (e instanceof SemossPixelException) {
					throw (SemossPixelException) e;
				} else {
					NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING,
							PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			} finally {
				closeFileHelpers();
				// need to rollback
				// TODO:
			}
		} else {
			try {
				// make a new id
				this.databaseId = UUID.randomUUID().toString();
				this.databaseName = databaseIdOrName;
				// validate database
				this.logger.info("Start validating database");
				UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.DATABASE, user, this.databaseName, this.databaseId);
				this.logger.info("Done validating database");
				// create database folder
				this.logger.info("Start generating database folder");
				this.databaseFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.DATABASE,
						this.databaseId, this.databaseName);
				this.logger.info("Complete");
				generateNewDatabase(user, this.databaseName, filePath);
				// and rename .temp to .smss
				this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
				FileUtils.copyFile(this.tempSmss, this.smssFile);
				this.tempSmss.delete();
				this.database.setSmssFilePath(this.smssFile.getAbsolutePath());
				UploadUtilities.addEngineToDIHelper(this.databaseId, this.databaseName, this.database, this.smssFile);
				// sync metadata
				this.logger.info("Process database metadata to allow for traversing across databases");
				UploadUtilities.updateMetadata(this.databaseId, user);

				this.logger.info("Complete");
			} catch (Exception e) {
				classLogger.error("Error occurred while generating new database '{}': {}", this.databaseName,
						e.getMessage(), e);
				this.error = true;
				if (e instanceof SemossPixelException) {
					throw (SemossPixelException) e;
				} else {
					NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING,
							PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			} finally {
				closeFileHelpers();
				if (this.error) {
					// need to delete everything...
					UploadUtilities.cleanUpCreateNewError(this.database, this.databaseId, this.smssFile, this.tempSmss,
							this.databaseFolder);
				}
			}

			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addEngineOwner(this.databaseId, user.getAccessToken(ap).getId());
			}
		}

		ClusterUtil.pushEngine(this.databaseId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), this.databaseId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	///////////////////////////////////////////////////////

	/*
	 * Execution methods This will be done by every implementation of the upload
	 * file reactors
	 */

	public abstract void generateNewDatabase(User user, final String newDatabaseName, final String filePath)
			throws Exception;

	public abstract void addToExistingDatabase(final String filePath) throws Exception;

	public abstract void closeFileHelpers();
}
