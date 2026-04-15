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
package prerna.reactor.database.upload.rdbms.excel;

import java.io.File;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IRDBMSEngine;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.database.upload.rdbms.RdbmsUploadReactorUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class RdbmsReplaceDatabaseLoaderSheetUploadReactor extends RdbmsLoaderSheetUploadReactor {

	public RdbmsReplaceDatabaseLoaderSheetUploadReactor() {
		this.keysToGet = new String[] { UploadInputUtility.DATABASE, UploadInputUtility.FILE_PATH,
				UploadInputUtility.SPACE, UploadInputUtility.ADD_TO_EXISTING, UploadInputUtility.DATA_TYPE_MAP,
				UploadInputUtility.NEW_HEADERS, UploadInputUtility.ADDITIONAL_DATA_TYPES,
				UploadInputUtility.CLEAN_STRING_VALUES, UploadInputUtility.REMOVE_DUPLICATE_ROWS };
	}

	@Override
	public NounMetadata execute() {
		/*
		 * JK!!!! THE ADD TO EXISITNG HAS NOT BEEN IMPLEMENTED YET! JUST THROW AN ERROR
		 * UNTIL WE FINISH THAT
		 */
		if (true) {
			throw new IllegalArgumentException(
					"The replace method using the loader sheet format has not been implemented yet.");
		}

		/*
		 * THIS LOGIC IS THE SAME AS THE LOGIC IN THE AbstractUploadFileReactor EXCEPT
		 * IT CAN ONLY BE FOR OVERRIDING AN EXISTING DATABASE THE LOGIC IS THE SAME
		 * EXCEPT THERE IS AN ADDITIONAL METHOD TO REMOVE THE DATABASE BEFORE RUNNING
		 * THE UPDATE
		 * 
		 */

		this.logger = getLogger(this.getClass().getName());

		organizeKeys();
		String databaseId = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()));
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		if (!new File(filePath).exists()) {
			throw new IllegalArgumentException("Could not find the specified file to use for importing");
		}
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

		// check if input is alias since we are adding ot existing
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseId);

		// throw error is user is not owner
		if (!SecurityEngineUtils.userIsOwner(user, databaseId)) {
			NounMetadata noun = new NounMetadata(
					"User must be the owner in order to replace all the data in the database",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		try {
			this.databaseId = databaseId;
			this.databaseName = MasterDatabaseUtility.getDatabaseAliasForId(this.databaseId);
			// get existing database
			this.logger.info("Get existing database");
			this.database = Utility.getDatabase(databaseId, true);
			if (this.database == null) {
				throw new IllegalArgumentException("Couldn't find the database " + databaseId + " to append data into");
			}
			if (!(this.database instanceof IRDBMSEngine)) {
				throw new IllegalArgumentException("Database must be using a relational database");
			}
			this.logger.info("Done");
			RdbmsUploadReactorUtility.deleteRowsFromAllTables((IRDBMSEngine) this.database);
			addToExistingDatabase(filePath);
			// NO NEED TO SYNC THE METADATA SINCE WE ARE ASSUMING IT IS THE SAME OWL IN THE
			// REPLACE!
			// this.logger.info("Process database metadata to allow for traversing across
			// databases");
			// UploadUtilities.updateMetadata(this.engine.getEngineId());
			this.logger.info("Complete");
		} catch (Exception e) {
			this.logger.error(Constants.STACKTRACE, e);
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

		// if we got here
		// no errors
		// we can do normal clean up of files
		// TODO:
		ClusterUtil.pushEngine(this.databaseId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), this.databaseId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

}
