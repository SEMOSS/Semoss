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
package prerna.reactor.utils;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.UserAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteEngineRunner;
import prerna.engine.api.IDatabaseEngine;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

@Deprecated
public class DeleteDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteDatabaseReactor.class);

	@Deprecated
	public DeleteDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Deprecated
	@Override
	public NounMetadata execute() {
		List<String> databaseIds = getDatabaseIds();
		// first validate all the inputs
		User user = this.insight.getUser();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			for (String databaseId : databaseIds) {
				if (AbstractSecurityUtils.adminOnlyEngineDelete(databaseId)) {
					throwFunctionalityOnlyExposedForAdminsError();
				}
				if (UserAssetUtils.isAssetProject(databaseId)) {
					throw new IllegalArgumentException(
							"Users are not allowed to delete your workspace or asset database.");
				}
				// we may have the alias
				databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
				boolean isOwner = SecurityEngineUtils.userIsOwner(user, databaseId);
				if (!isOwner) {
					throw new IllegalArgumentException("Database " + databaseId
							+ " does not exist or user does not have permissions to delete the database. User must be the owner to perform this function.");
				}
			}
		}

		for (String databaseId : databaseIds) {
			// we may have the alias
			databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
			IDatabaseEngine database = Utility.getDatabase(databaseId);

			deleteDatabase(database);
			EngineSyncUtility.clearEngineCache(databaseId);
			UserTrackingUtils.deleteEngine(databaseId);
			// Run the delete thread in the background for removing from cloud storage
			if (ClusterUtil.IS_CLUSTER) {
				Thread deleteAppThread = new Thread(new DeleteEngineRunner(databaseId, database.getCatalogType()));
				deleteAppThread.start();
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_ENGINE);
	}

	/**
	 * 
	 * @param database
	 * @return
	 */
	private boolean deleteDatabase(IDatabaseEngine database) {
		String engineId = database.getEngineId();
		UploadUtilities.removeEngineFromDIHelper(engineId);
		// remove from local master if database
		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		remover.deleteEngineRDBMS(engineId);
		// remove from security
		SecurityEngineUtils.deleteEngine(engineId);
		// remove from user tracking
		UserTrackingUtils.deleteEngine(engineId);

		try {
			database.delete();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return true;
	}

	/**
	 * Get inputs
	 * 
	 * @return list of databases to delete
	 */
	@Deprecated
	public List<String> getDatabaseIds() {
		List<String> databaseIds = new Vector<String>();

		// see if added as key
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				databaseIds.add(grs.get(i).toString());
			}
			return databaseIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			databaseIds.add(this.curRow.get(i).toString());
		}
		return databaseIds;
	}
}
