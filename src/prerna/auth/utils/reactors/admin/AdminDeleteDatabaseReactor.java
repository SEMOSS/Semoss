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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteEngineRunner;
import prerna.engine.api.IDatabaseEngine;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.EngineSyncUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Deletes databases for an admin, whatever permission the admin holds on them,
 * including a database that only admins may delete and a user's workspace or
 * asset database. Each database is removed from the loaded engines, the local
 * master, the security database and user tracking, then from disk, and in a
 * cluster from cloud storage on a background thread. Every delete is recorded
 * in the audit trail. This is the admin counterpart to
 * {@link prerna.reactor.utils.DeleteDatabaseReactor}, which deletes the
 * databases its user owns.
 */
public class AdminDeleteDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminDeleteDatabaseReactor.class);

	/**
	 * Creates the reactor with the database key.
	 */
	public AdminDeleteDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	/**
	 * Deletes every database given, once the user is known to be an admin and
	 * every database is known to exist.
	 *
	 * @return true once the databases are deleted
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		List<String> databaseIds = getDatabaseIds();
		for (String databaseId : databaseIds) {
			if (!SecurityEngineUtils.engineExists(databaseId)) {
				throw new IllegalArgumentException("Database " + databaseId + " does not exist");
			}
		}

		for (String databaseId : databaseIds) {
			IDatabaseEngine database = Utility.getDatabase(databaseId);
			String databaseName = database.getEngineName();

			deleteDatabase(database);
			UserAuditTrailUtils.recordEngineLifecycle(user, AuditEventType.ENGINE_DELETE, AuditTargetType.DATABASE,
					databaseId, databaseName, null);
			EngineSyncUtility.clearEngineCache(databaseId);
			if (ClusterUtil.IS_CLUSTER) {
				Thread.ofVirtual().start(new DeleteEngineRunner(databaseId, database.getCatalogType()));
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_ENGINE);
	}

	/**
	 * Removes one database from the loaded engines, the local master, the security
	 * database and user tracking, then deletes its files.
	 *
	 * @param database the database to delete
	 */
	private void deleteDatabase(IDatabaseEngine database) {
		String engineId = database.getEngineId();
		UploadUtilities.removeEngineFromDIHelper(engineId);
		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		remover.deleteEngineRDBMS(engineId);
		SecurityEngineUtils.deleteEngine(engineId);
		UserTrackingUtils.deleteEngine(engineId);

		try {
			database.delete();
		} catch (IOException e) {
			classLogger.error("Failed to delete the files of database {}", Utility.cleanLogString(engineId), e);
		}
	}

	/**
	 * Reads the database ids to delete, from the database key when it is set and
	 * otherwise from every input in the current row.
	 *
	 * @return the ids of the databases to delete
	 */
	private List<String> getDatabaseIds() {
		List<String> databaseIds = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				databaseIds.add(grs.get(i).toString());
			}
			return databaseIds;
		}

		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			databaseIds.add(this.curRow.get(i).toString());
		}
		return databaseIds;
	}

	/**
	 * Marks the tool as one that asks before it runs and shows in the sidebar.
	 *
	 * @return the MCP metadata of the tool
	 */
	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}

	/**
	 * Describes the reactor, its database key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Deletes databases regardless of the caller's database permissions. The calling user \
				must be an application admin or the reactor throws "User must be an admin to perform this function".

				This is the admin counterpart to DeleteDatabase, which deletes the databases the caller owns.

				Each database is removed from the catalog, from the security database and from disk, and in a \
				cluster from cloud storage. A database only admins may delete, and a user's workspace or asset \
				database, can be deleted here. Every delete is recorded in the audit trail.

				Inputs:
				  database (required) - the ids of the databases to delete.

				Returns true (DELETE_ENGINE/BOOLEAN) once the databases are deleted.
				""").key(ReactorKeysEnum.DATABASE, key -> key.list(String.class).description(
				"The ids of the databases to delete")).output(Boolean.class, "True once the databases are deleted");
	}

}
