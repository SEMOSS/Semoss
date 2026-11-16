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

import java.io.File;
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
import prerna.engine.api.IEngine;
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
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * Deletes engines for an admin, whatever permission the admin holds on them.
 * Each engine is removed from the local master, the security database, user
 * tracking and the engine cache, then from disk, and in a cluster from cloud
 * storage on a background thread. Every delete is recorded in the audit trail.
 * This is the admin counterpart to
 * {@link prerna.reactor.utils.DeleteEngineReactor}, which deletes the engines
 * its user owns.
 */
public class AdminDeleteEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminDeleteEngineReactor.class);

	/**
	 * Creates the reactor with the engine key.
	 */
	public AdminDeleteEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
	}

	/**
	 * Deletes every engine given, once the user is known to be an admin and every
	 * engine is known to exist.
	 *
	 * @return true once the engines are deleted
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		List<String> engineIds = getEngineIds();
		for (String engineId : engineIds) {
			if (!SecurityEngineUtils.engineExists(engineId)) {
				throw new IllegalArgumentException("Engine " + engineId + " does not exist");
			}
		}

		for (String engineId : engineIds) {
			IEngine engine = Utility.getEngine(engineId, false);
			String engineName = null;
			IEngine.CATALOG_TYPE engineType = null;
			if (engine != null) {
				engineName = engine.getEngineName();
				engineType = engine.getCatalogType();
			} else {
				engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
				engineType = SecurityEngineUtils.getEngineType(engineId);
			}

			deleteEngine(engine, engineId, engineName, engineType);
			UserAuditTrailUtils.recordEngineLifecycle(user, AuditEventType.ENGINE_DELETE,
					AuditTargetType.ofEngine(engineType), engineId, engineName, null);
			if (ClusterUtil.IS_CLUSTER) {
				Thread.ofVirtual().start(new DeleteEngineRunner(engineId, engineType));
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_ENGINE);
	}

	/**
	 * Removes one engine from the local master when it is a database, from the
	 * security database, from user tracking and from the engine cache, then from
	 * disk. An engine that could not be loaded has its folder and smss file
	 * removed by name.
	 *
	 * @param engine     the loaded engine, or null when it could not be loaded
	 * @param engineId   the id of the engine
	 * @param engineName the name of the engine
	 * @param engineType the catalog type of the engine
	 */
	private void deleteEngine(IEngine engine, String engineId, String engineName, IEngine.CATALOG_TYPE engineType) {
		if (IEngine.CATALOG_TYPE.DATABASE == engineType) {
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			remover.deleteEngineRDBMS(engineId);
		}
		SecurityEngineUtils.deleteEngine(engineId);
		UserTrackingUtils.deleteEngine(engineId);
		EngineSyncUtility.clearEngineCache(engineId);

		if (engine != null) {
			try {
				engine.delete();
			} catch (IOException e) {
				classLogger.error("An error occurred attempting to call the delete method for engine {}", engineId, e);
			}
			return;
		}

		String thisEngineFolder = EngineUtility.getSpecificEngineVersionFolder(engineType, engineId, engineName);
		File thisEngineF = new File(thisEngineFolder);
		if (thisEngineF.exists() && thisEngineF.isDirectory()) {
			thisEngineF.delete();
		}
		File thisSmssF = new File(thisEngineFolder + ".smss");
		if (thisSmssF.exists() && thisSmssF.isFile()) {
			thisSmssF.delete();
		}
	}

	/**
	 * Reads the engine ids to delete, from the engine key when it is set and
	 * otherwise from every input in the current row.
	 *
	 * @return the ids of the engines to delete
	 */
	private List<String> getEngineIds() {
		List<String> engineIds = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				engineIds.add(grs.get(i).toString());
			}
			return engineIds;
		}

		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			engineIds.add(this.curRow.get(i).toString());
		}
		return engineIds;
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
	 * Describes the reactor, its engine key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Deletes engines regardless of the caller's engine permissions. The calling user must be \
				an application admin or the reactor throws "User must be an admin to perform this function".

				This is the admin counterpart to DeleteEngine, which deletes the engines the caller owns.

				Each engine is removed from the catalog, from the security database and from disk, and in a \
				cluster from cloud storage. Every delete is recorded in the audit trail.

				Inputs:
				  engine (required) - the ids of the engines to delete.

				Returns true (DELETE_ENGINE/BOOLEAN) once the engines are deleted.
				""").key(ReactorKeysEnum.ENGINE, key -> key.list(String.class).description(
				"The ids of the engines to delete")).output(Boolean.class, "True once the engines are deleted");
	}

}
