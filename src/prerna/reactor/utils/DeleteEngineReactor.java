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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteEngineRunner;
import prerna.engine.api.IEngine;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class DeleteEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteEngineReactor.class);

	public DeleteEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		List<String> engineIds = getEngineIds();
		// first validate all the inputs
		User user = this.insight.getUser();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			for (String engineId : engineIds) {
				if (AbstractSecurityUtils.adminOnlyEngineDelete(engineId)) {
					throwFunctionalityOnlyExposedForAdminsError();
				}

				// we may have the alias
				engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
				boolean isOwner = SecurityEngineUtils.userIsOwner(user, engineId);
				if (!isOwner) {
					throw new IllegalArgumentException("Engine " + engineId
							+ " does not exist or user does not have permissions to delete the engine. User must be the owner to perform this function.");
				}
			}
		}

		// once all are good, we can delete
		for (String engineId : engineIds) {
			// we may have the alias
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			IEngine engine = Utility.getEngine(engineId, false);
			String engineName = null;
			IEngine.CATALOG_TYPE engineType = null;
			if (engine != null) {
				engineName = engine.getEngineName();
				engineType = engine.getCatalogType();
			} else {
				engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
				Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
				engineType = (IEngine.CATALOG_TYPE) typeAndSubtype[0];
			}

			deleteEngines(engine, engineId, engineName, engineType);
			// Run the delete thread in the background for removing from cloud storage
			if (ClusterUtil.IS_CLUSTER) {
				Thread.ofVirtual().start(new DeleteEngineRunner(engineId, engineType));
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_ENGINE);
	}

	/**
	 * 
	 * @param engine
	 * @return
	 */
	private boolean deleteEngines(IEngine engine, String engineId, String engineName, IEngine.CATALOG_TYPE engineType) {
		// remove from local master if database
		if (IEngine.CATALOG_TYPE.DATABASE == engineType) {
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			remover.deleteEngineRDBMS(engineId);
		}
		// remove from security
		SecurityEngineUtils.deleteEngine(engineId);
		// remove from user tracking
		UserTrackingUtils.deleteEngine(engineId);
		// remove the cache
		EngineSyncUtility.clearEngineCache(engineId);

		// now try to actually remove from disk
		if (engine != null) {
			try {
				engine.delete();
			} catch (IOException e) {
				classLogger.error("An error occurred attempting to call the delete method for engine {}", engineId, e);
			}
		} else {
			// try to delete based on the name of the folder and smss file
			// which we expect to be based on enginename__engineid
			String thisEngineFolder = EngineUtility.getSpecificEngineVersionFolder(engineType, engineId, engineName);
			File thisEngineF = new File(thisEngineFolder);
			if (thisEngineF.exists() && thisEngineF.isDirectory()) {
				thisEngineF.delete();
			}
			String smssFile = thisEngineFolder + ".smss";
			File thisSmssF = new File(smssFile);
			if (thisSmssF.exists() && thisSmssF.isFile()) {
				thisSmssF.delete();
			}
		}

		return true;
	}

	/**
	 * Get inputs
	 * 
	 * @return list of engines to delete
	 */
	public List<String> getEngineIds() {
		List<String> engineIds = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				engineIds.add(grs.get(i).toString());
			}
			return engineIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			engineIds.add(this.curRow.get(i).toString());
		}
		return engineIds;
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		// default to auto execution for reactors
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		// sidebar to view default json for reactor input+output
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}
}
