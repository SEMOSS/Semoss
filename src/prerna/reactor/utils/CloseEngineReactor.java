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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

/**
 * Closes engines so the next use opens them again from their smss file and
 * files. This is how an engine is restarted: closing shuts down the python
 * process the engine owns, so the reopened engine picks up changed files. In a
 * cluster the close is published to the other nodes, which close the engine
 * where they have it open. The user must own every engine, or be an admin.
 */
public class CloseEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CloseEngineReactor.class);

	public CloseEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);

		// check every engine before closing any
		List<String> engineIds = getEngineIds();
		for (String engineId : engineIds) {
			if (!isAdmin && !SecurityEngineUtils.userIsOwner(user, engineId)) {
				throw new IllegalArgumentException("Engine " + engineId
						+ " does not exist or user does not have permissions to close the engine. User must be the owner to perform this function.");
			}
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ENGINE_INFO);
		for (String engineId : engineIds) {
			classLogger.info("Closing engine {} for a restart", Utility.cleanLogString(engineId));
			boolean closed = EngineSyncUtility.closeLoadedEngine(engineId);
			try {
				ClusterUtil.publishEngineClose(engineId);
			} catch (SemossPixelException e) {
				// keep closing the remaining engines, and tell the user which nodes
				// still have the old instance open
				noun.addAdditionalReturn(NounMetadata.getWarningNounMessage(e.getMessage()));
			}
			String message = closed ? "Closed engine " + engineId + ". It restarts on its next use."
					: "Engine " + engineId + " was not running. It starts on its next use.";
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(message));
		}
		return noun;
	}

	/**
	 * Reads the engine ids to close, from the engine key when it is set and
	 * otherwise from every input in the current row.
	 * 
	 * @return list of engines to close
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
	public String getReactorDescription() {
		return """
				Close engines so their next use opens them again from their smss file and files. \
				Use this to restart an engine, for example to pick up changed python files: closing \
				shuts down the python process the engine owns. In a cluster every node closes the engine. \
				The user must own the engines.""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The ids of the engines to close";
		}
		return super.getDescriptionForKey(key);
	}
}
