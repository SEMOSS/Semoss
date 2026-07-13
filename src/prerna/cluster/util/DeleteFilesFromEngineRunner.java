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
package prerna.cluster.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.sync.impl.ClusterSynchronizerFactory;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class DeleteFilesFromEngineRunner implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(DeleteFilesFromEngineRunner.class);

	private final String ENGINE_ID;
	private final IEngine.CATALOG_TYPE ENGINE_TYPE;
	private final String[] FILE_PATHS;

	public DeleteFilesFromEngineRunner(String engineId, IEngine.CATALOG_TYPE engineType, String[] filePaths) {
		this.ENGINE_ID = engineId;
		this.ENGINE_TYPE = engineType;
		this.FILE_PATHS = filePaths;
	}

	@Override
	public void run() {
		for (int i = 0; i < this.FILE_PATHS.length; i++) {
			try {
				ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, FILE_PATHS[i]);
			} catch (Exception e) {
				classLogger.error("Failed to delete cloud file '{}' for engine '{}'", FILE_PATHS[i], ENGINE_ID, e);
			}
		}

		if (ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP) {
			try {
				ClusterUtil.getClusterSynchronizer().publishEngineChange(ENGINE_ID, ClusterSyncMethod.PULL_ENGINE,
						ENGINE_ID);
			} catch (Exception e) {
				classLogger.error("Failed to publish engine '{}' change to ZK cluster", ENGINE_ID, e);
				SemossPixelException err = new SemossPixelException(
						"Failed to publish engine '" + ENGINE_ID + "' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}
}
