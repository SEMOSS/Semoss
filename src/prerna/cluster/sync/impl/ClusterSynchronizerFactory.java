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
package prerna.cluster.sync.impl;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.UserAssetUtils;
import prerna.cluster.sync.IClusterSynchronizer;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;

/**
 * Selects and supplies the active {@link IClusterSynchronizer} implementation
 * based on which cluster-sync flag is enabled ({@code SEMOSS_IS_CLUSTER_REDIS}
 * or {@code SEMOSS_IS_CLUSTER_ZK}). This is the single entry point callers use
 * so the rest of the codebase stays agnostic of the underlying coordination
 * technology.
 */
public class ClusterSynchronizerFactory {

	private static final Logger classLogger = LogManager.getLogger(ClusterSynchronizerFactory.class);

	/** Whether the Redis-backed synchronizer is enabled. */
	private static final boolean IS_REDIS = Boolean
			.parseBoolean(IClusterSynchronizer.CLOUD_PROPS.get(IClusterSynchronizer.SEMOSS_IS_CLUSTER_REDIS_KEY));
	/** Whether the ZooKeeper-backed synchronizer is enabled. */
	private static final boolean IS_ZK = Boolean
			.parseBoolean(IClusterSynchronizer.CLOUD_PROPS.get(IClusterSynchronizer.SEMOSS_IS_CLUSTER_ZK_KEY));

	/** {@code true} when any cluster-synchronization backend is configured. */
	public static final boolean IS_CLUSTER_SYNC_SETUP = IS_REDIS || IS_ZK;

	static {
		if (IS_REDIS && IS_ZK) {
			classLogger.warn("Both '{}' and '{}' are enabled; defaulting to Redis for cluster synchronization.",
					IClusterSynchronizer.SEMOSS_IS_CLUSTER_REDIS_KEY, IClusterSynchronizer.SEMOSS_IS_CLUSTER_ZK_KEY);
		}
	}

	/**
	 * Returns the active cluster synchronizer for the configured backend. Redis
	 * takes precedence when both flags are enabled.
	 *
	 * @return the {@link IClusterSynchronizer} for the enabled backend
	 * @throws IllegalStateException if no cluster-sync backend is configured
	 * @throws Exception             if the selected synchronizer fails to
	 *                               initialize
	 */
	public static IClusterSynchronizer getClusterSynchronizer() throws Exception {
		if (IS_REDIS) {
			return RedisClusterSynchronizer.getInstance();
		}
		if (IS_ZK) {
			return ZKClusterSynchronizer.getInstance();
		}

		throw new IllegalStateException(
				"No cluster synchronizer is configured. Enable '" + IClusterSynchronizer.SEMOSS_IS_CLUSTER_REDIS_KEY
						+ "' or '" + IClusterSynchronizer.SEMOSS_IS_CLUSTER_ZK_KEY + "'.");
	}

	/**
	 * Determines whether a project is currently loaded on this node.
	 *
	 * @param projectId project identifier
	 * @return {@code true} if loaded and should be refreshed, otherwise
	 *         {@code false}
	 */
	static boolean projectLoaded(String projectId) {
		String projects = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS) + "";

		if (projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
				|| projects.endsWith(";" + projectId)) {
			classLogger.info("Loaded project {} is out of date. Pulling latest changes", projectId);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Determines whether an engine is currently loaded on this node.
	 *
	 * @param engineId engine identifier
	 * @return {@code true} if loaded and should be refreshed, otherwise
	 *         {@code false}
	 */
	static boolean engineLoaded(String engineId) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";

		if (engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
				|| engines.endsWith(";" + engineId)) {
			classLogger.info("Loaded engine {} is out of date. Pulling latest changes", engineId);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Determines whether a user asset project is present locally on this node.
	 *
	 * @param projectId user asset project identifier
	 * @return {@code true} if local files indicate loaded state, otherwise
	 *         {@code false}
	 */
	static boolean userLoaded(String projectId) {
		// User assets are not registered in DIHelper like projects/engines.
		// Check if the SMSS file exists locally it is only written here by
		// pullUserAssetOrWorkspace, meaning this pod has previously fetched this
		// user's data and may have a stale copy.
		String userFolder = EngineUtility.USER_FOLDER;
		String assetSmss = userFolder + "/" + SmssUtilities.getUniqueName(UserAssetUtils.ASSET_APP_NAME, projectId)
				+ ".smss";
		if (new File(assetSmss).exists()) {
			classLogger.info("User asset {} is out of date. Pulling latest changes", projectId);
			return true;
		}
		return false;
	}
}
