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
package prerna.auth;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.servlet.http.HttpSession;
import prerna.cluster.util.ClusterUtil;
import prerna.util.Constants;

public class SyncUserAssetsThread implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(SyncUserAssetsThread.class);

	Collection<String> workspaceIds = null;
	Collection<String> assetIds = null;

	public SyncUserAssetsThread(User user) {
		this.assetIds = user.getAssetEngineMap().values();
	}

	public SyncUserAssetsThread(Collection<String> assetIds) {
		this.assetIds = assetIds;
	}

	public SyncUserAssetsThread(String assetId) {
		if (assetId != null) {
			this.assetIds = new ArrayList<String>();
			this.assetIds.add(assetId);
		}
	}

	@Override
	public void run() {
		execute(this.assetIds);
	}

	/**
	 * TODO: should change this such that we dont require HttpSession jar from
	 * tomcat
	 * 
	 * @param session
	 */
	public static void execute(HttpSession session) {
		if (ClusterUtil.IS_CLUSTER) {
			Collection<String> assetIds = null;

			// now push all the values to be stored
			User user = (User) session.getAttribute(Constants.SESSION_USER);
			if (user != null) {
				assetIds = user.getAssetEngineMap().values();
			} else {
				// grab the maps from the session
				if (session.getAttribute(Constants.USER_ASSET_IDS) != null) {
					assetIds = ((Map<AuthProvider, String>) session.getAttribute(Constants.USER_ASSET_IDS)).values();
				}
			}

			execute(assetIds);
		}
	}

	public static void execute(Collection<String> assetIds) {
		if (ClusterUtil.IS_CLUSTER) {
			if (assetIds != null) {
				for (String assetAppId : assetIds) {
					try {
						ClusterUtil.pushUserAsset(assetAppId);
					} catch (Exception e) {
						classLogger.error("Failed to push user asset app {} to cloud storage", assetAppId, e);
					}
				}
			}
		}
	}

}
