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
import javax.servlet.http.HttpSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.cluster.util.ClusterUtil;
import prerna.util.Constants;

public class SyncUserAppsThread implements Runnable {
  protected static final Logger classLogger = LogManager.getLogger(SyncUserAppsThread.class);

  Collection<String> workspaceIds = null;
  Collection<String> assetIds = null;

  public SyncUserAppsThread(User user) {
    this.workspaceIds = user.getWorkspaceEngineMap().values();
    this.assetIds = user.getWorkspaceEngineMap().values();
  }

  public SyncUserAppsThread(Collection<String> workspaceIds, Collection<String> assetIds) {
    this.workspaceIds = workspaceIds;
    this.assetIds = assetIds;
  }

  public SyncUserAppsThread(String workspaceId, String assetId) {
    if (workspaceId != null) {
      this.workspaceIds = new ArrayList<String>();
      this.workspaceIds.add(workspaceId);
    }
    if (assetId != null) {
      this.assetIds = new ArrayList<String>();
      this.assetIds.add(assetId);
    }
  }

  @Override
  public void run() {
    execute(this.workspaceIds, this.assetIds);
  }

  /**
   * TODO: should change this such that we dont require HttpSession jar from tomcat
   *
   * @param session
   */
  public static void execute(HttpSession session) {
    if (ClusterUtil.IS_CLUSTER) {
      Collection<String> workspaceIds = null;
      Collection<String> assetIds = null;

      // now push all the values to be stored
      User user = (User) session.getAttribute(Constants.SESSION_USER);
      if (user != null) {
        workspaceIds = user.getWorkspaceEngineMap().values();
        assetIds = user.getAssetEngineMap().values();
      } else {
        // grab the maps from the session
        if (session.getAttribute(Constants.USER_WORKSPACE_IDS) != null) {
          workspaceIds =
              ((Map<AuthProvider, String>) session.getAttribute(Constants.USER_WORKSPACE_IDS))
                  .values();
        }
        if (session.getAttribute(Constants.USER_ASSET_IDS) != null) {
          assetIds =
              ((Map<AuthProvider, String>) session.getAttribute(Constants.USER_ASSET_IDS)).values();
        }
      }

      execute(workspaceIds, assetIds);
    }
  }

  public static void execute(Collection<String> workspaceIds, Collection<String> assetIds) {
    // now push all the values to be stored
    if (ClusterUtil.IS_CLUSTER) {
      if (workspaceIds != null) {
        for (String workspaceAppId : workspaceIds) {
          try {
            ClusterUtil.pushUserWorkspace(workspaceAppId, false);
          } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
          }
        }
      }
      if (assetIds != null) {
        for (String assetAppId : assetIds) {
          try {
            ClusterUtil.pushUserWorkspace(assetAppId, true);
          } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
          }
        }
      }
    }
  }
}
