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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DeleteWorkspaceReactor extends AbstractReactor {
  private static final Logger LOGGER = LogManager.getLogger(DeleteWorkspaceReactor.class);

  public DeleteWorkspaceReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.WORKSPACE_ID.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();

    String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

    Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
    if (current == null) {
      throw new IllegalArgumentException("Workspace not found");
    }
    String currentOwner = (String) current.get("owner");

    Object currentlySharingEnabled = current.get("sharing_enabled");
    Boolean currentlyShared = (Boolean) currentlySharingEnabled;

    boolean hasPermission = false;
    if (currentOwner != null) {
      for (AuthProvider provider : user.getLogins()) {
        if (currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
          hasPermission = true;
          break;
        }
      }
    }
    if (!hasPermission
        && (Boolean.TRUE != currentlyShared
            || !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(
                workspaceId, user, AccessPermissionEnum.OWNER.getId()))) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }

    try {
      ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
      if (AbstractSecurityUtils.containsProjectId(workspaceId)) {
        IProject project = Utility.getProject(workspaceId);
        ModelInferenceLogsUtils.deleteWorkspaceProject(workspaceId, project);
      }
    } catch (Exception e) {
      LOGGER.error(Constants.STACKTRACE, e);
      return getError("Error during workspace delete: " + e.getMessage());
    }
    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }
}
