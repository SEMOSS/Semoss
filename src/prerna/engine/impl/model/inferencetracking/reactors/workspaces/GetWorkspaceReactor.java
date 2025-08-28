/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.List;
import java.util.Map;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetWorkspaceReactor extends AbstractReactor {

  public static final String WITH_RESOURCES = "withResources";

  public GetWorkspaceReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.WORKSPACE_ID.getKey(), WITH_RESOURCES};
    this.keyRequired = new int[] {1, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();

    String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
    boolean withResources = !"false".equalsIgnoreCase(this.keyValue.get(WITH_RESOURCES));

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
            || !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(workspaceId, user))) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }

    if (withResources) {
      List<Map<String, Object>> resources =
          ModelInferenceLogsUtils.getWorkspaceResourcesByType(workspaceId, null);
      current.put("resources", resources);
    }

    return new NounMetadata(current, PixelDataType.MAP);
  }
}
