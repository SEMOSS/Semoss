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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetWorkspaceKnowledgeBaseReactor extends AbstractReactor {

  public GetWorkspaceKnowledgeBaseReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.WORKSPACE_ID.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
    this.keyRequired = new int[] {1, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
    Map<String, Object> paramMap = getMap();

    User user = this.insight.getUser();

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

    List<Map<String, Object>> knowledgeBase = new ArrayList<>();
    List<Map<String, Object>> workspaceKnowledgeEntries =
        ModelInferenceLogsUtils.getWorkspaceResourcesByType(
            workspaceId, IEngine.CATALOG_TYPE.VECTOR.toString());
    for (Map<String, Object> workspaceKnowledgeEntry : workspaceKnowledgeEntries) {
      String knowledgeId = (String) workspaceKnowledgeEntry.get("resource_id");
      if (knowledgeId == null) continue;

      IVectorDatabaseEngine engine = Utility.getVectorDatabase(knowledgeId);
      if (engine == null) continue;

      knowledgeBase.addAll(engine.listDocuments(paramMap));
    }
    return new NounMetadata(knowledgeBase, PixelDataType.MAP);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getMap() {
    GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
    if (mapGrs != null && !mapGrs.isEmpty()) {
      List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
      if (mapInputs != null && !mapInputs.isEmpty()) {
        return (Map<String, Object>) mapInputs.get(0).getValue();
      }
    }
    List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (mapInputs != null && !mapInputs.isEmpty()) {
      return (Map<String, Object>) mapInputs.get(0).getValue();
    }
    return new HashMap<>();
  }
}
