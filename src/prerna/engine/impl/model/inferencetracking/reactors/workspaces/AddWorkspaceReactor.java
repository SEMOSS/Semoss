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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class AddWorkspaceReactor extends AbstractReactor {

  private static final Logger LOGGER = LogManager.getLogger(AddWorkspaceReactor.class);

  public static final String NAME = "name";
  public static final String DESCRIPTION = "description";
  public static final String SYSTEM_PROMPT = "systemPrompt";
  public static final String SHARING_ENABLED = "sharingEnabled";

  public AddWorkspaceReactor() {
    this.keysToGet =
        new String[] {
          NAME,
          DESCRIPTION,
          SYSTEM_PROMPT,
          SHARING_ENABLED,
          ReactorKeysEnum.VECTORDB.getKey(),
          ReactorKeysEnum.FUNCTION.getKey()
        };
    this.keyRequired = new int[] {1, 0, 0, 0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User owner = this.insight.getUser();

    String workspaceId = UUID.randomUUID().toString();
    String workspaceName = this.keyValue.get(NAME);
    String workspaceDescription = Utility.decodeURIComponent(this.keyValue.get(DESCRIPTION));
    String workspaceSystemPrompt = Utility.decodeURIComponent(this.keyValue.get(SYSTEM_PROMPT));
    boolean sharingEnabled = Boolean.parseBoolean(this.keyValue.get(SHARING_ENABLED));

    List<Map<String, String>> workspaceResources = new ArrayList<>();
    Set<String> vectorDbs = getVectorDbs();
    for (String vectorDb : vectorDbs) {
      if (!SecurityEngineUtils.userCanViewEngine(owner, vectorDb)) {
        return getError("User lacks permission to one of the given vector dbs: " + vectorDb);
      }
      workspaceResources.add(makeResourceEntryMap(workspaceId, vectorDb));
    }
    Set<String> tools = getTools();
    for (String tool : tools) {
      if (!SecurityEngineUtils.userCanViewEngine(owner, tool)) {
        return getError("User lacks permission to one of the given functions: " + tool);
      }
      workspaceResources.add(makeResourceEntryMap(workspaceId, tool));
    }

    try {
      ModelInferenceLogsUtils.createNewWorkspaceEntry(
          workspaceId,
          owner.getPrimaryLoginToken().getId(),
          workspaceName,
          workspaceDescription,
          workspaceSystemPrompt,
          sharingEnabled,
          workspaceResources);
    } catch (Exception e) {
      return getError(e.getMessage());
    }

    if (sharingEnabled) {
      try {
        ModelInferenceLogsUtils.createWorkspaceProject(
            owner, workspaceId, ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG + "_" + workspaceId);
      } catch (Exception e) {
        LOGGER.error(Constants.STACKTRACE, e);
        try {
          ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
        } catch (Exception e2) {
          LOGGER.error(Constants.STACKTRACE, e2);
        }
        return getError("Failed to create workspace: " + e.getMessage());
      }
    }
    return getSuccess(workspaceId);
  }

  private Map<String, String> makeResourceEntryMap(String workspaceId, String engine) {
    Map<String, String> resource = new HashMap<>();
    Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engine);
    resource.put("workspace_resource_id", UUID.randomUUID().toString());
    resource.put("workspace_id", workspaceId);
    resource.put("resource_id", engine);
    resource.put("resource_type", typeAndSubtype[0].toString());
    resource.put("resource_subtype", typeAndSubtype[1].toString());
    return resource;
  }

  private Set<String> getVectorDbs() {
    Set<String> inputStrings = new HashSet<>();
    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.VECTORDB.getKey());
    if (grs != null && !grs.isEmpty()) {
      int size = grs.size();
      for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
    }
    return inputStrings;
  }

  private Set<String> getTools() {
    Set<String> inputStrings = new HashSet<>();
    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FUNCTION.getKey());
    if (grs != null && !grs.isEmpty()) {
      int size = grs.size();
      for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
    }
    return inputStrings;
  }
}
