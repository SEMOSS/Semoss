package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    this.keysToGet = new String[] {ReactorKeysEnum.WORKSPACE_ID.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
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

    Object currentlyIsActive = current.get("is_active");
    Boolean currentlyActive = (Boolean) currentlyIsActive;
    
    if (Boolean.TRUE != currentlyActive || !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(workspaceId, user)) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }

    List<Map<String, Object>> knowledgeBase = new ArrayList<>();
    List<Map<String, Object>> workspaceKnowledgeEntries = 
        ModelInferenceLogsUtils.getWorkspaceResourcesByType(workspaceId, IEngine.CATALOG_TYPE.VECTOR.toString());
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
    GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
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
