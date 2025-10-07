package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.List;
import java.util.Map;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

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
    
    // convert legacy workspaces into projects
    if (!AbstractSecurityUtils.containsProjectId(workspaceId)) {
    	ModelInferenceLogsUtils.createWorkspaceProject(
                user, workspaceId, ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG + "_" + workspaceId);
    }

    String permission = null;
    long userCount = 1;
    
    Object currentlyIsActive = current.get("is_active");
    Boolean currentlyActive = (Boolean) currentlyIsActive;
    
    if (Boolean.TRUE != currentlyActive || !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(workspaceId, user)) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }
    
    try {
        permission = SecurityProjectUtils.getActualUserProjectPermission(user, workspaceId);
        userCount = SecurityProjectUtils.getProjectUsersCount(user, workspaceId, null, null);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    
    if(withResources) {
    	List<Map<String, Object>> resources = ModelInferenceLogsUtils.getWorkspaceResourcesByType(workspaceId, null);
    	current.put("resources", resources);
    }

    current.put("permission", permission);
    current.put("number_collaborators", userCount);

    return new NounMetadata(current, PixelDataType.MAP);
  }
}
