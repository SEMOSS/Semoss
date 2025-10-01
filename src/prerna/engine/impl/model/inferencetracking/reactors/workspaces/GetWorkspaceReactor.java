package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.List;
import java.util.Map;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
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

    String permission = null;
    long userCount = 1;
    
    if (Boolean.TRUE != currentlyShared) {
      if (currentOwner != null) {
    	  for (AuthProvider provider : user.getLogins()) {
	        if (currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
	          permission = AccessPermissionEnum.OWNER.getPermission();
	          break;
	        }
	      }
      } else {
    	  throw new IllegalArgumentException("User is not a collaborator of this workspace");
      }
      
    } else {
    	if (ModelInferenceLogsUtils.isWorkspaceSharedWithUser(workspaceId, user)) {
    		try {
	          permission = SecurityProjectUtils.getActualUserProjectPermission(user, workspaceId);
	          userCount = SecurityProjectUtils.getProjectUsersCount(user, workspaceId, null, null);
	        } catch (IllegalAccessException e) {
	          e.printStackTrace();
	        }
    	} else {
    		throw new IllegalArgumentException("User is not a collaborator of this workspace");
    	}
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
