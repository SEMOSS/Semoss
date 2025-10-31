package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetWorkspaceReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GetWorkspaceReactor.class);
	
  // To get workspaces without resources, call MyProjects w/ type as workspace
  public GetWorkspaceReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.WORKSPACE_ID.getKey()};
    this.keyRequired = new int[] {1, 0};
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
    
    // convert legacy workspaces into projects
    if (!AbstractSecurityUtils.containsProjectId(workspaceId)) {
    	String workspaceName = (String) current.get("name");
    	if (!Utility.validateName(workspaceName)) {
    		workspaceName = cleanWorkspaceName(workspaceName);
    	}
    	ModelInferenceLogsUtils.createWorkspaceProject(
                user, workspaceId, workspaceName);
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
        classLogger.error(Constants.STACKTRACE, e);
      }
    
	List<Map<String, Object>> resources = ModelInferenceLogsUtils.getWorkspaceResourcesByType(workspaceId, null);
	
	List<Map<String, String>> mcps = new ArrayList<>();
	for (Map<String, Object> r : resources) {
		Map<String, String> mcpMap = new HashMap<>();
		mcpMap.put("id", (String) r.get("resource_id"));
		mcpMap.put("id", (String) r.get("resource_type"));
		mcps.add(mcpMap);
	}
		
	current.put("mcp", mcps);
    current.put("permission", permission);
    current.put("number_collaborators", userCount);

    return new NounMetadata(current, PixelDataType.MAP);
  }
  
  public static String cleanWorkspaceName(String workspaceName) {
	    if (workspaceName == null || workspaceName.isEmpty()) {
	        return "Unnamed Workspace";
	    }
	    
	    // Remove all invalid characters
	    String cleaned = workspaceName.replaceAll("[^a-zA-Z0-9 _-]", "");
	    
	    // Remove leading non-letters
	    cleaned = cleaned.replaceAll("^[^a-zA-Z]*", "");
	    
	    // If string is empty after cleaning, provide a default
	    if (cleaned.isEmpty()) {
	        return "Unnamed Workspace";
	    }
	    
	    return cleaned;
	}

  
}
