package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.EditWorkspaceReactor;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class EditWorkspaceReactor extends AbstractReactor {
  private static final Logger LOGGER = LogManager.getLogger(EditWorkspaceReactor.class);

  public static final String NAME = "name";
  public static final String DESCRIPTION = "description";
  public static final String SYSTEM_PROMPT = "systemPrompt";
  public static final String SHARING_ENABLED = "sharingEnabled";
  public static final String IS_ACTIVE = "isActive";

  public EditWorkspaceReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.WORKSPACE_ID.getKey(), NAME, DESCRIPTION, SYSTEM_PROMPT, SHARING_ENABLED, IS_ACTIVE, ReactorKeysEnum.VECTORDB.getKey(), ReactorKeysEnum.FUNCTION.getKey()};
    this.keyRequired = new int[] {1, 1, 0, 0, 0, 0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();

    String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
    String workspaceName = this.keyValue.get(NAME);
    String workspaceDescription = Utility.decodeURIComponent(this.keyValue.get(DESCRIPTION));
    String workspaceSystemPrompt = Utility.decodeURIComponent(this.keyValue.get(SYSTEM_PROMPT));
    boolean sharingEnabled = Boolean.parseBoolean(this.keyValue.get(SHARING_ENABLED));
    boolean isActive = !"false".equalsIgnoreCase(this.keyValue.get(IS_ACTIVE));

    Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
    if (current == null) {
      throw new IllegalArgumentException("Workspace not found");
    }
    String currentOwner = (String) current.get("owner");

    Object currentlySharingEnabled = current.get("sharing_enabled");
    Boolean currentlyShared = (Boolean) currentlySharingEnabled;
    
    Object currentlyIsActive = current.get("is_active");
    Boolean currentlyActive = (Boolean) currentlyIsActive;
    
    List<Map<String, String>> workspaceResources = new ArrayList<>();
    Set<String> vectorDbs = getVectorDbs();
    for(String vectorDb : vectorDbs) {
    	if(!SecurityEngineUtils.userCanViewEngine(user, vectorDb)) {
    		return getError("User lacks permission to one of the given vector dbs: " + vectorDb);
    	}
    	workspaceResources.add(makeResourceEntryMap(workspaceId, vectorDb));
    }
    Set<String> tools = getTools();
    for(String tool : tools) {
    	if(!SecurityEngineUtils.userCanViewEngine(user, tool)) {
    		return getError("User lacks permission to one of the given functions: " + tool);
    	}
    	workspaceResources.add(makeResourceEntryMap(workspaceId, tool));
    }
    
    boolean hasOwnerPermission = false;
    if (currentOwner != null && Boolean.TRUE != currentlyShared) {
      for (AuthProvider provider : user.getLogins()) {
        if (currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
          hasOwnerPermission = true;
          break;
        }
      }
    }

    int permissionLevel =
        Math.min(
            hasOwnerPermission ? AccessPermissionEnum.OWNER.getId() : Integer.MAX_VALUE,
            currentlyShared
                ? ModelInferenceLogsUtils.getWorkspaceSharePermission(
                    workspaceId,
                    user,
                    AccessPermissionEnum.OWNER.getId(),
                    AccessPermissionEnum.EDIT.getId())
                : Integer.MAX_VALUE);
    int neededPermissionLevel =
        ((currentlyShared ^ sharingEnabled) || (currentlyActive ^ isActive))
            ? AccessPermissionEnum.OWNER.getId()
            : AccessPermissionEnum.EDIT.getId();
    if (permissionLevel > neededPermissionLevel) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }
    
    try {
      ModelInferenceLogsUtils.updateWorkspaceEntry(
          workspaceId, workspaceName, workspaceDescription, workspaceSystemPrompt, sharingEnabled, isActive, workspaceResources);
      if (!currentlyShared && sharingEnabled) {
        if (AbstractSecurityUtils.containsProjectId(workspaceId)) {
          ModelInferenceLogsUtils.enableWorkspaceProject(user, workspaceId);
        } else {
          ModelInferenceLogsUtils.createWorkspaceProject(
              user, workspaceId, ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG + "_" + workspaceId);
        }
        List<Map<String, String>> currentResources = ModelInferenceLogsUtils.getWorkspaceResources(workspaceId, null, null);
        if (currentResources != null && !currentResources.isEmpty()) {
        	List<String> currResourceIds = currentResources.stream()
                    .map(resource -> resource.get("resource_id"))
                    .collect(Collectors.toList());
        	SecurityProjectUtils.updateProjectDependencies(user, workspaceId, currResourceIds);
        }
      } else if (currentlyShared && !sharingEnabled) {
    	throw new IllegalArgumentException("Disabling sharing is not permitted at this time - please remove users to make workspace private");
    	//
    	//  if (AbstractSecurityUtils.containsProjectId(workspaceId)) {
    	//  	ModelInferenceLogsUtils.disableWorkspaceProject(workspaceId);
    	//  }
      }
    } catch (Exception e) {
      LOGGER.error(Constants.STACKTRACE, e);
      return getError("Error during workspace update: " + e.getMessage());
    }
    return new NounMetadata(true, PixelDataType.BOOLEAN);
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
	      GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.VECTORDB.getKey());
	      if (grs != null && !grs.isEmpty()) {
	          int size = grs.size();
	          for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
	      }
	      return inputStrings;
	  }

	  private Set<String> getTools() {
	      Set<String> inputStrings = new HashSet<>();
	      GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.FUNCTION.getKey());
	      if (grs != null && !grs.isEmpty()) {
	          int size = grs.size();
	          for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
	      }
	      return inputStrings;
	  }
}
