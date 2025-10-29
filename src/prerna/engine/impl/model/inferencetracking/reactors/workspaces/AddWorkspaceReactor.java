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
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
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

  public AddWorkspaceReactor() {
    this.keysToGet = new String[] {NAME, DESCRIPTION, SYSTEM_PROMPT, ReactorKeysEnum.MCP.getKey()};
    this.keyRequired = new int[] {1, 0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User owner = this.insight.getUser();

    String workspaceId = UUID.randomUUID().toString();
    String workspaceName = this.keyValue.get(NAME);
    
	if (!Utility.validateName(workspaceName)) {
		throw new IllegalArgumentException("Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
	}
	
    String workspaceDescription = Utility.decodeURIComponent(this.keyValue.get(DESCRIPTION));
    String workspaceSystemPrompt = Utility.decodeURIComponent(this.keyValue.get(SYSTEM_PROMPT));

    List<Map<String, Object>> mcpMapList = getMcpMapList();
    Set<String> vectorDbs = new HashSet<>();
    Set<String> functions = new HashSet<>();
    Set<String> projectDependencies = new HashSet<>();

    if (!mcpMapList.isEmpty()) {
      for (Map<String, Object> mcpMap : mcpMapList) {
        if (mcpMap.containsKey("type") && mcpMap.containsKey("id")) {
          String type = (String) mcpMap.get("type");
          String id = (String) mcpMap.get("id");
          CATALOG_TYPE catalogType = CATALOG_TYPE.valueOf(type);
          switch (catalogType) {
            case VECTOR:
              vectorDbs.add(id);
              break;
            case FUNCTION:
              functions.add(id);
              break;
            case PROJECT:
              projectDependencies.add(id);
              break;
            default:
              return getError("Unsupported tool type: " + type);
          }
        } else {
          return getError("Tool map must contain both type and id");
        }
      }
    }

    
    List<Map<String, String>> workspaceResources = new ArrayList<>();
    for(String vectorDb : vectorDbs) {
    	if(!SecurityEngineUtils.userCanViewEngine(owner, vectorDb)) {
    		return getError("User lacks permission to one of the given vector dbs: " + vectorDb);
    	}
    	workspaceResources.add(makeResourceEntryMap(workspaceId, vectorDb));
    }
    for(String function : functions) {
    	if(!SecurityEngineUtils.userCanViewEngine(owner, function)) {
    		return getError("User lacks permission to one of the given functions: " + function);
    	}
    	workspaceResources.add(makeResourceEntryMap(workspaceId, function));
    }
    
    for (String project : projectDependencies) {
    	if (!SecurityProjectUtils.userCanViewProject(owner, project)) {
    		return getError("User lacks permission to one of the mcp tools/projects: " + project);
    	}
    	workspaceResources.add(makeProjectResourceEntryMap(workspaceId, project));
    }
    
    try {
      ModelInferenceLogsUtils.createNewWorkspaceEntry(
          workspaceId,
          owner.getPrimaryLoginToken().getId(),
          workspaceName,
          workspaceDescription,
          workspaceSystemPrompt,
          workspaceResources);
      
      ModelInferenceLogsUtils.createWorkspaceProject(
              owner, workspaceId, workspaceName);
    } catch (Exception e) {
	   LOGGER.error(Constants.STACKTRACE, e);
	   try {
	     ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
	   } catch (Exception e2) {
	     LOGGER.error(Constants.STACKTRACE, e2);
	   }
	   return getError("Failed to create workspace: " + e.getMessage());
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
	
	private Map<String, String> makeProjectResourceEntryMap(String workspaceId, String project) {
		Map<String, String> resource = new HashMap<>();
		IProject projectObj = Utility.getProject(project);
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", project);
		resource.put("resource_type", CATALOG_TYPE.PROJECT.name());
		resource.put("resource_subtype", projectObj.getProjectType().name());
		return resource;
	}

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getMcpMapList() {
    List<Map<String, Object>> mcpMapList = new ArrayList<>();
      GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MCP.getKey());
      if (grs != null && !grs.isEmpty()) {
          int size = grs.size();
          for (int i = 0; i < size; i++) {
        	 mcpMapList.add((Map<String, Object>) grs.get(i));
          }
      }
      return mcpMapList;
  }
}
