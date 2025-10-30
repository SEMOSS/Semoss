package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.sql.SQLException;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteEngineRunner;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.utils.DeleteEngineReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
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
	
//	TODO: Account for cluster? Need to pull and push?
	resources.stream().forEach(resourceMap -> {
		String resourceId = (String) resourceMap.getOrDefault("resource_id", null);
//		System.out.println(workspaceId + "\n" + resourceId + "\n" + workspaceId.equals(resourceId));
		if (workspaceId.equals(resourceId)) {
//			Disallows resources in a workspace from having the same id. This will duplicate and delete the old resource.
//			Then, we will mark in the engine SMSS that this resource has been duplicated so that downstream applications can update themselves accordingly
//			CORE MIGRATION

			try {
				
//			First step: Duplicate Engine - pull to a temp folder, change the smss prop + engine id and go through engine creation process
				
//				Get the original engine
				String oldEngineId = resourceId;
				IEngine oldEngine = Utility.getEngine(oldEngineId);

//				Clone the properties map
				Properties newEngineSmssProp = (Properties) oldEngine.getSmssProp().clone();
				
//				Get more properties
				String oldEngineName = null;
				IEngine.CATALOG_TYPE oldEngineType = null;
				if(oldEngine != null) {
					oldEngineName = oldEngine.getEngineName();
					oldEngineType = oldEngine.getCatalogType();
				}

//				Generate new UUID
				String newEngineId = UUID.randomUUID().toString();

//				Same as old name
				String newEngineName = oldEngineName;

//				Same as old class
				IEngine.CATALOG_TYPE newEngineType = oldEngineType;
				String newEngineClass = newEngineType.getClass().getName();

				newEngineSmssProp.put("MOVED_FROM", newEngineSmssProp.get("ENGINE"));
//				Need to remove this as we're adding that directly through the create funct.
				newEngineSmssProp.remove("ENGINE");

				Map<String, Object> propsMap = newEngineSmssProp.stringPropertyNames().stream()
						.collect(Collectors.toMap(key -> key, newEngineSmssProp::getProperty));

				File newEngineSmssTempFile = UploadUtilities.createTemporaryEngineSmss(oldEngine.getCatalogType(),
						newEngineId, newEngineName, newEngineClass, propsMap);

				IEngine newEngine = (IEngine) Class.forName(newEngineClass).getDeclaredConstructor().newInstance();

				newEngine.open(newEngineSmssTempFile.getAbsolutePath());

				File newEngineSmssFile = new File(newEngineSmssTempFile.getAbsolutePath().replace(".temp", ".smss"));
				FileUtils.copyFile(newEngineSmssTempFile, newEngineSmssFile);
				newEngineSmssTempFile.delete();

				newEngine.setSmssFilePath(newEngineSmssFile.getAbsolutePath());

				UploadUtilities.updateDIHelper(newEngineId, newEngineName, newEngine, newEngineSmssFile);

//			      Engine has been duplicated but need to copy over file contents

//			      Old engine file
				File oldEnginePath = new File(EngineUtility.getSpecificEngineBaseFolder(oldEngineId));
//			      New engine file
				File newEnginePath = new File(EngineUtility.getSpecificEngineBaseFolder(newEngineId));

				FileUtils.copyDirectory(oldEnginePath, newEnginePath);
				
//				TODO: We need to propagate the new engine wherever in the workspace

//			      TODO: Unsure if any cluster updates need to take place here
				
//				Next - delete oldEngine
//				TODO: Do we want to make this protected, call from an instance of the deleteEngineReactor instantiated with admin user? Or are we okay to expose and put in utils file
				DeleteEngineReactor.deleteEngine(oldEngine, oldEngineId, oldEngineName, oldEngineType);
				EngineSyncUtility.clearEngineCache(oldEngineId);
				UserTrackingUtils.deleteEngine(oldEngineId);
				// Run the delete thread in the background for removing from cloud storage
				if (ClusterUtil.IS_CLUSTER) {
					Thread deleteAppThread = new Thread(new DeleteEngineRunner(oldEngineId, oldEngineType));
					deleteAppThread.start();
				}
				
//				Finally - edit core workspace resource table to reflect the changes
				
				ModelInferenceLogsUtils.replaceResourceIdForWorkspace(newEngineId, workspaceId, oldEngineId);

//				TODO: Likely need to do some finally cleanup or cleanup depending on error
			} catch (IOException e) {
				classLogger.error("Error with file operations: " + e);
//				TODO: Throw
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException
					| ClassNotFoundException e) {
				// Catch block for reflection-related exceptions
				classLogger.error("Error constructing new engine: " + e);
			} catch (SQLException e) {
				classLogger.error("Error propagating resource change to database: " + e);
			} catch (Exception e) {
				classLogger.error("Error opening engine: " + e);
			}

//			Done
		}
	});
	
	current.put("resources", resources);

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
