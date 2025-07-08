package prerna.engine.impl.model.workspace;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class EditWorkspaceReactor extends AbstractReactor {
	private static final Logger LOGGER = LogManager.getLogger(EditWorkspaceReactor.class);
	
	public static final String WORKSPACE_ID = "workspaceId";
	public static final String NAME = "name";
	public static final String DESCRIPTION = "description";
	public static final String SYSTEM_PROMPT = "systemPrompt";
	public static final String SHARING_ENABLED = "sharingEnabled";
	
	public EditWorkspaceReactor() {
		this.keysToGet = new String[] { WORKSPACE_ID, NAME, DESCRIPTION, SYSTEM_PROMPT, SHARING_ENABLED };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		User user = this.insight.getUser();
		
		String workspaceId = this.keyValue.get(WORKSPACE_ID);
		String workspaceName = this.keyValue.get(NAME); 
		String workspaceDescription = Utility.decodeURIComponent(this.keyValue.get(DESCRIPTION));
		String workspaceSystemPrompt = Utility.decodeURIComponent(this.keyValue.get(SYSTEM_PROMPT));
		boolean sharingEnabled = Boolean.parseBoolean(this.keyValue.get(SHARING_ENABLED));
		
		Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if(current == null) {
			throw new IllegalArgumentException("Workspace not found");
		}
		String currentOwner = (String) current.get("owner");

		Object currentlySharingEnabled = current.get("sharing_enabled");
		Boolean currentlyShared = (Boolean) currentlySharingEnabled;
		
		boolean hasOwnerPermission = false;
		if(currentOwner != null) {
			for(AuthProvider provider : user.getLogins()) {
				if(currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
					hasOwnerPermission = true;
					break;
				}
			}
		}
		
		int permissionLevel = Math.min(hasOwnerPermission ? AccessPermissionEnum.OWNER.getId() : Integer.MAX_VALUE, currentlyShared ? ModelInferenceLogsUtils.getWorkspaceSharePermission(workspaceId, user, AccessPermissionEnum.OWNER.getId(), AccessPermissionEnum.EDIT.getId()) : Integer.MAX_VALUE);
		int neededPermissionLevel = ((!currentlyShared && sharingEnabled) || (currentlyShared && !sharingEnabled)) ? AccessPermissionEnum.OWNER.getId() : AccessPermissionEnum.EDIT.getId();
		if(permissionLevel > neededPermissionLevel) {
			throw new IllegalArgumentException("User unauthorized to perform this operation");
		}
		
		try {
			ModelInferenceLogsUtils.updateWorkspaceEntry(workspaceId, workspaceName, workspaceDescription, workspaceSystemPrompt, sharingEnabled);
			if(!currentlyShared && sharingEnabled) {
				if(AbstractSecurityUtils.containsProjectId(workspaceId)) {
					ModelInferenceLogsUtils.enableWorkspaceProject(user, workspaceId);
				} else {
					ModelInferenceLogsUtils.createWorkspaceProject(user, workspaceId, ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG + "_" + workspaceId);
				}
			} else if(currentlyShared && !sharingEnabled) {
				if(AbstractSecurityUtils.containsProjectId(workspaceId)) {
					ModelInferenceLogsUtils.disableWorkspaceProject(workspaceId);
				}
			}
		} catch(Exception e) {
			LOGGER.error(Constants.STACKTRACE, e);
			return getError("Error during workspace update: " + e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
}
