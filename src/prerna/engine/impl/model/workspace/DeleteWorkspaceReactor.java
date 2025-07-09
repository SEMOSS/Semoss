package prerna.engine.impl.model.workspace;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DeleteWorkspaceReactor extends AbstractReactor {
  private static final Logger LOGGER = LogManager.getLogger(DeleteWorkspaceReactor.class);

  public static final String WORKSPACE_ID = "workspaceId";

  public DeleteWorkspaceReactor() {
    this.keysToGet = new String[] {WORKSPACE_ID};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();

    String workspaceId = this.keyValue.get(WORKSPACE_ID);

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
            || !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(
                workspaceId, user, AccessPermissionEnum.OWNER.getId()))) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }

    try {
      ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
      if (AbstractSecurityUtils.containsProjectId(workspaceId)) {
        IProject project = Utility.getProject(workspaceId);
        ModelInferenceLogsUtils.deleteWorkspaceProject(workspaceId, project);
      }
      if (AbstractSecurityUtils.containsEngineId(workspaceId)) {
        IEngine engine = Utility.getEngine(workspaceId);
        ModelInferenceLogsUtils.deleteWorkspaceVectorDb(workspaceId, engine);
      }
    } catch (Exception e) {
      LOGGER.error(Constants.STACKTRACE, e);
      return getError("Error during workspace delete: " + e.getMessage());
    }
    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }
}
