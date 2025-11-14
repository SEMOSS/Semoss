package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DeleteWorkspaceReactor extends AbstractReactor {
  private static final Logger LOGGER = LogManager.getLogger(DeleteWorkspaceReactor.class);

  public DeleteWorkspaceReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.WORKSPACE_ID.getKey()};
    this.keyRequired = new int[] {1};
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
    
    Object currentlyIsActive = current.get("is_active");
    Boolean currentlyActive = (Boolean) currentlyIsActive;
    
    if (Boolean.TRUE != currentlyActive || !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(workspaceId, user)) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }

    try {
      ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
      if (AbstractSecurityUtils.containsProjectId(workspaceId)) {
        IProject project = Utility.getProject(workspaceId);
        ModelInferenceLogsUtils.deleteWorkspaceProject(workspaceId, project);
      }
    } catch (Exception e) {
      LOGGER.error(Constants.STACKTRACE, e);
      return getError("Error during workspace delete: " + e.getMessage());
    }
    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }
}
