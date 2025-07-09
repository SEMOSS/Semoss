package prerna.engine.impl.model.workspace;

import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class AddWorkspaceReactor extends AbstractReactor {

  private static final Logger LOGGER = LogManager.getLogger(AddWorkspaceReactor.class);

  public static final String NAME = "name";
  public static final String DESCRIPTION = "description";
  public static final String SYSTEM_PROMPT = "systemPrompt";
  public static final String SHARING_ENABLED = "sharingEnabled";

  public AddWorkspaceReactor() {
    this.keysToGet = new String[] {NAME, DESCRIPTION, SYSTEM_PROMPT, SHARING_ENABLED};
    this.keyRequired = new int[] {1, 0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User owner = this.insight.getUser();

    String workspaceId = UUID.randomUUID().toString();
    String workspaceName = this.keyValue.get(NAME);
    String workspaceDescription = Utility.decodeURIComponent(this.keyValue.get(DESCRIPTION));
    String workspaceSystemPrompt = Utility.decodeURIComponent(this.keyValue.get(SYSTEM_PROMPT));
    boolean sharingEnabled = Boolean.parseBoolean(this.keyValue.get(SHARING_ENABLED));

    try {
      ModelInferenceLogsUtils.createNewWorkspaceEntry(
          workspaceId,
          owner.getPrimaryLoginToken().getId(),
          workspaceName,
          workspaceDescription,
          workspaceSystemPrompt,
          sharingEnabled);
    } catch (Exception e) {
      return getError(e.getMessage());
    }

    if (sharingEnabled) {
      try {
        ModelInferenceLogsUtils.createWorkspaceProject(
            owner, workspaceId, ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG + "_" + workspaceId);
      } catch (Exception e) {
        LOGGER.error(Constants.STACKTRACE, e);
        try {
          ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
        } catch (Exception e2) {
          LOGGER.error(Constants.STACKTRACE, e2);
        }
        return getError("Failed to create workspace: " + e.getMessage());
      }
    }
    return getSuccess(workspaceId);
  }
}
