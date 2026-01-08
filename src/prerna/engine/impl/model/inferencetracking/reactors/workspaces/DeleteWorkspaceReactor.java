package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteProjectRunner;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class DeleteWorkspaceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteWorkspaceReactor.class);

	public DeleteWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

		if (AbstractSecurityUtils.adminOnlyProjectDelete()) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		boolean isOwner = SecurityProjectUtils.userIsOwner(user, workspaceId);
		if (!isOwner) {
			throw new IllegalArgumentException("Workspace " + workspaceId
					+ " does not exist or user does not have permissions to delete the workspace. "
					+ "User must be the owner to perform this function.");
		}
		try {
			ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
			if (AbstractSecurityUtils.containsProjectId(workspaceId)) {
				IProject project = Utility.getProject(workspaceId);
				deleteProject(project);
				if (ClusterUtil.IS_CLUSTER) {
					Thread deleteThread = new Thread(new DeleteProjectRunner(workspaceId));
					deleteThread.start();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return getError("Error during workspace delete: " + e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * 
	 * @param project
	 * @return
	 */
	private boolean deleteProject(IProject project) {
		String projectId = project.getProjectId();
		// remove from DIHelper
		UploadUtilities.removeProjectFromDIHelper(projectId);
		// remove from security
		SecurityProjectUtils.deleteProject(projectId);
		// remove from user tracking
		UserTrackingUtils.deleteProject(projectId);

		// now try to actually remove from disk
		try {
			project.delete();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return true;
	}
}
