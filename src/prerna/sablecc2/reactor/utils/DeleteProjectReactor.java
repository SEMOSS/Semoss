package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteProjectRunner;
import prerna.project.api.IProject;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.upload.UploadUtilities;

public class DeleteProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteProjectReactor.class);

	public DeleteProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		List<String> projectIds = getProjectIds();
		for (String projectId : projectIds) {
			if(WorkspaceAssetUtils.isAssetOrWorkspaceProject(projectId)) {
				throw new IllegalArgumentException("Users are not allowed to delete your workspace or asset app.");
			}
			User user = this.insight.getUser();
			
			// we may have the alias
			projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
			if(!isAdmin) {
				if(AbstractSecurityUtils.adminOnlyProjectDelete()) {
					throwFunctionalityOnlyExposedForAdminsError();
				}
				
				boolean isOwner = SecurityProjectUtils.userIsOwner(user, projectId);
				if(!isOwner) {
					throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have permissions to delete the project. "
							+ "User must be the owner to perform this function.");
				}
			}

			IProject project = Utility.getProject(projectId);
			deleteProject(project);

			// Run the delete thread in the background for removing from cloud storage
			if (ClusterUtil.IS_CLUSTER) {
				Thread deleteThread = new Thread(new DeleteProjectRunner(projectId));
				deleteThread.start();
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_PROJECT);
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

	/**
	 * Get inputs
	 * @return list of projects to delete
	 */
	public List<String> getProjectIds() {
		List<String> projectIds = new Vector<String>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				projectIds.add(grs.get(i).toString());
			}
			return projectIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			projectIds.add(this.curRow.get(i).toString());
		}
		return projectIds;
	}
}
