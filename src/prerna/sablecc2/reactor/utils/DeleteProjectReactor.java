package prerna.sablecc2.reactor.utils;

import java.util.List;
import java.util.Vector;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUpdateUtils;
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
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DeleteProjectReactor extends AbstractReactor {

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
			if(AbstractSecurityUtils.securityEnabled()) {
				projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
				boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
				if(!isAdmin) {
					boolean isOwner = SecurityProjectUtils.userIsOwner(user, projectId);
					if(!isOwner) {
						throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have permissions to delete the project. "
								+ "User must be the owner to perform this function.");
					}
				}
			}
//			else {
//				projectId = MasterDatabaseUtility.testEngineIdIfAlias(projectId);
//				if(!MasterDatabaseUtility.getAllEngineIds().contains(projectId)) {
//					throw new IllegalArgumentException("Project " + projectId + " does not exist");
//				}
//			}

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
		project.deleteProject();

		// remove from dihelper... this is absurd
		String projectIds = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		projectIds = projectIds.replace(";" + projectId, "");
		// in case it was the first engine loaded
		projectIds = projectIds.replace(projectId + ";", "");
		DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projectIds);

		SecurityUpdateUtils.deleteProject(projectId);
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
