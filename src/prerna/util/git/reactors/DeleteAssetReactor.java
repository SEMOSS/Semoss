package prerna.util.git.reactors;

import java.util.List;
import java.util.Vector;

import org.h2.store.fs.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class DeleteAssetReactor extends AbstractReactor {

	public DeleteAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// check if user is logged in
		User user = this.insight.getUser();
		String author = null;
		String email = null;
		// check if user is logged in
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
			// Get the user's email
			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			email = accessToken.getEmail();
			author = accessToken.getUsername();
		}

		// get asset base folder
		String space = this.keyValue.get(this.keysToGet[2]);
		String assetFolder = AssetUtility.getAssetVersionBasePath(this.insight, space, true);
		// relative path is used for git if insight is saved
		String relativePath = "";
		if (this.insight.isSavedInsight()) {
			relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		}

		// get the file path to delete
		String fileName = Utility.normalizePath(keyValue.get(keysToGet[0]));
		String comment = this.keyValue.get(this.keysToGet[1]);

		List<String> files = new Vector<>();
		files.add(relativePath + "/" + fileName);
		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String baseFolder = AssetUtility.getAssetBasePath(this.insight, space, AbstractSecurityUtils.securityEnabled());
		FileUtils.delete(baseFolder + "/" + fileName);

		// commit it
		if (this.insight.isSavedInsight()) {
			GitDestroyer.removeSpecificFiles(assetFolder, true, files);
			GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);
		}
		
		// push to the cloud
		if(ClusterUtil.IS_CLUSTER) {
			//is it a user asset  change
			if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
				AuthProvider provider = user.getPrimaryLogin();
				String projectId = user.getAssetProjectId(provider);
				if(projectId!=null && !(projectId.isEmpty())) {
					IProject project = Utility.getUserAssetWorkspaceProject(projectId, true);
					ClusterUtil.reactorPushUserWorkspace(project, true);
				}
			// is it an insight asset change
			} else if(space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				IProject project = Utility.getProject(this.insight.getProjectId());
				ClusterUtil.reactorPushProjectFolder(project, assetFolder);
			// this is a project asset. space is the projectId
			} else {
				IProject project = Utility.getProject(space);
				ClusterUtil.reactorPushProjectFolder(project, assetFolder);
			}
		}

		return NounMetadata.getSuccessNounMessage("Success!");
	}
}
