package prerna.util.git.reactors;

import java.util.List;
import java.util.Vector;

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
import prerna.util.git.GitRepoUtils;

public class CommitAssetReactor extends AbstractReactor {

	public CommitAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();
		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		String comment = this.keyValue.get(this.keysToGet[1]);
		String space = this.keyValue.get(this.keysToGet[2]);
		if(space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
			// if we are in the insight space
			// it must be a saved insight
			if(!this.insight.isSavedInsight()) {
				return NounMetadata.getWarningNounMessage("Unable to commit file. All files will be commited once the insight is saved.");
			}
		}
		
		String assetFolder = AssetUtility.getAssetVersionBasePath(this.insight, space, true);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		
		// check the file to see if it is version/
		// if not add it here
		// make the asset folder to be the first piece of the file path
		// need to get the first piece of filepath
		// add it to the asset
		// and pass that as asset folder
		String [] fileTokens = filePath.split("/");
		String baseDir = fileTokens[0];
		assetFolder = assetFolder + "/" + baseDir;
		filePath = filePath.replace(baseDir, "");

		// add file to git
		List<String> files = new Vector<>();
		files.add(relativePath + DIR_SEPARATOR + filePath);		
		GitRepoUtils.addSpecificFiles(assetFolder, files);

		// commit it
		GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);
		if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
			AuthProvider provider = user.getPrimaryLogin();
			String projectId = user.getAssetProjectId(provider);
			if(projectId!=null && !(projectId.isEmpty())) {
				ClusterUtil.pushUserWorkspace(projectId, true);
			}
		} else {
			// if space is null or it is in the insight, push using insight id to get engine
			if(space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				IProject project = Utility.getProject(this.insight.getProjectId());
				ClusterUtil.pushProjectFolder(project, assetFolder);
			} else {
				// this is a project asset. space is the projectId
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, assetFolder);
			}
		}

		return NounMetadata.getSuccessNounMessage("Success!");
	}
}
