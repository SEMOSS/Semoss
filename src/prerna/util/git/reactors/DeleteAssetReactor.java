package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class DeleteAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteAssetReactor.class);
	
	public DeleteAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// check if user is logged in
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		// get asset base folder
		String space = this.keyValue.get(this.keysToGet[2]);
		String baseFolderPath = AssetUtility.getAssetVersionBasePath(this.insight, space, true);
		// relative path is used for git if insight is saved
		// or if we are dealing with project space
		String relativePath = "";
		if (space != null || insight.isSavedInsight()) {
			relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		}

		// get the file path to delete
		String inputFilePath = Utility.normalizePath(keyValue.get(keysToGet[0]));
		if(inputFilePath == null || (inputFilePath.trim().isEmpty())) {
			throw new IllegalArgumentException("Must pass in a file name to delete");
		}
		String comment = this.keyValue.get(this.keysToGet[1]);

		String realFilePath = baseFolderPath+"/"+inputFilePath;
		realFilePath=realFilePath.replace("\\", "/");
		File realFile = new File(realFilePath);
		if(!realFile.exists()) {
			throw new IllegalArgumentException("Cannot find the folder/file at path " + inputFilePath);
		}
		
		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(relativePath + "/" + inputFilePath);

		if(realFile.isDirectory()) {
			try {
				FileUtils.deleteDirectory(realFile);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred trying to delete folder at path " + inputFilePath);
			}
		} else {
			try {
				FileUtils.forceDelete(realFile);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred trying to delete file at path " + inputFilePath);
			}
		}

		// commit it
		// but need to make sure we are in a git...
		// which depends on the space and if the insight is saved...
		if(insight.isSavedInsight() && (space == null || AssetUtility.INSIGHT_SPACE_KEY.equalsIgnoreCase(space))) {
			IProject project = Utility.getProject(this.insight.getProjectId());
			String gitVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), insight.getProjectId()).replace("\\", "/");
			if(realFilePath.startsWith(gitVersionFolder)) {
				GitDestroyer.removeSpecificFiles(gitVersionFolder, true, gitRelativeFilePaths);
				GitRepoUtils.commitAddedFiles(gitVersionFolder, comment, author, email);
			}
		} else if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
			AuthProvider provider = user.getPrimaryLogin();
			String userProjectId = user.getAssetProjectId(provider);
			if(userProjectId!=null && !(userProjectId.isEmpty())) {
				IProject userProject = Utility.getUserAssetWorkspaceProject(userProjectId, true);
				String gitVersionFolder = AssetUtility.getUserAssetAndWorkspaceAssetFolder(userProject.getProjectName(), userProject.getProjectId()).replace("\\", "/");
				if(realFilePath.startsWith(gitVersionFolder)) {
					GitDestroyer.removeSpecificFiles(gitVersionFolder, true, gitRelativeFilePaths);
					GitRepoUtils.commitAddedFiles(gitVersionFolder, comment, author, email);
				}
			}
		} else {
			IProject project = Utility.getProject(space);
			String gitVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), space).replace("\\", "/");
			if(realFilePath.startsWith(gitVersionFolder)) {
				GitDestroyer.removeSpecificFiles(gitVersionFolder, true, gitRelativeFilePaths);
				GitRepoUtils.commitAddedFiles(gitVersionFolder, comment, author, email);
			}
		}
		
		//TODO: consolidate below with above
		//TODO: create new methods to directly deleteFromStorage instead of an entire sync
		
		// push to the cloud
		if(ClusterUtil.IS_CLUSTER) {
			//is it a user asset  change
			if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
				AuthProvider provider = user.getPrimaryLogin();
				String projectId = user.getAssetProjectId(provider);
				if(projectId!=null && !(projectId.isEmpty())) {
					ClusterUtil.pushUserWorkspace(projectId, true);
				}
			// is it an insight asset change
			} else if(space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				if(this.insight.isSavedInsight()) {
					// we can limit the push the parent directory of the deleted content
					IProject project = Utility.getProject(this.insight.getProjectId());
					ClusterUtil.pushProjectFolder(project, realFile.getParent());
				}
			// this is a project asset. space is the projectId
			} else {
				// we can limit the push the parent directory of the deleted content
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, realFile.getParent());
			}
		}

		return NounMetadata.getSuccessNounMessage("Success!");
	}
}
