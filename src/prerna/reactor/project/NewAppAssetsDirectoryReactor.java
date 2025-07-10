package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class NewAppAssetsDirectoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(NewAppAssetsDirectoryReactor.class);

	public NewAppAssetsDirectoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), 
				ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] {1,1,0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have access to edit assets.");
		}
		IProject project = Utility.getProject(projectId);

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());

		String filePath = this.keyValue.get(this.keysToGet[1]);
		String comment = this.keyValue.get(this.keysToGet[2]);
		if(comment == null) {
			comment = "add: creating new directory";
		}

		File directory = new File(assetFolder + "/" + filePath);
		try {
			directory.mkdirs();
			File placeholder = new File(directory.getAbsolutePath(), "placeholder.txt");
			FileUtils.writeStringToFile(placeholder, "placeholder", Charset.forName("UTF-8"));
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to create directory: " + filePath);
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + filePath);
		
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushProjectFolder(project, assetFolder);
				
		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Create a new empty directory in the projects assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file to create. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		} else if(key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while creating and saving the new file within the git repository for the project";
		} 
		return super.getDescriptionForKey(key);
	}

}
