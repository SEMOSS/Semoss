package prerna.reactor.project;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class ProjectCommitRestoreReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ProjectCommitRestoreReactor.class);

	public ProjectCommitRestoreReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.COMMIT_ID_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String commitId = this.keyValue.get(this.keysToGet[1]);

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the projectid");
		}
		if (commitId == null || commitId.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the commitid");
		}

		User user = this.insight.getUser();

		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to restore the commits",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectVersionFolder = null;
		try {

			IProject project = Utility.getProject(projectId);
			projectVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);

		} catch (Exception e) {
			classLogger.error("Please provide a valid projectid " + projectId, e);
			throw new IllegalArgumentException("Please provide a valid projectid " + projectId, e);
		}

		try {
			String gitCheckoutCommand = "git checkout " + commitId + " -- .";
			String[] checkoutCommand = gitCheckoutCommand.split(" ");
			runCommand(projectVersionFolder, checkoutCommand);

			String gitAddCommand = "git add .";
			String[] addCommand = gitAddCommand.split(" ");
			runCommand(projectVersionFolder, addCommand);

		} catch (Exception e) {
			classLogger.error("Please provide a valid commitid " + commitId, e);
			throw new IllegalArgumentException("Please provide a valid commitid " + commitId, e);
		}

		GitRepoUtils.commitAddedFiles(projectVersionFolder, "Reverted to commit: " + commitId, user);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	private static void runCommand(String workingDir, String[] command) throws IOException, InterruptedException {
		ProcessBuilder pb = new ProcessBuilder(command);
		pb.directory(new File(workingDir));
		pb.redirectErrorStream(true);
		Process process = pb.start();

		int exitCode = process.waitFor();
		if (exitCode != 0) {
			throw new RuntimeException();
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the details of all the commits in a project";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "This is a required field containing the project id of a project";
		} else if (key.equals(ReactorKeysEnum.COMMIT_ID_KEY.getKey())) {
			return "This is a required field containing the commit id of a project";
		}
		return super.getDescriptionForKey(key);
	}

}
