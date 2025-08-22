package prerna.reactor.project;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
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

		User user = this.insight.getUser();

		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			classLogger.error("Unauthorized access: you must be logged in to perform this action");
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String commitId = this.keyValue.get(this.keysToGet[1]);

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the projectid");
		}
		if (commitId == null || commitId.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the commitid");
		}

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		String projectVersionFolder = null;
		IProject project = Utility.getProject(projectId);
		projectVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);

		try {
			
			String gitCheckoutCommand = "git checkout " + commitId + " -- .";
			runCommand(projectVersionFolder, gitCheckoutCommand);

			String gitAddCommand = "git add .";
			runCommand(projectVersionFolder, gitAddCommand);
			
		} catch (Exception e) {
			classLogger.error("Please provide a valid commitid " + commitId, e);
			throw new IllegalArgumentException("Please provide a valid commitid " + commitId, e);
		}

		GitRepoUtils.commitAddedFiles(projectVersionFolder, "Reverted to commit: " + commitId, user);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	private static void runCommand(String workingDir, String gitCommand) throws IOException, InterruptedException {
		String[] command=gitCommand.split(" ");
		ProcessBuilder pb = new ProcessBuilder(command);
		pb.directory(new File(workingDir));
		pb.redirectErrorStream(true);
		Process process = pb.start();

		int exitCode = process.waitFor();
		if (exitCode != 0) {
			classLogger.error("Command Failed " + gitCommand);
			throw new RuntimeException();
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor reverts to the requested commit id";
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
