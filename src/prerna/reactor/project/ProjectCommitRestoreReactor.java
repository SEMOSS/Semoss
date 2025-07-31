package prerna.reactor.project;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
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
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String commitId = this.keyValue.get(this.keysToGet[1]);
		
		User user = this.insight.getUser();

		IProject project = Utility.getProject(projectId);
		String projectVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);

		try {

			runCommand(projectVersionFolder, "git", "checkout", commitId, "--", ".");

			runCommand(projectVersionFolder, "git", "add", ".");
			
			GitRepoUtils.commitAddedFiles(projectVersionFolder, "Reverted to commit: " + commitId, user);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	private static void runCommand(String workingDir, String... command) throws IOException, InterruptedException {
		ProcessBuilder pb = new ProcessBuilder(command);
		pb.directory(new File(workingDir));
		pb.redirectErrorStream(true);
		Process process = pb.start();

		int exitCode = process.waitFor();
		if (exitCode != 0) {
			throw new RuntimeException("Command failed: " + String.join(" ", command));
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
