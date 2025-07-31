package prerna.reactor.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

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

		IProject project = Utility.getProject(projectId);
		String filePath = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);

		try {

			runCommand(filePath, "git", "checkout", commitId, "--", ".");

			runCommand(filePath, "git", "add", ".");

			runCommand(filePath, "git", "commit", "-m", "Cloned content from commit: " + commitId);

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

}
