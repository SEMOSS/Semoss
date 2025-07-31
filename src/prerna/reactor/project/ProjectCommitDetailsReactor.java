package prerna.reactor.project;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class ProjectCommitDetailsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ProjectCommitDetailsReactor.class);

	public ProjectCommitDetailsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);

		List<List<String>> commits = new ArrayList<>();
		String filePath = null;
		try {
			ProcessBuilder builder = new ProcessBuilder("git", "log", "--pretty=format:%H%nDate: %ad%nMessage: %s%n",
					"--date=local");
			IProject project = Utility.getProject(projectId);
			filePath = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
			builder.directory(new File(filePath));
			Process process = builder.start();

			InputStream inputStream = process.getInputStream();
			byte[] allBytes = inputStream.readAllBytes();
			String output = new String(allBytes, Charset.forName("UTF-8"));

			String[] lines = output.split("\n");

			for (int i = 0; i < lines.length - 2; i += 4) {
				String commitId = lines[i].trim();
				String comitDate = lines[i + 1].replace("Date:", "").trim();
				String commitMessage = lines[i + 2].replace("Message:", "").trim();
				commits.add(new ArrayList(Arrays.asList(commitId, comitDate, commitMessage)));
			}

		} catch (IOException e) {
			classLogger.error("Please provide a valid directory name " + filePath, e);
			throw new IllegalArgumentException("Please provide a valid directory name " + filePath, e);
		}

		return new NounMetadata(commits, PixelDataType.CONST_STRING, PixelOperationType.PROJECT_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the details of all the commits in a project";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "This is a required value containing the project id of a project";
		}
		return super.getDescriptionForKey(key);
	}

}