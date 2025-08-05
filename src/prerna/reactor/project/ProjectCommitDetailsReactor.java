package prerna.reactor.project;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
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
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the projectid");
		}
		if (limit == null || limit.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the limit");
		}
		if (offset == null || offset.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the offset");
		}

		List<Map<String, Object>> commits = new ArrayList<>();
		String filePath = null;

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		String gitGetLogCommand=null;
		try {
			gitGetLogCommand = "git log --pretty=format:%H%n%an%n%ae%n%ad%n%s";
			String[] command=gitGetLogCommand.split(" ");
			ProcessBuilder builder = new ProcessBuilder(command);
			IProject project = Utility.getProject(projectId);
			filePath = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
			builder.directory(new File(filePath));
			Process process = builder.start();

			InputStream inputStream = process.getInputStream();
			byte[] allBytes = inputStream.readAllBytes();
			String output = new String(allBytes, Charset.forName("UTF-8"));

			String[] lines = output.split("\n");

			for (int i = 0; i < lines.length - 3; i += 5) {
				Map<String, Object> details = new LinkedHashMap<>();
				details.put("commitId", lines[i]);
				Map<String, String> authorDetails = new LinkedHashMap<>();
				authorDetails.put("userId", lines[i + 1]);
				authorDetails.put("userEmail", lines[i + 2]);
				details.put("author", authorDetails);
				details.put("date", lines[i + 3]);
				details.put("commitMessage", lines[i + 4]);
				commits.add(details);
			}

		} catch (IOException e) {
			classLogger.error("Command failed " + gitGetLogCommand, e);
			throw new IllegalArgumentException("Command failed " + gitGetLogCommand, e);
		}

		List<Map<String, Object>> paginatedCommits = new ArrayList<>();

		int totalCommits = commits.size();
		int fromIndex = Math.min((Integer.parseInt(offset) - 1) * Integer.parseInt(limit), totalCommits);
		int toIndex = Math.min(fromIndex + Integer.parseInt(limit), totalCommits);

		paginatedCommits.addAll(commits.subList(fromIndex, toIndex));

		return new NounMetadata(paginatedCommits, PixelDataType.MAP, PixelOperationType.PROJECT_INFO);
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