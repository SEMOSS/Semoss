package prerna.reactor.project;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.lib.Ref;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
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

		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			classLogger.error("Unauthorized access: you must be logged in to perform this action");
			throwAnonymousUserError();
		}

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
		String projectVersionFolder = null;

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		projectVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);

		Git thisGit = null;
		try {

			thisGit = Git.open(new File(projectVersionFolder));

			// Get all tags once
			List<Ref> tagList = thisGit.tagList().call();

			Iterable<RevCommit> gitCommits = thisGit.log().call();

			for (RevCommit commit : gitCommits) {

				// Get all commit details
				Map<String, Object> details = new LinkedHashMap<>();
				details.put("commitId", commit.getName());
				Map<String, String> authorDetails = new LinkedHashMap<>();
				authorDetails.put("userId", commit.getAuthorIdent().getName());
				authorDetails.put("userEmail", commit.getAuthorIdent().getEmailAddress());
				details.put("author", authorDetails);
				details.put("date", commit.getAuthorIdent().getWhen().toString());
				details.put("commitMessage", commit.getFullMessage());

				// Collect tags pointing to this commit
				List<String> tagsForCommit = new ArrayList<>();
				try (RevWalk walk = new RevWalk(thisGit.getRepository())) {
					for (Ref tag : tagList) {
						RevCommit taggedCommit = walk.parseCommit(thisGit.getRepository().peel(tag).getObjectId());
						if (taggedCommit.equals(commit)) {
							tagsForCommit.add(tag.getName().replace("refs/tags/", ""));
						}
					}
				}
				details.put("tags", tagsForCommit);
				commits.add(details);
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (thisGit != null) {
				thisGit.close();
			}
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
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "This is a required field containing the limit of a project";
		} else if (key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return "This is a required field containing the offset a project";
		}
		return super.getDescriptionForKey(key);
	}

}