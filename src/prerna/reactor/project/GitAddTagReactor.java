package prerna.reactor.project;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.RefAlreadyExistsException;
import org.eclipse.jgit.lib.ObjectId;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class GitAddTagReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitAddTagReactor.class);
	private static final String COMMIT_ID_KEY = "commitId";

	public GitAddTagReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), COMMIT_ID_KEY,
				ReactorKeysEnum.TAGS.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {

		User user = this.insight.getUser();
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String commitId = this.keyValue.get(this.keysToGet[1]);
		String tag = this.keyValue.get(this.keysToGet[2]);

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the projectid");
		}
		if (tag == null || tag.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass in the tag");
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

		Git thisGit = null;
		try {

			thisGit = Git.open(new File(projectVersionFolder));

			ObjectId commitObjectId = thisGit.getRepository().resolve(commitId);

			thisGit.tag().setName(tag).setObjectId(thisGit.getRepository().parseCommit(commitObjectId)).call();

		} catch (RefAlreadyExistsException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Tag is already present " + tag, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("CommitId not found " + commitId, e);
		} finally {
			if (thisGit != null) {
				thisGit.close();
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor add tag to a particular commit id";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "This is a required field containing the project id of a project";
		} else if (key.equals(COMMIT_ID_KEY)) {
			return "This is a required field containing the commit id of a project";
		} else if (key.equals(ReactorKeysEnum.TAGS.getKey())) {
			return "This is a required field containing the tag of a project";
		}
		return super.getDescriptionForKey(key);
	}

}