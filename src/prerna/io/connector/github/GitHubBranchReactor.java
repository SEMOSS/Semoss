package prerna.io.connector.github;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GitHubBranchReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubBranchReactor.class);

	private static final String ACTION = "action";
	private static final String OWNER = "owner";
	private static final String REPO = "repo";
	private static final String BRANCH = "branch";
	private static final String FROM_BRANCH = "fromBranch";
	private static final String PAGE = "page";
	private static final String PER_PAGE = "perPage";

	public GitHubBranchReactor() {
		this.keysToGet = new String[] { ACTION, OWNER, REPO, BRANCH, FROM_BRANCH, PAGE, PER_PAGE };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String accessToken = GitHubUtils.getGitHubToken(this.insight.getUser());
			String action = this.keyValue.get(ACTION);
			String owner = this.keyValue.get(OWNER);
			String repo = this.keyValue.get(REPO);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException(ACTION + " is required.");
			}
			if (owner == null || owner.trim().isEmpty()) {
				throw new SemossPixelException(OWNER + " is required.");
			}
			if (repo == null || repo.trim().isEmpty()) {
				throw new SemossPixelException(REPO + " is required.");
			}
			action = action.trim().toLowerCase();
			owner = owner.trim();
			repo = repo.trim();

			Object result;
			switch (action) {
			case "list":
				int page = 1;
				String pageValue = this.keyValue.get(PAGE);
				if (pageValue != null && !pageValue.trim().isEmpty()) {
					try {
						page = Integer.parseInt(pageValue.trim());
					} catch (NumberFormatException e) {
						throw new SemossPixelException("Invalid number for pagination: '" + pageValue.trim() + "'.", e);
					}
				}
				int perPage = 30;
				String perPageValue = this.keyValue.get(PER_PAGE);
				if (perPageValue != null && !perPageValue.trim().isEmpty()) {
					try {
						perPage = Integer.parseInt(perPageValue.trim());
					} catch (NumberFormatException e) {
						throw new SemossPixelException("Invalid number for pagination: '" + perPageValue.trim() + "'.",
								e);
					}
				}
				result = GitHubHelper.listBranches(accessToken, owner, repo, page, perPage);
				break;
			case "create":
				String branch = this.keyValue.get(BRANCH);
				if (branch == null || branch.trim().isEmpty()) {
					throw new SemossPixelException(BRANCH + " is required.");
				}
				String fromBranch = this.keyValue.get(FROM_BRANCH);
				if (fromBranch != null) {
					fromBranch = fromBranch.trim();
					if (fromBranch.isEmpty()) {
						fromBranch = null;
					}
				}
				result = GitHubHelper.createBranch(accessToken, owner, repo, branch.trim(), fromBranch);
				break;
			case "delete":
				branch = this.keyValue.get(BRANCH);
				if (branch == null || branch.trim().isEmpty()) {
					throw new SemossPixelException(BRANCH + " is required.");
				}
				result = GitHubHelper.deleteBranch(accessToken, owner, repo, branch.trim());
				break;
			default:
				throw new SemossPixelException(
						"Invalid action '" + action + "'. Valid values are: list, create, delete.");
			}

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error in GitHubBranchReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to execute GitHubBranchReactor", e);
			throw new SemossPixelException("An error occurred in GitHubBranchReactor. Error message: " + e.getMessage(),
					e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Manages branches in a GitHub repository by listing, creating, or deleting branches.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ACTION.equals(key)) {
			return "Required action: list, create, or delete.";
		} else if (OWNER.equals(key)) {
			return "Required repository owner, either a user or organization.";
		} else if (REPO.equals(key)) {
			return "Required repository name.";
		} else if (BRANCH.equals(key)) {
			return "Branch name. Required for create and delete.";
		} else if (FROM_BRANCH.equals(key)) {
			return "Optional source branch to branch from. Defaults to the repository's default branch.";
		} else if (PAGE.equals(key)) {
			return "Page number for pagination. Used by list.";
		} else if (PER_PAGE.equals(key)) {
			return "Results per page. Used by list.";
		}
		return super.getDescriptionForKey(key);
	}
}