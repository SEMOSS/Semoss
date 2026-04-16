package prerna.io.connector.github;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GitHubRepoReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubRepoReactor.class);

	private static final String ACTION = "action";
	private static final String OWNER = "owner";
	private static final String QUERY = "query";
	private static final String PAGE = "page";
	private static final String PER_PAGE = "perPage";

	public GitHubRepoReactor() {
		this.keysToGet = new String[] { ACTION, OWNER, QUERY, PAGE, PER_PAGE };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String accessToken = GitHubUtils.getGitHubToken(this.insight.getUser());
			String action = this.keyValue.get(ACTION);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException(ACTION + " is required.");
			}
			action = action.trim().toLowerCase();

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
					throw new SemossPixelException("Invalid number for pagination: '" + perPageValue.trim() + "'.", e);
				}
			}

			Object result;
			switch (action) {
			case "list_repos": {
				String owner = this.keyValue.get(OWNER);
				if (owner != null) {
					owner = owner.trim();
					if (owner.isEmpty()) {
						owner = null;
					}
				}
				result = GitHubHelper.listRepositories(accessToken, owner, page, perPage);
				break;
			}
			case "search_repos": {
				String query = this.keyValue.get(QUERY);
				if (query == null || query.trim().isEmpty()) {
					throw new SemossPixelException(QUERY + " is required.");
				}
				result = GitHubHelper.searchRepositories(accessToken, query.trim(), page, perPage);
				break;
			}
			default:
				throw new SemossPixelException(
						"Invalid action '" + action + "'. Valid values are: list_repos, search_repos.");
			}

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error in GitHubRepoReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to execute GitHubRepoReactor", e);
			throw new SemossPixelException("An error occurred in GitHubRepoReactor. Error message: " + e.getMessage(),
					e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Lists or searches GitHub repositories.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ACTION.equals(key)) {
			return "Required action: list_repos or search_repos.";
		} else if (OWNER.equals(key)) {
			return "GitHub username or organization. Optional for list_repos.";
		} else if (QUERY.equals(key)) {
			return "Search query string. Required for search_repos.";
		} else if (PAGE.equals(key)) {
			return "Page number for pagination. Defaults to 1.";
		} else if (PER_PAGE.equals(key)) {
			return "Results per page. Defaults to 30 and is capped at 100.";
		}
		return super.getDescriptionForKey(key);
	}
}