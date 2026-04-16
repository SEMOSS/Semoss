package prerna.io.connector.github;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GitHubUserReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubUserReactor.class);
	private static final String USERNAME = "username";

	public GitHubUserReactor() {
		this.keysToGet = new String[] { USERNAME };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String accessToken = GitHubUtils.getGitHubToken(this.insight.getUser());
			String username = this.keyValue.get(USERNAME);
			if (username != null) {
				username = username.trim();
				if (username.isEmpty()) {
					username = null;
				}
			}

			Map<String, Object> result;
			if (username == null) {
				result = GitHubHelper.getAuthenticatedUser(accessToken);
			} else {
				result = GitHubHelper.getUserByUsername(accessToken, username);
			}

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error in GitHubUserReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to execute GitHubUserReactor", e);
			throw new SemossPixelException("An error occurred in GitHubUserReactor. Error message: " + e.getMessage(),
					e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Gets GitHub user details for the authenticated user or for a specific GitHub username.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (USERNAME.equals(key)) {
			return "Optional GitHub username to look up. If not provided, returns the authenticated user's profile.";
		}
		return super.getDescriptionForKey(key);
	}
}