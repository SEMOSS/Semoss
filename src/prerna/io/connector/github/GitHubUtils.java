package prerna.io.connector.github;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;

public final class GitHubUtils {

	private static final Logger classLogger = LogManager.getLogger(GitHubUtils.class);

	private GitHubUtils() {
	}

	/**
	 * Gets the GitHub access token for the authenticated user.
	 *
	 * @param user current user session
	 * @return GitHub OAuth access token
	 */
	public static String getGitHubToken(User user) {
		try {
			if (user == null) {
				throwLoginRequiredError();
			}

			AccessToken gitHubToken = user.getAccessToken(AuthProvider.GITHUB);
			if (gitHubToken == null) {
				throwLoginRequiredError();
			}

			return gitHubToken.getAccess_token();
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to resolve GitHub access token", e);
			throwLoginRequiredError();
			return null;
		}
	}

	private static void throwLoginRequiredError() {
		Map<String, Object> details = new HashMap<String, Object>();
		details.put("type", "git");
		details.put("message", "Please login to your GitHub account");
		AbstractReactor.throwLoginError(details);
	}
}