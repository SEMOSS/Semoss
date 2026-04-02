package prerna.io.connector.jira;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class JiraUtils {

	private static final Logger classLogger = LogManager.getLogger(JiraUtils.class);

	private static final String JIRA_API_BASE = "https://api.atlassian.com/ex/jira/";

	/**
	 * Retrieves both the Jira access token and base URL for the current user in a
	 * single token lookup.
	 *
	 * @param user current session user
	 * @return a {@link Pair} containing the access token and base URL
	 */
	public static Pair<String, String> getJiraCredentials(User user) {
		AccessToken jiraToken = getJiraToken(user);
		String accessToken = jiraToken.getAccess_token();
		String cloudId = jiraToken.getId();
		if (cloudId == null || cloudId.trim().isEmpty()) {
			classLogger.error("Jira Cloud ID not found on token.");
			throw new SemossPixelException("Jira Cloud ID not found on token. Please reconnect your Jira account.");
		}
		return new Pair<>(accessToken, JIRA_API_BASE + cloudId);
	}

	/**
	 * Gets the Jira token object for a user with consistent validation.
	 *
	 * @param user current session user
	 * @return Jira {@link AccessToken}
	 */
	private static AccessToken getJiraToken(User user) {
		if (user == null) {
			classLogger.error("User session is null.");
			throw new SemossPixelException("User session is null. Please log in with Jira.");
		}
		AccessToken jiraToken = user.getAccessToken(AuthProvider.JIRA);
        if (jiraToken == null) {
            classLogger.error("No Jira token found for user.");
            throw new SemossPixelException("No Jira token found for user. Please connect your Jira account.");
        }
        return jiraToken;
	}
}
