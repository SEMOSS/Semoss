package prerna.io.connector.jira;

import java.util.regex.Pattern;

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

	private static final Pattern ISSUE_KEY_PATTERN = Pattern.compile("^[A-Z][A-Z0-9_]+-\\d+$");
	private static final Pattern PROJECT_KEY_PATTERN = Pattern.compile("^[A-Z][A-Z0-9_]+$");
	private static final Pattern DATE_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

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
	 * Normalizes a user-supplied string: trims whitespace and converts empty,
	 * blank, or literal {@code "null"} values to {@code null}.
	 *
	 * @param value raw user input
	 * @return trimmed non-empty string or {@code null}
	 */
	public static String nullSafe(String value) {
		if (value == null || value.trim().isEmpty() || value.trim().equalsIgnoreCase("null")) {
			return null;
		}
		return value.trim();
	}

	/**
	 * Validates that a Jira issue key matches the expected {@code KEY-123} format.
	 *
	 * @param issueKey value to validate
	 * @param paramName parameter name for error messages
	 */
	public static void validateIssueKey(String issueKey, String paramName) {
		if (issueKey == null || !ISSUE_KEY_PATTERN.matcher(issueKey).matches()) {
			throw new SemossPixelException(paramName + " must be a valid Jira issue key in KEY-NUMBER format"
					+ " (for example RTJ-123). Received: " + issueKey);
		}
	}

	/**
	 * Validates that a Jira project key matches the expected uppercase format.
	 *
	 * @param projectKey value to validate
	 * @param paramName parameter name for error messages
	 */
	public static void validateProjectKey(String projectKey, String paramName) {
		if (projectKey == null || !PROJECT_KEY_PATTERN.matcher(projectKey).matches()) {
			throw new SemossPixelException(paramName + " must be a valid Jira project key in uppercase"
					+ " (for example RTJ). Received: " + projectKey);
		}
	}

	/**
	 * Validates that a date string matches {@code YYYY-MM-DD} format.
	 *
	 * @param date value to validate
	 * @param paramName parameter name for error messages
	 */
	public static void validateDateFormat(String date, String paramName) {
		if (date != null && !DATE_PATTERN.matcher(date).matches()) {
			throw new SemossPixelException(paramName + " must be in YYYY-MM-DD format"
					+ " (for example 2026-04-30). Received: " + date);
		}
	}

	/**
	 * Validates and clamps maxResults to a safe range for the Jira API.
	 *
	 * @param maxResults raw value from the user
	 * @return clamped value between 1 and 100
	 */
	public static int clampMaxResults(int maxResults) {
		if (maxResults < 1) {
			return 50;
		}
		return Math.min(maxResults, 100);
	}

	/**
	 * Escapes a string value for safe inclusion in a JQL quoted string.
	 * Handles backslashes, double quotes, and single quotes.
	 *
	 * @param value raw string to embed in JQL
	 * @return escaped string safe for use inside JQL double-quoted literals
	 */
	public static String escapeJqlString(String value) {
		if (value == null) {
			return "";
		}
		return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("'", "\\'");
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
