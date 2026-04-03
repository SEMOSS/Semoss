package prerna.io.connector.jira;

import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraGetAssignableUsersReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetAssignableUsersReactor.class);

	private static final String PROJECT = "project";
	private static final String QUERY = "query";

	public JiraGetAssignableUsersReactor() {
		this.keysToGet = new String[] { PROJECT, QUERY };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String projectKey = this.keyValue.get(PROJECT);
			String query = this.keyValue.get(QUERY);
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			List<Map<String, Object>> result = JiraHelper.getAssignableUsers(accessToken, baseUrl, projectKey, nullSafe(query));
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while retrieving Jira assignable users", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve Jira assignable users", e);
			throw new SemossPixelException(
					"An error occurred while retrieving assignable users. Error message: " + e.getMessage());
		}
	}

	private static String nullSafe(String value) {
		if (value == null || value.trim().isEmpty() || value.trim().equalsIgnoreCase("null")) {
			return null;
		}
		return value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Lists users assignable in one Jira project. Use before JiraCreateTicketReactor or JiraUpdateTicketReactor when you need a valid assignee accountId. Returns accountId, displayName, and emailAddress. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required. Jira project key in uppercase, for example RTJ. Get it from JiraGetProjectsReactor. Results are scoped to this project. Fails if missing or invalid.";
		} else if (key.equals(QUERY)) {
			return "Optional. Free-text filter for name or email, for example 'jane' or 'jane@company.com'. Omit for the full project list. Returns an empty list if nothing matches.";
		}
		return super.getDescriptionForKey(key);
	}
}