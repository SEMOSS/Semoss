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
		return "Lists the users who can be assigned issues in a specific Jira project. Use this before JiraCreateTicketReactor or JiraUpdateTicketReactor when you need a valid assignee accountId; do not guess from display name or email. Returns a list of maps containing accountId, displayName, and emailAddress. Preconditions: the current SEMOSS user must already have Jira credentials and project must be a valid Jira project key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required. Jira project key in uppercase, for example RTJ. Get this from JiraGetProjectsReactor if unknown. The returned users are only assignable within this project. If the key is wrong or missing, the lookup fails or returns no valid users.";
		} else if (key.equals(QUERY)) {
			return "Optional. Free-text search string used to narrow users by name or email address, for example 'jane' or 'jane@company.com'. If omitted, the reactor returns the unfiltered assignable-user list for the project. If no users match, the result is an empty list.";
		}
		return super.getDescriptionForKey(key);
	}
}