package prerna.io.connector.jira;

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

public class JiraGetTicketsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetTicketsReactor.class);

	private static final String PROJECT = "project";
	private static final String STATUS = "status";
	private static final String ASSIGNEE = "assignee";
	private static final String PRIORITY = "priority";
	private static final String NEXT_PAGE_TOKEN = "nextPageToken";
	private static final String MAX_RESULTS = "maxResults";

	public JiraGetTicketsReactor() {
		this.keysToGet = new String[] { PROJECT, STATUS, ASSIGNEE, PRIORITY, NEXT_PAGE_TOKEN, MAX_RESULTS };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String projectKey = this.keyValue.get(PROJECT);
			String statusFilter = this.keyValue.get(STATUS);
			String assigneeFilter = this.keyValue.get(ASSIGNEE);
			String priorityFilter = this.keyValue.get(PRIORITY);
			String nextPageToken = this.keyValue.get(NEXT_PAGE_TOKEN);

			int maxResults = 50;
			String maxResultsRaw = this.keyValue.get(MAX_RESULTS);
			if (maxResultsRaw != null && !maxResultsRaw.trim().isEmpty()) {
				try {
					maxResults = Integer.parseInt(maxResultsRaw.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException("Invalid value for maxResults. Must be an integer.");
				}
			}

			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.listIssues(accessToken, baseUrl, projectKey,
					nullSafe(statusFilter), nullSafe(assigneeFilter),
					nullSafe(priorityFilter), nullSafe(nextPageToken), maxResults);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while retrieving Jira tickets", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve Jira tickets", e);
			throw new SemossPixelException(
					"An error occurred while retrieving Jira tickets. Error message: " + e.getMessage());
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
		return "Lists issues in one Jira project with simple filters and pagination. Use when you know the project key; use JiraSearchReactor for JQL or cross-project search. Returns issues, isLast, maxResults, and nextPageToken when more pages exist. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required. Jira project key in uppercase, for example RTJ. Get it from JiraGetProjectsReactor. This reactor only searches this project. Fails if missing or invalid.";
		} else if (key.equals(STATUS)) {
			return "Optional. Exact Jira status name, for example 'To Do', 'In Progress', or 'Done'. Omit for no status filter. Returns no matches if the value does not exist in Jira.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. Jira assignee accountId. Get it from JiraGetAssignableUsersReactor or JiraReadTicketReactor. Do not pass a display name. Omit for no assignee filter.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. Exact Jira priority name, for example Highest, High, Medium, or Low. Get it from JiraGetPriorityReactor. Omit for no priority filter.";
		} else if (key.equals(NEXT_PAGE_TOKEN)) {
			return "Optional. Opaque pagination token from a previous JiraGetTicketsReactor response. Pass it back unchanged for the next page. Omit for the first page. Invalid tokens can fail.";
		} else if (key.equals(MAX_RESULTS)) {
			return "Optional. Max issues per page as an integer string, for example '25' or '100'. Default is 50. Non-numeric values fail before the Jira call.";
		}
		return super.getDescriptionForKey(key);
	}
}