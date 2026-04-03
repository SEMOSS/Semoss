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
		return "Lists issues in a single Jira project with simple built-in filters. Use this when the agent already knows the project key and only needs project-scoped browsing; use JiraSearchReactor instead for arbitrary JQL, cross-project search, or advanced conditions. Returns a paginated map containing issues, isLast, maxResults, and optionally nextPageToken. Each issue entry includes key, summary, status, assignee, priority, issuetype, duedate, and labels. Preconditions: the current SEMOSS user must already have Jira credentials and project must be a valid Jira project key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required. Jira project key in uppercase, for example RTJ. Get this from JiraGetProjectsReactor if unknown. This reactor only searches inside this single project. If the key is wrong or missing, the request fails or returns no issues.";
		} else if (key.equals(STATUS)) {
			return "Optional. Exact Jira status name used to filter the project issue list, for example 'To Do', 'In Progress', or 'Done'. Use Jira status values from Jira itself. If omitted, no status filter is applied. If the value does not match Jira's status names, the result can be empty.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. Jira assignee accountId string used to filter results. Get this from JiraGetAssignableUsersReactor or from assigneeAccountId returned by JiraReadTicketReactor. Do not pass a display name. If omitted, no assignee filter is applied. If the accountId is wrong, the result can be empty.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. Exact Jira priority name used to filter results, for example Highest, High, Medium, or Low. Get valid values from JiraGetPriorityReactor. If omitted, no priority filter is applied. If the name is wrong, the result can be empty.";
		} else if (key.equals(NEXT_PAGE_TOKEN)) {
			return "Optional. Opaque pagination token returned by a previous JiraGetTicketsReactor response. Pass it back unchanged to fetch the next page. Do not invent, parse, or modify it. If omitted, the first page is returned. If the token is stale or invalid, Jira can reject the request.";
		} else if (key.equals(MAX_RESULTS)) {
			return "Optional. Maximum number of issues to return per page, provided as an integer string such as '25' or '100'. Default is 50 when omitted. If this value is non-numeric, the reactor fails before calling Jira.";
		}
		return super.getDescriptionForKey(key);
	}
}