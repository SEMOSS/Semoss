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

public class JiraSearchReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraSearchReactor.class);

	private static final String JQL = "jql";
	private static final String NEXT_PAGE_TOKEN = "nextPageToken";
	private static final String MAX_RESULTS = "maxResults";

	public JiraSearchReactor() {
		this.keysToGet = new String[] { JQL, NEXT_PAGE_TOKEN, MAX_RESULTS };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String jqlQuery = this.keyValue.get(JQL);
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
			Map<String, Object> result = JiraHelper.searchIssues(accessToken, baseUrl, jqlQuery, nullSafe(nextPageToken), maxResults);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while searching Jira issues", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to search Jira issues", e);
			throw new SemossPixelException(
					"An error occurred while searching Jira issues. Error message: " + e.getMessage());
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
		return "Runs an arbitrary Jira JQL search. Use this instead of JiraGetTicketsReactor when the query spans multiple projects, needs date logic, text search, OR or AND conditions, or any filter beyond simple project, status, assignee, and priority matching. Returns a paginated map containing issues, isLast, maxResults, and optionally nextPageToken. Each issue entry includes key, summary, status, assignee, priority, issuetype, duedate, and labels. Preconditions: the current SEMOSS user must already have Jira credentials and jql must be a valid Jira Query Language string.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JQL)) {
			return "Required. Full Jira Query Language string, for example 'project = RTJ AND assignee = currentUser() AND status = \"In Progress\"'. Use Jira field names and Jira JQL syntax exactly. If you only need a simple project-scoped list, prefer JiraGetTicketsReactor instead. If this string is malformed or missing, Jira rejects the search.";
		} else if (key.equals(NEXT_PAGE_TOKEN)) {
			return "Optional. Opaque pagination token returned by a previous JiraSearchReactor response. Pass it back unchanged to fetch the next page. Do not invent, parse, or modify it. If omitted, the first page is returned. If the token is stale or invalid, Jira can reject the request.";
		} else if (key.equals(MAX_RESULTS)) {
			return "Optional. Maximum number of issues to return per page, provided as an integer string such as '25' or '100'. Default is 50 when omitted. If this value is non-numeric, the reactor fails before calling Jira.";
		}
		return super.getDescriptionForKey(key);
	}
}