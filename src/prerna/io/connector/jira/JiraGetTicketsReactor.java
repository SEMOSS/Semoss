package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

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
			if (this.keyValue.get(MAX_RESULTS) != null) {
				maxResults = Integer.parseInt(this.keyValue.get(MAX_RESULTS));
			}

			User user = this.insight.getUser();
			return JiraHelper.listIssues(user, projectKey, nullSafe(statusFilter), nullSafe(assigneeFilter),
					nullSafe(priorityFilter), nullSafe(nextPageToken), maxResults);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
		return "This reactor lists Jira issues for a project with optional filters and pagination. Returns a paginated list with total count.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "The Jira project key to list issues for (e.g. MYPROJECT).";
		} else if (key.equals(STATUS)) {
			return "Optional. Filter issues by status (e.g. 'In Progress', 'Done', 'To Do').";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. Filter issues by assignee accountId.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. Filter issues by priority (e.g. High, Medium, Low).";
		} else if (key.equals(NEXT_PAGE_TOKEN)) {
			return "Optional. Token from a previous response to fetch the next page of results.";
		} else if (key.equals(MAX_RESULTS)) {
			return "Optional. Maximum number of results to return per page. Default is 50.";
		}
		return super.getDescriptionForKey(key);
	}
}
