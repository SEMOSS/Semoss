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
		return "Run an arbitrary JQL query against Jira with pagination. Use for cross-project searches, date-range queries, or any search that goes beyond a single project filter. Returns paginated issue list with total count.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JQL)) {
			return "The JQL query string to run (e.g. 'assignee = currentUser() AND status = \"In Progress\"').";
		} else if (key.equals(NEXT_PAGE_TOKEN)) {
			return "Optional. Token from a previous response to fetch the next page of results.";
		} else if (key.equals(MAX_RESULTS)) {
			return "Optional. Maximum number of results to return per page. Default is 50.";
		}
		return super.getDescriptionForKey(key);
	}
}