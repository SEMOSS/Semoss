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
			String jqlQuery = JiraUtils.nullSafe(this.keyValue.get(JQL));
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
			Map<String, Object> result = JiraHelper.searchIssues(accessToken, baseUrl, jqlQuery, JiraUtils.nullSafe(nextPageToken), maxResults);
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

	@Override
	public String getReactorDescription() {
		return "Runs a Jira JQL search with pagination. Use for cross-project or advanced filtering; use JiraGetTicketsReactor for simple project lists. Returns issues, isLast, maxResults, and nextPageToken when more pages exist. Requires Jira auth and valid JQL.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JQL)) {
			return "Required. Full Jira Query Language string, for example 'project = RTJ AND status = \"In Progress\"'. Use JiraSearchReactor only when simple project filters are not enough. Jira rejects missing or invalid JQL.";
		} else if (key.equals(NEXT_PAGE_TOKEN)) {
			return "Optional. Opaque pagination token from a previous JiraSearchReactor response. Pass it back unchanged for the next page. Omit for the first page. Invalid tokens can fail.";
		} else if (key.equals(MAX_RESULTS)) {
			return "Optional. Max issues per page as an integer string, for example '25' or '100'. Default is 50. Non-numeric values fail before the Jira call.";
		}
		return super.getDescriptionForKey(key);
	}
}