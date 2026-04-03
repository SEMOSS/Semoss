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

public class JiraIssueTypeReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraIssueTypeReactor.class);

	private static final String PROJECT = "project";

	public JiraIssueTypeReactor() {
		this.keysToGet = new String[] { PROJECT };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String projectKey = this.keyValue.get(PROJECT);
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			List<Map<String, Object>> result = JiraHelper.getIssueTypes(accessToken, baseUrl, nullSafe(projectKey));
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while retrieving Jira issue types", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve Jira issue types", e);
			throw new SemossPixelException(
					"An error occurred while retrieving Jira issue types. Error message: " + e.getMessage());
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
		return "Lists Jira issue types. Use this before JiraCreateTicketReactor to get the exact issue type name Jira accepts. If project is provided, the list is limited to issue types available for that project; if project is omitted, the reactor returns the broader instance-wide type list. Returns a list of maps containing id, name, and subtask. Preconditions: the current SEMOSS user must already have Jira credentials.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Optional. Jira project key in uppercase, for example RTJ. Get this from JiraGetProjectsReactor if unknown. Supply this when you plan to create an issue in a specific project so you only see issue types valid for that project. If omitted, the reactor returns the broader instance-wide list. If the project key is wrong, the lookup fails.";
		}
		return super.getDescriptionForKey(key);
	}
}