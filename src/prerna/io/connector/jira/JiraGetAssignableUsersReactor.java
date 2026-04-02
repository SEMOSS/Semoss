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
import prerna.util.Constants;

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
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
		return "This reactor retrieves users assignable to a Jira project. Returns accountId, displayName, and emailAddress for each user. Use accountId when assigning issues.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "The Jira project key to fetch assignable users for (e.g. MYPROJECT).";
		} else if (key.equals(QUERY)) {
			return "Optional. Search query to filter users by name or email.";
		}
		return super.getDescriptionForKey(key);
	}
}