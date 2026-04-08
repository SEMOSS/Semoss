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

public class JiraAssignTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraAssignTicketReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String ASSIGNEE = "assignee";

	public JiraAssignTicketReactor() {
		this.keysToGet = new String[] { JIRAID, ASSIGNEE };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = JiraUtils.nullSafe(this.keyValue.get(JIRAID));
			String assignee = JiraUtils.nullSafe(this.keyValue.get(ASSIGNEE));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.assignIssue(accessToken, baseUrl, issueKey, assignee);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while assigning a Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to assign a Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while assigning the Jira ticket. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Assigns or unassigns a Jira issue. Use for quick reassignment without touching other fields; use JiraUpdateTicketReactor when changing multiple fields at once. Returns key, assignee, and success. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Fails if missing or invalid.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. Jira assignee accountId, not display name or email. Get it from JiraGetAssignableUsersReactor. Omit or pass blank to unassign the issue.";
		}
		return super.getDescriptionForKey(key);
	}
}
