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

public class JiraUpdateTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraUpdateTicketReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String SUMMARY = "summary";
	private static final String DESCRIPTION = "description";
	private static final String ASSIGNEE = "assignee";
	private static final String PRIORITY = "priority";
	private static final String DUEDATE = "duedate";
	private static final String STATUS = "status";

	public JiraUpdateTicketReactor() {
		this.keysToGet = new String[] { JIRAID, SUMMARY, DESCRIPTION, ASSIGNEE, PRIORITY, DUEDATE, STATUS };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String jiraId = this.keyValue.get(JIRAID);
			String summary = this.keyValue.get(SUMMARY);
			String description = this.keyValue.get(DESCRIPTION);
			String assignee = this.keyValue.get(ASSIGNEE);
			String priority = this.keyValue.get(PRIORITY);
			String dueDate = this.keyValue.get(DUEDATE);
			String status = this.keyValue.get(STATUS);
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.updateIssue(accessToken, baseUrl, jiraId, nullSafe(summary),
					nullSafe(description), nullSafe(assignee), nullSafe(priority),
					nullSafe(dueDate), nullSafe(status));
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while updating a Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update a Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while updating the Jira ticket. Error message: " + e.getMessage());
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
		return "Updates fields on an existing Jira issue. Use for summary, description, assignee, priority, due date, or status changes; use JiraAddCommentReactor for comments. Only supplied fields are changed. Returns key, success, and status if a transition was applied. Requires Jira auth and a valid issue key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Not a project key or numeric id. Fails if missing or invalid.";
		} else if (key.equals(SUMMARY)) {
			return "Optional. New Jira issue summary as plain text. Omit, blank, or 'null' to leave it unchanged. Fails if Jira rejects the value.";
		} else if (key.equals(DESCRIPTION)) {
			return "Optional. New Jira issue description as plain text. Omit, blank, or 'null' to leave it unchanged. Fails if Jira rejects the value.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. Jira assignee accountId, not display name or email. Get it from JiraGetAssignableUsersReactor or JiraReadTicketReactor. Omit, blank, or 'null' to leave it unchanged. Fails if invalid.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. Exact Jira priority name, for example Highest, High, Medium, or Low. Get it from JiraGetPriorityReactor. Omit, blank, or 'null' to leave it unchanged. Fails if invalid.";
		} else if (key.equals(DUEDATE)) {
			return "Optional. New due date in YYYY-MM-DD format, for example 2026-04-30. Omit, blank, or 'null' to leave it unchanged. Fails if the format is invalid.";
		} else if (key.equals(STATUS)) {
			return "Optional. Jira transition or target status name for this specific issue. Get a valid value from JiraGetStatusReactor for the same jiraid. Omit, blank, or 'null' to leave status unchanged. Fails if the transition is invalid.";
		}
		return super.getDescriptionForKey(key);
	}
}