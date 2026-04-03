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
		return "Updates fields on an existing Jira issue. Use this when the issue already exists and one or more fields must change; use JiraAddCommentReactor to add comments and JiraCreateTicketReactor to create new issues. Only nonblank optional fields are sent, so omitted values remain unchanged. Returns a map containing key, success, and sometimes status if a workflow transition was applied. Preconditions: the current SEMOSS user must already have Jira credentials, jiraid must identify an existing issue, and at least one optional field should be supplied. For status changes, call JiraGetStatusReactor first to get a valid transition for that specific issue.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get this from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Do not pass a project key or numeric id. If this value is wrong or missing, the update fails.";
		} else if (key.equals(SUMMARY)) {
			return "Optional. New Jira issue summary or title as plain text. If omitted, blank, or the literal string 'null', the summary is left unchanged. If the new value violates Jira validation rules, the update fails.";
		} else if (key.equals(DESCRIPTION)) {
			return "Optional. New Jira issue description as plain text. This reactor converts the text to Atlassian document format internally. If omitted, blank, or 'null', the description is left unchanged. If Jira rejects the new description, the update fails.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. Jira assignee accountId string, not display name and not email address. Get this from JiraGetAssignableUsersReactor for the same project or from assigneeAccountId returned by JiraReadTicketReactor. If omitted, blank, or 'null', assignee is left unchanged. If the accountId is wrong or not assignable, the update fails.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. Exact Jira priority name, for example Highest, High, Medium, or Low. Get valid values from JiraGetPriorityReactor. If omitted, blank, or 'null', priority is left unchanged. If the name is not valid in Jira, the update fails.";
		} else if (key.equals(DUEDATE)) {
			return "Optional. New due date in ISO date format YYYY-MM-DD, for example 2026-04-30. If omitted, blank, or 'null', due date is left unchanged. If the format is wrong, Jira validation fails.";
		} else if (key.equals(STATUS)) {
			return "Optional. Exact Jira transition or target status name for this specific issue. Call JiraGetStatusReactor first and use one of the returned transition names or target statuses for the same jiraid. If omitted, blank, or 'null', the issue status is left unchanged. If the supplied value is not valid for the issue's current workflow state, the transition fails.";
		}
		return super.getDescriptionForKey(key);
	}
}