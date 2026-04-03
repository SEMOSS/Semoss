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

public class JiraCreateTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraCreateTicketReactor.class);

	private static final String PROJECT = "project";
	private static final String SUMMARY = "summary";
	private static final String DESCRIPTION = "description";
	private static final String ISSUETYPE = "issuetype";
	private static final String ASSIGNEE = "assignee";
	private static final String PRIORITY = "priority";
	private static final String DUEDATE = "duedate";
	private static final String PARENT = "parent";
	private static final String STATUS = "status";

	public JiraCreateTicketReactor() {
		this.keysToGet = new String[] { PROJECT, SUMMARY, DESCRIPTION, ISSUETYPE, ASSIGNEE, PRIORITY, DUEDATE, PARENT, STATUS };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String projectKey = this.keyValue.get(PROJECT);
			String summary = this.keyValue.get(SUMMARY);
			String description = this.keyValue.get(DESCRIPTION);
			String issueType = this.keyValue.get(ISSUETYPE);
			String assigneeAccountId = this.keyValue.get(ASSIGNEE);
			String priority = this.keyValue.get(PRIORITY);
			String dueDate = this.keyValue.get(DUEDATE);
			String parentKey = this.keyValue.get(PARENT);
			String status = this.keyValue.get(STATUS);
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.createIssue(accessToken, baseUrl, nullSafe(projectKey), nullSafe(summary),
					nullSafe(description), nullSafe(issueType), nullSafe(assigneeAccountId),
					nullSafe(priority), nullSafe(dueDate), nullSafe(parentKey), nullSafe(status));
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while creating a Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create a Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while creating the Jira ticket. Error message: " + e.getMessage());
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
		return "Creates a new Jira issue in a Jira project. Use this when the agent needs a new ticket; do not use JiraUpdateTicketReactor because that reactor only changes an existing issue. Returns a map containing id, key, self, summary, success, and sometimes status if a post-create transition was applied. Preconditions: the current SEMOSS user must already have Jira credentials. In most flows, call JiraGetProjectsReactor first to get the project key, JiraIssueTypeReactor to get a valid issue type, JiraGetAssignableUsersReactor to get an assignee accountId, and JiraGetPriorityReactor to get allowed priority names.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required. Jira project key in uppercase, for example RTJ. Get this from JiraGetProjectsReactor if unknown. Do not pass the project name. If the key is wrong or missing, the issue cannot be created.";
		} else if (key.equals(SUMMARY)) {
			return "Required. Short Jira issue summary or title as plain text. This becomes the ticket headline shown in Jira. If this value is missing or blank, Jira rejects the create request.";
		} else if (key.equals(DESCRIPTION)) {
			return "Optional. Detailed issue description as plain text. If omitted, blank, or the literal string 'null', no description is sent. This reactor converts the text to Atlassian document format internally. If Jira rejects the content, the create request fails.";
		} else if (key.equals(ISSUETYPE)) {
			return "Required. Exact Jira issue type name, for example Bug, Task, Story, Epic, or Subtask. Get the valid value from JiraIssueTypeReactor, ideally scoped to the same project. If the issue type is missing, misspelled, or not allowed for the project, Jira rejects the create request.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. Jira assignee accountId string, not display name and not email address. Get this from JiraGetAssignableUsersReactor for the same project. If omitted, Jira uses its default assignee behavior. If the accountId is wrong or the user is not assignable, the create request fails.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. Exact Jira priority name, for example Highest, High, Medium, or Low. Get this from JiraGetPriorityReactor if unknown. If omitted, Jira uses the default priority. If the name does not exist in Jira, the create request fails.";
		} else if (key.equals(DUEDATE)) {
			return "Optional. Due date in ISO date format YYYY-MM-DD, for example 2026-04-30. If omitted, blank, or 'null', no due date is sent. If the format is wrong, Jira validation fails.";
		} else if (key.equals(PARENT)) {
			return "Optional. Parent issue key in KEY-NUMBER format, for example RTJ-45. Get this from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. This is typically required when creating a Subtask. If the parent issue key is wrong or incompatible with the issue type, Jira rejects the create request.";
		} else if (key.equals(STATUS)) {
			return "Optional. Exact Jira transition or target status name to apply after the issue is created. Use this only when the new issue must move immediately after creation. If omitted, the issue stays in Jira's default initial status. If the supplied transition is not valid for the new issue's workflow, the transition step fails.";
		}
		return super.getDescriptionForKey(key);
	}
}