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
			projectKey = JiraUtils.nullSafe(projectKey);
			summary = JiraUtils.nullSafe(summary);
			description = JiraUtils.nullSafe(description);
			issueType = JiraUtils.nullSafe(issueType);
			assigneeAccountId = JiraUtils.nullSafe(assigneeAccountId);
			priority = JiraUtils.nullSafe(priority);
			dueDate = JiraUtils.nullSafe(dueDate);
			parentKey = JiraUtils.nullSafe(parentKey);
			status = JiraUtils.nullSafe(status);
			JiraUtils.validateDateFormat(dueDate, "duedate");
			Map<String, Object> result = JiraHelper.createIssue(accessToken, baseUrl, projectKey, summary,
					description, issueType, assigneeAccountId,
					priority, dueDate, parentKey, status);
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

	@Override
	public String getReactorDescription() {
		return "Creates a new Jira issue. Use for new tickets; use JiraUpdateTicketReactor for existing ones. Returns id, key, self, summary, success, and status if a transition was applied. Requires Jira auth; usually get project, issue type, assignee, and priority from the lookup reactors first.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required. Jira project key in uppercase, for example RTJ. Get it from JiraGetProjectsReactor. Do not pass the project name. Fails if missing or invalid.";
		} else if (key.equals(SUMMARY)) {
			return "Required. Jira issue summary or title as plain text. Jira rejects a missing or blank value.";
		} else if (key.equals(DESCRIPTION)) {
			return "Optional. Jira issue description as plain text. Omit, blank, or 'null' to leave it unset. Fails if Jira rejects the content.";
		} else if (key.equals(ISSUETYPE)) {
			return "Required. Exact Jira issue type name, for example Bug, Task, Story, Epic, or Subtask. Get it from JiraIssueTypeReactor. Fails if missing, invalid, or not allowed for the project.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. Jira assignee accountId, not display name or email. Get it from JiraGetAssignableUsersReactor. If wrong or not assignable, the create request fails.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. Exact Jira priority name, for example Highest, High, Medium, or Low. Get it from JiraGetPriorityReactor. Fails if the value is not valid in Jira.";
		} else if (key.equals(DUEDATE)) {
			return "Optional. Due date in YYYY-MM-DD format, for example 2026-04-30. Omit, blank, or 'null' to leave it unset. Fails if the format is invalid.";
		} else if (key.equals(PARENT)) {
			return "Optional. Parent issue key in KEY-NUMBER format, for example RTJ-45. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Usually required for Subtask. Fails if invalid or incompatible.";
		} else if (key.equals(STATUS)) {
			return "Optional. Jira transition or target status name to apply after create. If omitted, Jira keeps the default initial status. Fails if the transition is not valid for the new issue.";
		}
		return super.getDescriptionForKey(key);
	}
}