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
import prerna.util.Constants;

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
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
		return "This reactor creates a Jira issue with full field support. Returns id, key, self link, and summary of the created issue.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "The Jira project key to create the issue in (e.g. MYPROJECT).";
		} else if (key.equals(SUMMARY)) {
			return "The issue title/summary.";
		} else if (key.equals(DESCRIPTION)) {
			return "Optional. The issue description body.";
		} else if (key.equals(ISSUETYPE)) {
			return "The issue type name (e.g. Bug, Task, Story, Epic, Subtask). Use JiraIssueTypeReactor to get available types.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. The accountId of the user to assign the issue to. Use JiraGetAssignableUsersReactor to get accountIds.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. The priority name (e.g. High, Medium, Low). Use JiraGetPrioritiesReactor to get available priorities.";
		} else if (key.equals(DUEDATE)) {
			return "Optional. The due date in YYYY-MM-DD format.";
		} else if (key.equals(PARENT)) {
			return "Optional. The parent issue key (e.g. PROJECT-10). Required when creating a Subtask.";
		} else if (key.equals(STATUS)) {
			return "Optional. The status to transition the issue to after creation.";
		}
		return super.getDescriptionForKey(key);
	}
}