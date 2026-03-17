package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

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
			return JiraHelper.updateIssue(user, jiraId, nullSafe(summary), nullSafe(description), nullSafe(assignee),
					nullSafe(priority), nullSafe(dueDate), nullSafe(status));
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
		return "This reactor updates an existing Jira issue. Only provided fields are updated — omitted fields are left unchanged. To change status use JiraTransitionTicketReactor instead.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "The Jira issue key to update (e.g. PROJECT-123).";
		} else if (key.equals(SUMMARY)) {
			return "Optional. New issue title/summary.";
		} else if (key.equals(DESCRIPTION)) {
			return "Optional. New issue description body.";
		} else if (key.equals(ASSIGNEE)) {
			return "Optional. The accountId of the user to assign to. Use JiraGetAssignableUsersReactor to get accountIds.";
		} else if (key.equals(PRIORITY)) {
			return "Optional. New priority name (e.g. High, Medium, Low).";
		} else if (key.equals(DUEDATE)) {
			return "Optional. New due date in YYYY-MM-DD format.";
		} else if (key.equals(STATUS)) {
			return "Optional. The status to transition the issue to.";
		}
		return super.getDescriptionForKey(key);
	}
}
