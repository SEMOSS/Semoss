package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class JiraDeleteTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraDeleteTicketReactor.class);

	private static final String PROJECT = "project";
	private static final String JIRAID = "jiraid";
	private static final String DELETE_SUBTASKS   = "deleteSubtasks";

	public JiraDeleteTicketReactor() {
		this.keysToGet = new String[] { PROJECT, JIRAID, DELETE_SUBTASKS };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String projectName = this.keyValue.get(PROJECT);
			String jiraId = this.keyValue.get(JIRAID);
			String deleteSubtasksRaw = nullSafe(this.keyValue.get(DELETE_SUBTASKS));
			boolean deleteSubtasks = "true".equalsIgnoreCase(deleteSubtasksRaw);
			User user = this.insight.getUser();
			return JiraHelper.deleteIssue(user, projectName, jiraId, deleteSubtasks);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while deleting ticket. Error message: " + e.getMessage());
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
		return "This reactor is used to delete a Jira ticket/issue by its ID and project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "The Jira project key the issue belongs to (e.g. MYPROJECT).";
		} else if (key.equals(JIRAID)) {
			return "The Jira ID of the ticket/issue to be deleted.";
		} else if (key.equals(DELETE_SUBTASKS)) {
			return "Optional. Whether to delete subtasks of the issue. Accepts 'true' or 'false'.";
		}
		return super.getDescriptionForKey(key);
	}
}
