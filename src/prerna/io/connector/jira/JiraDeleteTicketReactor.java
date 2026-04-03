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
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.deleteIssue(accessToken, baseUrl, nullSafe(projectName), nullSafe(jiraId), deleteSubtasks);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting a Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete a Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while deleting the Jira ticket. Error message: " + e.getMessage());
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
		return "Permanently deletes a Jira issue. Use only for removal, not for closing or transitioning a ticket. Returns success. Requires Jira auth, a valid issue key, and the owning project key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required. Jira project key in uppercase, for example RTJ. Get it from JiraGetProjectsReactor. It must own the issue being deleted. Fails if missing or mismatched.";
		} else if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Not a numeric id. Fails if missing, invalid, or not in the given project.";
		} else if (key.equals(DELETE_SUBTASKS)) {
			return "Optional. 'true' or 'false' string for deleting subtasks with the parent issue. Defaults to false. If false and subtasks exist, Jira may reject the delete.";
		}
		return super.getDescriptionForKey(key);
	}
}