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
		return "Deletes an existing Jira issue permanently. Use this only for permanent removal; this is not the same as transitioning an issue to Done or Closed. Returns a map with success. Preconditions: the current SEMOSS user must already have Jira credentials, jiraid must identify an existing issue, and project must match the project that owns that issue. This reactor validates the issue-project relationship before deletion.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required. Jira project key in uppercase, for example RTJ. Get this from JiraGetProjectsReactor if unknown. This must be the project that owns the issue identified by jiraid. If it is wrong or missing, the delete validation fails.";
		} else if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get this from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Do not pass a Jira numeric id. If the key is wrong, missing, or does not belong to the supplied project, the delete request fails.";
		} else if (key.equals(DELETE_SUBTASKS)) {
			return "Optional. Literal string 'true' or 'false' controlling whether Jira should also delete subtasks when deleting a parent issue. Default behavior is false when omitted. Any value other than case-insensitive 'true' is treated as false. If the issue has subtasks and this flag is false, Jira can reject the delete request.";
		}
		return super.getDescriptionForKey(key);
	}
}