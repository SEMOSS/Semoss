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

public class JiraDeleteWorklogReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraDeleteWorklogReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String WORKLOG_ID = "worklogId";

	public JiraDeleteWorklogReactor() {
		this.keysToGet = new String[] { JIRAID, WORKLOG_ID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = JiraUtils.nullSafe(this.keyValue.get(JIRAID));
			String worklogId = JiraUtils.nullSafe(this.keyValue.get(WORKLOG_ID));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.deleteWorklog(accessToken, baseUrl, issueKey, worklogId);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting Jira worklog", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete Jira worklog", e);
			throw new SemossPixelException(
					"An error occurred while deleting the Jira worklog. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Permanently deletes a worklog entry from a Jira issue. Use JiraGetWorklogsReactor first to get the worklog ID. Returns success. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Fails if missing or invalid.";
		} else if (key.equals(WORKLOG_ID)) {
			return "Required. The numeric worklog ID to delete. Get it from JiraGetWorklogsReactor. Fails if missing or if the worklog does not exist.";
		}
		return super.getDescriptionForKey(key);
	}
}
