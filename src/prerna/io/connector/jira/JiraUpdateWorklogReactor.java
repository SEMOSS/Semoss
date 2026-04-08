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

public class JiraUpdateWorklogReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraUpdateWorklogReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String WORKLOG_ID = "worklogId";
	private static final String TIME_SPENT = "timeSpent";
	private static final String COMMENT = "comment";
	private static final String STARTED = "started";

	public JiraUpdateWorklogReactor() {
		this.keysToGet = new String[] { JIRAID, WORKLOG_ID, TIME_SPENT, COMMENT, STARTED };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = JiraUtils.nullSafe(this.keyValue.get(JIRAID));
			String worklogId = JiraUtils.nullSafe(this.keyValue.get(WORKLOG_ID));
			String timeSpent = JiraUtils.nullSafe(this.keyValue.get(TIME_SPENT));
			String comment = JiraUtils.nullSafe(this.keyValue.get(COMMENT));
			String started = JiraUtils.nullSafe(this.keyValue.get(STARTED));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.updateWorklog(accessToken, baseUrl, issueKey, worklogId,
					timeSpent, comment, started);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while updating Jira worklog", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update Jira worklog", e);
			throw new SemossPixelException(
					"An error occurred while updating the Jira worklog. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Updates an existing worklog entry on a Jira issue. Use JiraGetWorklogsReactor first to get the worklog ID. Returns id, timeSpent, timeSpentSeconds, author, updated, and success. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Fails if missing or invalid.";
		} else if (key.equals(WORKLOG_ID)) {
			return "Required. The numeric worklog ID to update. Get it from JiraGetWorklogsReactor. Fails if missing or if the worklog does not exist.";
		} else if (key.equals(TIME_SPENT)) {
			return "Required. Updated time in Jira notation, for example '3h', '1d 2h', '45m'. Supports weeks (w), days (d), hours (h), and minutes (m). Fails if missing or in invalid format.";
		} else if (key.equals(COMMENT)) {
			return "Optional. Updated plain-text description of what was done. Omit to leave the existing comment unchanged.";
		} else if (key.equals(STARTED)) {
			return "Optional. Updated start datetime in ISO 8601 format, for example '2026-04-07T09:00:00.000+0000'. Omit to leave it unchanged.";
		}
		return super.getDescriptionForKey(key);
	}
}
