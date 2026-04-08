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

public class JiraLogWorkReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraLogWorkReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String TIME_SPENT = "timeSpent";
	private static final String COMMENT = "comment";
	private static final String STARTED = "started";

	public JiraLogWorkReactor() {
		this.keysToGet = new String[] { JIRAID, TIME_SPENT, COMMENT, STARTED };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = JiraUtils.nullSafe(this.keyValue.get(JIRAID));
			String timeSpent = JiraUtils.nullSafe(this.keyValue.get(TIME_SPENT));
			String comment = JiraUtils.nullSafe(this.keyValue.get(COMMENT));
			String started = JiraUtils.nullSafe(this.keyValue.get(STARTED));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.logWork(accessToken, baseUrl, issueKey, timeSpent, comment, started);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while logging work on Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to log work on Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while logging work on the Jira ticket. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Logs time spent working on a Jira issue. Use for time tracking and resource reporting. Returns id, timeSpent, timeSpentSeconds, author, created, and success. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Fails if missing or invalid.";
		} else if (key.equals(TIME_SPENT)) {
			return "Required. Time in Jira notation, for example '2h 30m', '1d', '45m'. Supports weeks (w), days (d), hours (h), and minutes (m). Fails if missing or in invalid format.";
		} else if (key.equals(COMMENT)) {
			return "Optional. Plain-text description of what was done during this work period. Omit to log time without a comment.";
		} else if (key.equals(STARTED)) {
			return "Optional. When the work started in ISO 8601 format, for example '2026-04-07T09:00:00.000+0000'. Defaults to now if omitted.";
		}
		return super.getDescriptionForKey(key);
	}
}
