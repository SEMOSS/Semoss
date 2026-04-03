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

public class JiraAddCommentReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraAddCommentReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String COMMENT = "comment";

	public JiraAddCommentReactor() {
		this.keysToGet = new String[] { JIRAID, COMMENT };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = this.keyValue.get(JIRAID);
			String comment = this.keyValue.get(COMMENT);
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.addComment(accessToken, baseUrl, issueKey, comment);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while adding a comment to a Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to add a comment to a Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while adding a comment to the Jira ticket. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Adds a comment to an existing Jira issue. Use for discussion updates only; use JiraUpdateTicketReactor for field changes. Returns id, author, created, and success. Requires Jira auth and a valid issue key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Not a project key or numeric id. Fails if missing or invalid.";
		} else if (key.equals(COMMENT)) {
			return "Required. Plain-text comment body. Line breaks are allowed. Empty or missing text is rejected by Jira.";
		}
		return super.getDescriptionForKey(key);
	}
}