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
		return "Adds a new comment to an existing Jira issue. Use this when the task is to append discussion or notes to a ticket; do not use JiraUpdateTicketReactor for comments because that reactor only updates issue fields. Returns a map with id, author, created, and success for the new comment. Preconditions: the current SEMOSS user must already have Jira credentials, and jiraid must identify an existing Jira issue.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get this from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor if the issue is not already known. Do not pass a project key or Jira numeric id. If this value is wrong or missing, Jira cannot find the issue and the comment is not added.";
		} else if (key.equals(COMMENT)) {
			return "Required. Plain-text comment content to add to the issue. Provide the full message string, including line breaks if needed. This reactor converts the text to Atlassian document format internally. If the value is empty or missing, Jira rejects the request.";
		}
		return super.getDescriptionForKey(key);
	}
}