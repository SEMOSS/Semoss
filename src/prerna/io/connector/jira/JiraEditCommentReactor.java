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

public class JiraEditCommentReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraEditCommentReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String COMMENT_ID = "commentId";
	private static final String COMMENT = "comment";

	public JiraEditCommentReactor() {
		this.keysToGet = new String[] { JIRAID, COMMENT_ID, COMMENT };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = JiraUtils.nullSafe(this.keyValue.get(JIRAID));
			String commentId = JiraUtils.nullSafe(this.keyValue.get(COMMENT_ID));
			String comment = JiraUtils.nullSafe(this.keyValue.get(COMMENT));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.editComment(accessToken, baseUrl, issueKey, commentId, comment);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while editing a Jira comment", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to edit a Jira comment", e);
			throw new SemossPixelException(
					"An error occurred while editing the Jira comment. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Edits an existing comment on a Jira issue. Use when correcting or updating a previous comment; use JiraAddCommentReactor for new comments. Returns id, author, created, updated, and success. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Fails if missing or invalid.";
		} else if (key.equals(COMMENT_ID)) {
			return "Required. The numeric comment ID. Get it from JiraGetCommentsReactor. Fails if missing or if the comment does not exist.";
		} else if (key.equals(COMMENT)) {
			return "Required. Updated plain-text comment body. Replaces the entire existing comment. Empty text is rejected by Jira.";
		}
		return super.getDescriptionForKey(key);
	}
}
