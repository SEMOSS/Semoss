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

public class JiraDeleteCommentReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraDeleteCommentReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String COMMENT_ID = "commentId";

	public JiraDeleteCommentReactor() {
		this.keysToGet = new String[] { JIRAID, COMMENT_ID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = JiraUtils.nullSafe(this.keyValue.get(JIRAID));
			String commentId = JiraUtils.nullSafe(this.keyValue.get(COMMENT_ID));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.deleteComment(accessToken, baseUrl, issueKey, commentId);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting a Jira comment", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete a Jira comment", e);
			throw new SemossPixelException(
					"An error occurred while deleting the Jira comment. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Permanently deletes a comment from a Jira issue. Use only for removal; use JiraEditCommentReactor to update text. Returns success. Requires Jira auth and you must be the comment author or a project admin.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Fails if missing or invalid.";
		} else if (key.equals(COMMENT_ID)) {
			return "Required. The numeric comment ID to delete. Get it from JiraGetCommentsReactor. Fails if missing or if the comment does not exist.";
		}
		return super.getDescriptionForKey(key);
	}
}
