package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

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
			return JiraHelper.addComment(user, issueKey, comment);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred while adding a comment to the Jira ticket. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor adds a comment to a Jira issue. Returns id, author, created, and success flag.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "The Jira issue key to add a comment to (e.g. PROJECT-123).";
		} else if (key.equals(COMMENT)) {
			return "The comment text to add to the issue.";
		}
		return super.getDescriptionForKey(key);
	}
}
