package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraHelper;

public class JiraDeleteTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraDeleteTicketReactor.class);

	public JiraDeleteTicketReactor() {
		this.keysToGet = new String[] { "keyname", "jiraid" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String keyName = this.keyValue.get(this.keysToGet[0]);
			String jiraId = this.keyValue.get(this.keysToGet[1]);
			return JiraHelper.deleteIssue(jiraId, keyName);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while deleting ticket. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to delete a Jira ticket/issue by its ID and project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("keyname")) {
			return "The unique key name for the Jira issue to be deleted.";
		} else if (key.equals("jiraid")) {
			return "The Jira ID of the ticket/issue to be deleted.";
		}
		return super.getDescriptionForKey(key);
	}
}
