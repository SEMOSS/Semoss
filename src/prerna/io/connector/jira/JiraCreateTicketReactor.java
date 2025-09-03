package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class JiraCreateTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraCreateTicketReactor.class);

	public JiraCreateTicketReactor() {
		this.keysToGet = new String[] { "keyname", "summary", "description", "issuetype" };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String keyName = this.keyValue.get(this.keysToGet[0]);
			String summary = this.keyValue.get(this.keysToGet[1]);
			String description = this.keyValue.get(this.keysToGet[2]);
			String issuetype = this.keyValue.get(this.keysToGet[3]);
			return JiraHelper.createIssue(summary, description, issuetype, keyName);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while creating ticket. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to create a new Jira ticket/issue.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("keyname")) {
			return "The unique key name for the Jira issue.";
		} else if (key.equals("summary")) {
			return "A brief summary/title for the Jira issue.";
		} else if (key.equals("description")) {
			return "A detailed description of the Jira issue.";
		} else if (key.equals("issuetype")) {
			return "The type of Jira issue (e.g., Bug, Task, Story).";
		} else if (key.equals("project")) {
			return "The Jira project where the issue will be created.";
		}
		return super.getDescriptionForKey(key);
	}
}
