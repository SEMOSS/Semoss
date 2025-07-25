package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraHelper;

public class JiraCreateTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraCreateTicketReactor.class);

	public JiraCreateTicketReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.KEY_NAME.getKey(), ReactorKeysEnum.SUMMARY.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.ISSUETYPE.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1};
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
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to create a new Jira ticket/issue.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.KEY_NAME.getKey())) {
			return "The unique key name for the Jira issue.";
		} else if (key.equals(ReactorKeysEnum.SUMMARY.getKey())) {
			return "A brief summary/title for the Jira issue.";
		} else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
			return "A detailed description of the Jira issue.";
		} else if (key.equals(ReactorKeysEnum.ISSUETYPE.getKey())) {
			return "The type of Jira issue (e.g., Bug, Task, Story).";
		} else if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The Jira project key where the issue will be created.";
		}
		return super.getDescriptionForKey(key);
	}
}
