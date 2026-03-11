package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class JiraCreateTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraCreateTicketReactor.class);

	private static final String KEYNAME = "keyname";
	private static final String SUMMARY = "summary";
	private static final String DESCRIPTION = "description";
	private static final String ISSUETYPE = "issuetype";

	public JiraCreateTicketReactor() {
		this.keysToGet = new String[] { KEYNAME, SUMMARY, DESCRIPTION, ISSUETYPE };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String keyName = this.keyValue.get(KEYNAME);
			String summary = this.keyValue.get(SUMMARY);
			String description = this.keyValue.get(DESCRIPTION);
			String issuetype = this.keyValue.get(ISSUETYPE);
			return JiraHelper.createIssue(summary, description, issuetype, keyName);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
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
		if (key.equals(KEYNAME)) {
			return "The unique key name for the Jira issue.";
		} else if (key.equals(SUMMARY)) {
			return "A brief summary/title for the Jira issue.";
		} else if (key.equals(DESCRIPTION)) {
			return "A detailed description of the Jira issue.";
		} else if (key.equals(ISSUETYPE)) {
			return "The type of Jira issue (e.g., Bug, Task, Story).";
		} 
		return super.getDescriptionForKey(key);
	}
}
