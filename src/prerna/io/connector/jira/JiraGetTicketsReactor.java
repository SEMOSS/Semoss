package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraHelper;

public class JiraGetTicketsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetTicketsReactor.class);

	public JiraGetTicketsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.KEY_NAME.getKey()};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String keyName = this.keyValue.get(this.keysToGet[0]);
			return JiraHelper.listIssue(keyName);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor lists all Jira tickets/issues for a given project and user key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.KEY_NAME.getKey())) {
			return "The key name identifying the Jira user credentials.";
		} else if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The Jira project key for which to list issues.";
		}
		return super.getDescriptionForKey(key);
	}
}
