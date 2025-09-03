package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class JiraGetTicketsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetTicketsReactor.class);

	public JiraGetTicketsReactor() {
		this.keysToGet = new String[] { "keyname" };
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
			throw new SemossPixelException(
					"An error occurred while getting tickets and its details. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor lists all Jira tickets/issues for a given project and user key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("keyname")) {
			return "The keyname of the connection from DB through which details can be fetched of a user.";
		}
		return super.getDescriptionForKey(key);
	}
}
