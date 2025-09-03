package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class JiraDeleteApiKeyReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraDeleteApiKeyReactor.class);

	public JiraDeleteApiKeyReactor() {
		this.keysToGet = new String[] { "keyname" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String keyName = this.keyValue.get(this.keysToGet[0]);
			return JiraHelper.deleteRecordForUser(keyName);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred while deleting api key from JIRA DB. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to delete a database user record via Jira integration.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("keyname")) {
			return "The unique key name identifying the database user to delete.";
		}
		return super.getDescriptionForKey(key);
	}
}
