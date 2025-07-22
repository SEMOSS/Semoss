package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraHelper;

public class JiraListAllTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraListAllTicketReactor.class);

	public JiraListAllTicketReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.KEY_NAME.getKey(), ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String keyName = this.keyValue.get(this.keysToGet[0]);
			String project = this.keyValue.get(this.keysToGet[1]);
			return JiraHelper.listIssue(project, keyName);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
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
