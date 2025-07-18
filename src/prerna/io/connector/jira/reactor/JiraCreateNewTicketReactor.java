package prerna.io.connector.jira.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.jira.reactor.JiraCreateNewTicketReactor;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraHelper;

public class JiraCreateNewTicketReactor extends AbstractReactor{
	
	private static final Logger classLogger = LogManager.getLogger(JiraCreateNewTicketReactor.class);

	public JiraCreateNewTicketReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.KEY_NAME.getKey(),
				ReactorKeysEnum.SUMMARY.getKey(), ReactorKeysEnum.DESCRIPTION.getKey(),
				ReactorKeysEnum.ISSUETYPE.getKey(), ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String keyName = this.keyValue.get(this.keysToGet[0]);
			String summary = this.keyValue.get(this.keysToGet[1]);
			String description = this.keyValue.get(this.keysToGet[2]);
			String issuetype = this.keyValue.get(this.keysToGet[3]);
			String project = this.keyValue.get(this.keysToGet[4]);
			return JiraHelper.createIssue(summary, description, issuetype, project, keyName);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

}
