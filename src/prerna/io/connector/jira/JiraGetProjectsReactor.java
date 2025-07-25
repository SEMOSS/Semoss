package prerna.io.connector.jira;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraHelper;

public class JiraGetProjectsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetProjectsReactor.class);

	public JiraGetProjectsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.KEY_NAME.getKey() , ReactorKeysEnum.API_KEY.getKey()};
		this.keyRequired = new int[] { 1 ,1 ,1};
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String url = this.keyValue.get(this.keysToGet[0]);
			String keyName = this.keyValue.get(this.keysToGet[1]);
			String apiKey = this.keyValue.get(this.keysToGet[2]);
			return JiraHelper.getAllProjects(url,keyName, apiKey);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to retrieve all Jira projects accessible to the user.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.KEY_NAME.getKey())) {
			return "The unique key name used for authentication or context.";
		}
		return super.getDescriptionForKey(key);
	}
}
