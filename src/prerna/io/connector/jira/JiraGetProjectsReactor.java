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
		this.keysToGet = new String[] { ReactorKeysEnum.URL.getKey(), "keyname", ReactorKeysEnum.API_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String url = this.keyValue.get(this.keysToGet[0]);
			String userId = this.keyValue.get(this.keysToGet[1]);
			String apiKey = this.keyValue.get(this.keysToGet[2]);
			return JiraHelper.getAllProjects(url, userId, apiKey);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred while getting project details. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to retrieve all Jira projects accessible to the user.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("keyname")) {
			return "The keyname of the connection from DB through which details can be fetched of a user.";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "The Jira URL on which all projects are present and tickets can be created";
		} else if (key.equals(ReactorKeysEnum.API_KEY.getKey())) {
			return "The api key of the token created by user to interact with JIRA Dashboard";
		}
		return super.getDescriptionForKey(key);
	}
}
