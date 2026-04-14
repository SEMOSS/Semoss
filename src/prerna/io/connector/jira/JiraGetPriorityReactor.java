package prerna.io.connector.jira;

import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraGetPriorityReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetPriorityReactor.class);

	public JiraGetPriorityReactor() {

	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			List<Map<String, Object>> result = JiraHelper.getPriorities(accessToken, baseUrl);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while retrieving Jira priorities", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve Jira priorities", e);
			throw new SemossPixelException(
					"An error occurred while retrieving Jira priorities. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Lists available Jira priority levels with their ids and names.";
	}
}