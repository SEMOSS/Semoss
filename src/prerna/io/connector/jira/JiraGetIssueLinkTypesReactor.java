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

public class JiraGetIssueLinkTypesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetIssueLinkTypesReactor.class);

	public JiraGetIssueLinkTypesReactor() {
		
	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			List<Map<String, Object>> result = JiraHelper.getIssueLinkTypes(accessToken, baseUrl);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while retrieving Jira issue link types", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve Jira issue link types", e);
			throw new SemossPixelException(
					"An error occurred while retrieving Jira issue link types. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Lists all issue link types available in the Jira instance (for example, Blocks, Relates, Duplicate).";
	}
}
