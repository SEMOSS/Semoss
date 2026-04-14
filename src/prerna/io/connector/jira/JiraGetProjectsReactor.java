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

public class JiraGetProjectsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetProjectsReactor.class);

	public JiraGetProjectsReactor() {

	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			List<Map<String, Object>> result = JiraHelper.getAllProjects(accessToken, baseUrl);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while retrieving Jira projects", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve Jira projects", e);
			throw new SemossPixelException(
					"An error occurred while retrieving Jira projects. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Lists all Jira projects visible to the current user. Returns id, project key, name, projectTypeKey, and lead for each project.";
	}
}