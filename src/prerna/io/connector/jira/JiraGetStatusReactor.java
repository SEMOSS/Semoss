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

public class JiraGetStatusReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetStatusReactor.class);

	private static final String JIRAID = "jiraid";

	public JiraGetStatusReactor() {
		this.keysToGet = new String[] { JIRAID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = this.keyValue.get(JIRAID);
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			List<Map<String, Object>> result = JiraHelper.getStatus(accessToken, baseUrl, issueKey);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while retrieving Jira status options", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve Jira status options", e);
			throw new SemossPixelException(
					"An error occurred while retrieving Jira status options for the ticket. Error message: "
							+ e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns the workflow transitions currently available for a specific Jira issue. Use this before JiraUpdateTicketReactor when the task is to move an issue to a new status; this reactor does not return every status in the project, only the transitions valid for the current issue state and workflow. Returns a list of maps with id, name, and toStatus. Preconditions: the current SEMOSS user must already have Jira credentials and jiraid must identify the exact issue being transitioned.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get this from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Transitions depend on the current issue state, so using a different ticket can produce the wrong status options. If the key is wrong or missing, Jira cannot return transitions.";
		}
		return super.getDescriptionForKey(key);
	}
}