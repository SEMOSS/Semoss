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
			String issueKey = JiraUtils.nullSafe(this.keyValue.get(JIRAID));
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
		return "Lists workflow transitions currently available for one Jira issue. Use before JiraUpdateTicketReactor for status changes; this is not a global status list. Returns id, name, and toStatus. Requires Jira auth and a valid issue key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Status options are specific to this issue. Fails if missing or invalid.";
		}
		return super.getDescriptionForKey(key);
	}
}