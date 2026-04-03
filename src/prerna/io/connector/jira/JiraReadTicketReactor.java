package prerna.io.connector.jira;

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

public class JiraReadTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraReadTicketReactor.class);

	private static final String JIRAID = "jiraid";

	public JiraReadTicketReactor() {
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
			Map<String, Object> result = JiraHelper.readTicket(accessToken, baseUrl, issueKey);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading a Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to read a Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while reading the Jira ticket. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor reads full details of a single Jira issue by its key. Returns id, key, summary, status, priority, issuetype, assignee, assigneeAccountId, duedate, labels, and description.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "The Jira issue key to fetch (e.g. PROJECT-123).";
		}
		return super.getDescriptionForKey(key);
	}
}