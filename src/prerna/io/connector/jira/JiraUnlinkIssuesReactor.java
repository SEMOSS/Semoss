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

public class JiraUnlinkIssuesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraUnlinkIssuesReactor.class);

	private static final String LINK_ID = "linkId";

	public JiraUnlinkIssuesReactor() {
		this.keysToGet = new String[] { LINK_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String linkId = JiraUtils.nullSafe(this.keyValue.get(LINK_ID));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.unlinkIssues(accessToken, baseUrl, linkId);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while unlinking Jira issues", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to unlink Jira issues", e);
			throw new SemossPixelException(
					"An error occurred while unlinking Jira issues. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Removes an existing link between two Jira issues. Use after JiraReadTicketReactor to get the link ID from the issue's issuelinks field. Returns success. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(LINK_ID)) {
			return "Required. The numeric issue link ID to remove. Get it from JiraReadTicketReactor's issuelinks data. Fails if missing or if the link does not exist.";
		}
		return super.getDescriptionForKey(key);
	}
}
