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

public class JiraReadIssueReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraReadIssueReactor.class);

	private static final String JIRAID = "jiraid";

	public JiraReadIssueReactor() {
		this.keysToGet = new String[] { JIRAID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.readIssue(accessToken, baseUrl, issueKey);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading a Jira issue", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to read a Jira issue", e);
			throw new SemossPixelException(
					"An error occurred while reading the Jira issue. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Reads a single Jira issue by key, returning its id, key, and all system and custom fields.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required Jira issue key in KEY-NUMBER format (for example, RTJ-123).";
		}
		return super.getDescriptionForKey(key);
	}
}