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

public class JiraGetUpdateFieldsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetUpdateFieldsReactor.class);

	private static final String JIRAID = "jiraid";

	public JiraGetUpdateFieldsReactor() {
		this.keysToGet = new String[] { JIRAID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			String issueKey = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));

			List<Map<String, Object>> fields = JiraHelper.getEditMetaFields(accessToken, baseUrl, issueKey);
			return new NounMetadata(fields, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error in JiraGetUpdateFieldsReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get Jira editable fields", e);
			throw new SemossPixelException(
					"An error occurred while getting Jira editable fields. Error: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns editable fields for an existing Jira issue. Each field includes fieldId, name, required, schema (with type indicating value structure such as string, priority, user, array), and allowedValues listing valid options for enum-style fields.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required Jira issue key (for example, RTJ-42) from a previous get response.";
		}
		return super.getDescriptionForKey(key);
	}
}
