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

public class JiraGetCreateFieldsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetCreateFieldsReactor.class);

	private static final String PROJECT = "project";
	private static final String ISSUETYPEID = "issuetypeid";

	public JiraGetCreateFieldsReactor() {
		this.keysToGet = new String[] { PROJECT, ISSUETYPEID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			String projectKey = JiraUtils.validateProjectKey(this.keyValue.get(PROJECT));
			String issueTypeId = JiraUtils.validateNumericId(this.keyValue.get(ISSUETYPEID), "issuetypeid");

			List<Map<String, Object>> fields = JiraHelper.getCreateMetaFields(accessToken, baseUrl, projectKey, issueTypeId);
			return new NounMetadata(fields, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error in JiraGetCreateFieldsReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get Jira fields", e);
			throw new SemossPixelException(
					"An error occurred while getting Jira fields. Error: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns available fields for a given Jira project and issue type. Each field includes fieldId, name, required, hasDefaultValue, schema (with type indicating value structure such as string, project, priority, user, date, array), and allowedValues listing valid options for enum-style fields.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required Jira project key in uppercase (for example, RTJ).";
		} else if (key.equals(ISSUETYPEID)) {
			return "Required numeric issue type id.";
		}
		return super.getDescriptionForKey(key);
	}
}
