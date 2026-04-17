package prerna.io.connector.jira;

import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraCreateIssueReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraCreateIssueReactor.class);

	private static final String PARAM_MAP = "paramMap";

	public JiraCreateIssueReactor() {
		this.keysToGet = new String[] { PARAM_MAP };
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

			Map<String, Object> fieldMap = getInputFieldMap();
			if (fieldMap == null || fieldMap.isEmpty()) {
				throw new SemossPixelException(
						"paramMap is required and must be passed as a JSON object / MAP");
			}

			Map<String, Object> result = JiraHelper.createIssueFromMap(accessToken, baseUrl, fieldMap);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while creating a Jira issue", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create a Jira issue", e);
			throw new SemossPixelException(
					"An error occurred while creating the Jira issue. Error message: " + e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getInputFieldMap() {
		GenRowStruct grs = this.store.getNoun(PARAM_MAP);
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if (mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}
		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, Object>) mapNouns.get(0).getValue();
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Creates a new Jira issue. Only accepts issue creation fields as returned by JiraGetCreateFields. Returns the created issue id and key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PARAM_MAP)) {
			return "Required object / MAP of issue creation field key-value pairs as returned by JiraGetCreateFields. Pass paramMap as a raw JSON object / MAP, not as a quoted JSON string.";
		}
		return super.getDescriptionForKey(key);
	}
}