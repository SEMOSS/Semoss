package prerna.io.connector.jira;

import java.util.HashMap;
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

public class JiraUpdateTicketReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraUpdateTicketReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String PARAM_MAP = "paramMap";

	public JiraUpdateTicketReactor() {
		this.keysToGet = new String[] { JIRAID, PARAM_MAP };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String jiraId = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));

			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			Map<String, Object> inputMap = getInputFieldMap();
			if (inputMap == null || inputMap.isEmpty()) {
				throw new SemossPixelException("Provide a map of fields to update.");
			}

			Map<String, Object> fieldMap = new HashMap<>(inputMap);

			String statusValue = extractStringValue(fieldMap.remove("status"));
			String transitionValue = extractStringValue(fieldMap.remove("transition"));

			boolean hasFields = !fieldMap.isEmpty();
			boolean hasTransition = transitionValue != null || statusValue != null;

			if (!hasFields && !hasTransition) {
				throw new SemossPixelException("No editable fields or status/transition provided.");
			}
			if (hasFields) {
				JiraHelper.updateIssueFromMap(accessToken, baseUrl, jiraId, fieldMap);
			}
			if (transitionValue != null) {
				if (transitionValue.chars().allMatch(Character::isDigit)) {
					JiraHelper.transitionIssueById(accessToken, baseUrl, jiraId, transitionValue);
				} else {
					JiraHelper.transitionIssue(accessToken, baseUrl, jiraId, transitionValue);
				}
			} else if (statusValue != null) {
				JiraHelper.transitionIssue(accessToken, baseUrl, jiraId, statusValue);
			}

			Map<String, Object> result = new HashMap<>();
			result.put("jiraid", jiraId);
			result.put("success", true);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while updating a Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update a Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while updating the Jira ticket. Error message: " + e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private String extractStringValue(Object value) {
		if (value instanceof String && !((String) value).trim().isEmpty()) {
			return ((String) value).trim();
		}
		if (value instanceof Map) {
			Object nameVal = ((Map<?, ?>) value).get("name");
			if (nameVal instanceof String && !((String) nameVal).trim().isEmpty()) {
				return ((String) nameVal).trim();
			}
			Object idVal = ((Map<?, ?>) value).get("id");
			if (idVal != null && !idVal.toString().trim().isEmpty()) {
				return idVal.toString().trim();
			}
		}
		return null;
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
		return "Updates an existing Jira issue. Only accepts editable issue fields as returned by JiraGetUpdateFields, plus status or transition for workflow changes.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required Jira issue key in KEY-NUMBER format (for example, RTJ-123).";
		} else if (key.equals(PARAM_MAP)) {
			return "Required map of editable field key-value pairs as returned by JiraGetUpdateFields. Also accepts status (target status name) or transition (transition id or name) for workflow changes.";
		}
		return super.getDescriptionForKey(key);
	}
}