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

public class JiraLinkIssuesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraLinkIssuesReactor.class);

	private static final String LINK_TYPE = "linkType";
	private static final String INWARD_ISSUE = "inwardIssue";
	private static final String OUTWARD_ISSUE = "outwardIssue";

	public JiraLinkIssuesReactor() {
		this.keysToGet = new String[] { LINK_TYPE, INWARD_ISSUE, OUTWARD_ISSUE };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String linkType = JiraUtils.nullSafe(this.keyValue.get(LINK_TYPE));
			String inwardIssue = JiraUtils.nullSafe(this.keyValue.get(INWARD_ISSUE));
			String outwardIssue = JiraUtils.nullSafe(this.keyValue.get(OUTWARD_ISSUE));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.linkIssues(accessToken, baseUrl, linkType, inwardIssue, outwardIssue);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while linking Jira issues", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to link Jira issues", e);
			throw new SemossPixelException(
					"An error occurred while linking Jira issues. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Creates a relationship link between two Jira issues (for example, blocks, is blocked by, relates to, duplicates). Use to model dependencies and relationships. Returns success, linkType, inwardIssue, and outwardIssue. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(LINK_TYPE)) {
			return "Required. The exact link type name, for example 'Blocks', 'Relates', 'Duplicate'. Get valid names from JiraGetIssueLinkTypesReactor. Fails if the type does not exist.";
		} else if (key.equals(INWARD_ISSUE)) {
			return "Required. Jira issue key for the inward side of the link, for example RTJ-123. The inward side typically represents the 'is blocked by' or 'is duplicated by' end.";
		} else if (key.equals(OUTWARD_ISSUE)) {
			return "Required. Jira issue key for the outward side of the link, for example RTJ-456. The outward side typically represents the 'blocks' or 'duplicates' end.";
		}
		return super.getDescriptionForKey(key);
	}
}
