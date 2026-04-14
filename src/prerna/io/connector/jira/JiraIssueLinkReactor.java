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

public class JiraIssueLinkReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraIssueLinkReactor.class);

	private static final String ACTION = "action";
	private static final String LINK_TYPE = "linkType";
	private static final String INWARD_ISSUE = "inwardIssue";
	private static final String OUTWARD_ISSUE = "outwardIssue";
	private static final String LINK_ID = "linkId";

	public JiraIssueLinkReactor() {
		this.keysToGet = new String[] { ACTION, LINK_TYPE, INWARD_ISSUE, OUTWARD_ISSUE, LINK_ID };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String action = this.keyValue.get(ACTION);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException("The action parameter is required. Valid values are: link, unlink.");
			}
			action = action.trim().toLowerCase();

			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			switch (action) {
			case "link": {
				String linkType = this.keyValue.get(LINK_TYPE);
				if (linkType == null || linkType.trim().isEmpty()) {
					throw new SemossPixelException("Link type (linkType) is required for the link action.");
				}
				String inwardIssue = JiraUtils.validateIssueKey(this.keyValue.get(INWARD_ISSUE));
				String outwardIssue = JiraUtils.validateIssueKey(this.keyValue.get(OUTWARD_ISSUE));
				if (inwardIssue.equalsIgnoreCase(outwardIssue)) {
					throw new SemossPixelException(
							"Cannot link an issue to itself. inwardIssue and outwardIssue must be different.");
				}
				Map<String, Object> result = JiraHelper.linkIssues(accessToken, baseUrl, linkType, inwardIssue, outwardIssue);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "unlink": {
				String linkId = JiraUtils.validateNumericId(this.keyValue.get(LINK_ID), "linkId");
				Map<String, Object> result = JiraHelper.unlinkIssues(accessToken, baseUrl, linkId);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			default:
				throw new SemossPixelException("Invalid action '" + action + "'. Valid values are: link, unlink.");
			}
		} catch (SemossPixelException e) {
			classLogger.error("Error in JiraIssueLinkReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to execute JiraIssueLinkReactor", e);
			throw new SemossPixelException(
					"An error occurred in JiraIssueLinkReactor. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Manages relationship links between Jira issues. Supports creating and removing issue links.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ACTION)) {
			return "Required operation to perform. Valid values: link - creates a link between two issues (linkType, inwardIssue, outwardIssue required). unlink - removes an existing link (linkId required).";
		} else if (key.equals(LINK_TYPE)) {
			return "Exact link type name (for example, Blocks, Relates, Duplicate). Required for link action.";
		} else if (key.equals(INWARD_ISSUE)) {
			return "Jira issue key for the inward side of the link (for example, RTJ-123). Required for link action.";
		} else if (key.equals(OUTWARD_ISSUE)) {
			return "Jira issue key for the outward side of the link (for example, RTJ-456). Required for link action.";
		} else if (key.equals(LINK_ID)) {
			return "Numeric issue link id to remove. Required for unlink action.";
		}
		return super.getDescriptionForKey(key);
	}
}
