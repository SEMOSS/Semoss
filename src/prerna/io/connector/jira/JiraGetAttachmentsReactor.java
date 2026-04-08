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

public class JiraGetAttachmentsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraGetAttachmentsReactor.class);

	private static final String JIRAID = "jiraid";

	public JiraGetAttachmentsReactor() {
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
			List<Map<String, Object>> result = JiraHelper.getAttachments(accessToken, baseUrl, issueKey);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while retrieving Jira attachments", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to retrieve Jira attachments", e);
			throw new SemossPixelException(
					"An error occurred while retrieving Jira attachments. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Lists all file attachments on a Jira issue. Use to see what files are attached before downloading or adding more. Returns id, filename, mimeType, size, created, contentUrl, and author. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Fails if missing or invalid.";
		}
		return super.getDescriptionForKey(key);
	}
}
