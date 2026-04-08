package prerna.io.connector.jira;

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraAddAttachmentReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraAddAttachmentReactor.class);

	private static final String JIRAID = "jiraid";
	private static final String FILE_PATH = "filePath";

	public JiraAddAttachmentReactor() {
		this.keysToGet = new String[] { JIRAID, FILE_PATH };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String issueKey = JiraUtils.nullSafe(this.keyValue.get(JIRAID));
			String filePath = JiraUtils.nullSafe(this.keyValue.get(FILE_PATH));
			User user = this.insight.getUser();
			File filePathDir = new File(filePath);
			String audioFileName = filePathDir.getName();
			String insightFolder = this.insight.getInsightFolder();
			String tempFilePath = insightFolder + File.separator + audioFileName;
			File localFileDir = new File(tempFilePath);
			String localFilePath = localFileDir.getAbsolutePath();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.addAttachment(accessToken, baseUrl, issueKey, localFilePath);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while adding attachment to Jira ticket", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to add attachment to Jira ticket", e);
			throw new SemossPixelException(
					"An error occurred while adding the attachment. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Uploads a file attachment to a Jira issue. Use to share documents, screenshots, or logs with the issue. Returns id, filename, size, and success. Requires Jira auth and a valid local file path.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JIRAID)) {
			return "Required. Jira issue key in KEY-NUMBER format, for example RTJ-123. Get it from JiraGetTicketsReactor, JiraSearchReactor, or JiraReadTicketReactor. Fails if missing or invalid.";
		} else if (key.equals(FILE_PATH)) {
			return "Required. Absolute path to the local file to upload, for example '/tmp/screenshot.png'. The file must exist and be readable. Fails if file not found.";
		}
		return super.getDescriptionForKey(key);
	}
}
