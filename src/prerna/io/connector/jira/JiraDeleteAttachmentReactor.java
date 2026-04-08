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

public class JiraDeleteAttachmentReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraDeleteAttachmentReactor.class);

	private static final String ATTACHMENT_ID = "attachmentId";

	public JiraDeleteAttachmentReactor() {
		this.keysToGet = new String[] { ATTACHMENT_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String attachmentId = JiraUtils.nullSafe(this.keyValue.get(ATTACHMENT_ID));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.deleteAttachment(accessToken, baseUrl, attachmentId);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting Jira attachment", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete Jira attachment", e);
			throw new SemossPixelException(
					"An error occurred while deleting the Jira attachment. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Permanently deletes a file attachment from a Jira issue. Use JiraGetAttachmentsReactor first to get the attachment ID. Returns success. Requires Jira auth.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ATTACHMENT_ID)) {
			return "Required. The numeric attachment ID to delete. Get it from JiraGetAttachmentsReactor. Fails if missing or if the attachment does not exist.";
		}
		return super.getDescriptionForKey(key);
	}
}
