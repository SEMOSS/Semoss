package prerna.io.connector.docs;

import java.util.HashMap;
import java.util.Map;

import com.google.api.services.docs.v1.Docs;
import com.google.api.services.docs.v1.model.Document;
import com.google.api.services.drive.Drive;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsCreateReactor extends AbstractReactor {

	public GoogleDocsCreateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROMPT_TITLE.getKey(), ReactorKeysEnum.CONTENT.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String title = this.keyValue.get(this.keysToGet[0]);
		String content = null;

		if (this.keyValue.get(this.keysToGet[1]) != null && !this.keyValue.get(this.keysToGet[1]).isEmpty()) {
			content = this.keyValue.get(this.keysToGet[1]);
		}

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleDocsUtils.getGoogleAccessToken(user);
			Docs service = GoogleDocsUtils.getDocsServiceUsingToken(accessToken);
			Drive getDriveService = GoogleDocsUtils.getDriveServiceUsingToken(accessToken);

			boolean success = false;
			String DOCID = "docid";
			String SUCCESS = "success";
			Map<String, Object> map = new HashMap<>();
			try {
				Document doc = GoogleDocsHelper.createDoc(service, getDriveService, title, content);
				if (doc != null && doc.getDocumentId() != null) {
					success = true;
					map.put(DOCID, doc.getDocumentId());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			map.put(SUCCESS, success);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Please provide valid input", e);
		}

	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to create the Google document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROMPT_TITLE.getKey())) {
			return "Title of the Document " + ReactorKeysEnum.PROMPT_TITLE.getKey();
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Content to be added to the document " + ReactorKeysEnum.CONTENT.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
