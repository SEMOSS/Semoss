package prerna.io.connector.docs;

import java.util.HashMap;
import java.util.Map;

import com.google.api.services.docs.v1.Docs;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsUpdateReactor extends AbstractReactor {

	public GoogleDocsUpdateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.CONTENT.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[0]);
		String content = this.keyValue.get(this.keysToGet[1]);

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleDocsUtils.getGoogleAccessToken(user);
			Docs service = GoogleDocsUtils.getDocsServiceUsingToken(accessToken);
			boolean updateresult = GoogleDocsHelper.updateDoc(service, id, content);
			Map<String, Object> map = new HashMap<>();
			map.put("status: ", updateresult);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Please provide valid input", e);
		}

	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to update the Google document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROMPT_TITLE.getKey())) {
			return "Title of the Document " + ReactorKeysEnum.PROMPT_TITLE.getKey();
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Updated Content to be added to the document " + ReactorKeysEnum.CONTENT.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
