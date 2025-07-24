package prerna.io.connector.docs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.HashMap;
import java.util.Map;

import com.google.api.services.docs.v1.Docs;
import com.google.api.services.docs.v1.model.Document;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsReadReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDocsReadReactor.class);

	public GoogleDocsReadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[0]);

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleDocsUtils.getGoogleAccessToken(user);
			Docs service = GoogleDocsUtils.getDocsServiceUsingToken(accessToken);
			String title = getDocTitle(service, id);
			String contentValue = GoogleDocsHelper.readDoc(service, id);
			Map<String, Object> map = new HashMap<>();
			map.put("title", title);
			map.put("content", contentValue);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}

	}
	
	public static String getDocTitle(Docs service, String docId) throws Exception {
	    Document doc = service.documents().get(docId).execute();
	    return doc.getTitle();
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to read the Google document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "ID of the Document to be read" + ReactorKeysEnum.ID.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}
