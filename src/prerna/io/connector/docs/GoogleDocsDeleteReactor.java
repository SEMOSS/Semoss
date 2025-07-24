package prerna.io.connector.docs;

import com.google.api.services.drive.Drive;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsDeleteReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDocsDeleteReactor.class);

	public GoogleDocsDeleteReactor() {
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
			Drive getDriveService = GoogleDocsUtils.getDriveServiceUsingToken(accessToken);
			boolean deleteresult = GoogleDocsHelper.deleteDoc(getDriveService, id);
			Map<String, Object> map = new HashMap<>();
			map.put("status", deleteresult);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}

	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to delete the Google document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "ID of the Document to be deleted" + ReactorKeysEnum.ID.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}
