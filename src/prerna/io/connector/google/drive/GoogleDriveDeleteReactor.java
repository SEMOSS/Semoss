package prerna.io.connector.google.drive;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleDriveDeleteReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDriveDeleteReactor.class);

	public GoogleDriveDeleteReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey()};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String fileId = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			Map<String, Object> result = GoogleDriveHelper.deleteFile(accessToken, fileId);
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch(SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred deleting the file. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Delete an existing file in Google Drive";
	}

	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "Unique identifier of the Google Drive file to be deleted " + ReactorKeysEnum.ID.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}
