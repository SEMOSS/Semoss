package prerna.io.connector.google.drive;

import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleDriveUploadReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDriveUploadReactor.class);

	public GoogleDriveUploadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String name = this.keyValue.get(this.keysToGet[0]);
		String path = this.keyValue.get(this.keysToGet[1]);
		
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			Map<String, Object> result = GoogleDriveHelper.uploadFile(accessToken, name, path);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while uploading in drive. Error message: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Upload a file in google drive.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.NAME.getKey())) {
			return "Name of the file " + ReactorKeysEnum.NAME.getKey();
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Path where the file will be uploaded " + ReactorKeysEnum.FILE_PATH.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
