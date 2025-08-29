package prerna.io.connector.google.drive;

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

public class GoogleDriveDownloadReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDriveDownloadReactor.class);
	
	private static final String FILE_NAME = "fileName";

	public GoogleDriveDownloadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), FILE_NAME};
		this.keyRequired = new int[] { 1, 1, 0 };
	}
	

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String fileId = this.keyValue.get(this.keysToGet[0]);
		String path = this.keyValue.get(this.keysToGet[1]);
		String fileName = null;
		
		if (this.keyValue.get(this.keysToGet[2]) != null && !this.keyValue.get(this.keysToGet[2]).isEmpty()) {
			fileName = this.keyValue.get(this.keysToGet[2]);
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			boolean result = GoogleDriveHelper.downloadFile(accessToken, fileId, path, fileName);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred while downloading the file. Error message: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Download a file form google drive.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Unique identifier of the Google Drive file to be downloaded " + ReactorKeysEnum.ID.getKey();
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Path where the file will be downloaded " + ReactorKeysEnum.FILE_PATH.getKey();
		}
		return super.getDescriptionForKey(key);
	}
}
