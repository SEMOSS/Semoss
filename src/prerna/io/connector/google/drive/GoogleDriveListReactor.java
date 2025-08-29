package prerna.io.connector.google.drive;

import java.util.List;
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

public class GoogleDriveListReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(GoogleDriveListReactor.class);

	public GoogleDriveListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String limitStr = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			int limit = Integer.parseInt(limitStr);
			List<Map<String, Object>> result = GoogleDriveHelper.fileIdList(accessToken, limit);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch(SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred getting the files. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Get the list of files from google drive";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The limit for the maximum number of files";
		}
		return super.getDescriptionForKey(key);
	}
}
