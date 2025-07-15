package prerna.io.connector.google;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SpreadSheetHelper;

public class GoogleSpreadSheetReadReactor extends AbstractReactor{
	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadSheetReadReactor.class);

	public GoogleSpreadSheetReadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TITLESHEET_NAME.getKey(), ReactorKeysEnum.SHEET_NAME.getKey()};
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String titleSheetName = this.keyValue.get(this.keysToGet[0]);
			String sheetName = this.keyValue.get(this.keysToGet[1]);
			String accessToken = getAccessToken();
			return SpreadSheetHelper.readData(titleSheetName, sheetName, accessToken);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}
	
	private String getAccessToken() {
		String accessToken = null;
		User user = this.insight.getUser();
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			} else {
				AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken = msToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}
		return accessToken;
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is read the data present on Google spread sheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TITLESHEET_NAME.getKey())) {
			return "TitleSheet name of the Google spread sheet" + ReactorKeysEnum.TITLESHEET_NAME.getKey();
		}else if (key.equals(ReactorKeysEnum.SHEET_NAME.getKey())) {
			return "Sheet name from Google spreadsheet" + ReactorKeysEnum.SHEET_NAME.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
