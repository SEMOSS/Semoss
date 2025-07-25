package prerna.io.connector.google;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SpreadSheetHelper;

public class GoogleDeleteSheetReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GoogleDeleteSheetReactor.class);

	public GoogleDeleteSheetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TITLESHEET_ID.getKey(), ReactorKeysEnum.SHEET_ID.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String titleSheetID = this.keyValue.get(this.keysToGet[0]);
			String sheetID = this.keyValue.get(this.keysToGet[1]);
			String accessToken = getAccessToken();
			return SpreadSheetHelper.deleteSheet(titleSheetID, sheetID, accessToken);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	private String getAccessToken() {
		String accessToken = null;
		User user = this.insight.getUser();
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				classLogger.error("user can not be null");
				throwLoginError(retMap);
			} else {
				AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken = msToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			classLogger.error("Error while getting access token");
			throwLoginError(retMap);
		}
		return accessToken;
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to delete sheet for google spreadsheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TITLESHEET_ID.getKey())) {
			return "TitleSheet id of the Google spread sheet" + ReactorKeysEnum.TITLESHEET_ID.getKey();
		} else if (key.equals(ReactorKeysEnum.SHEET_ID.getKey())) {
			return "Sheet id from Google spreadsheet" + ReactorKeysEnum.SHEET_ID.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
