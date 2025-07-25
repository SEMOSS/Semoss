package prerna.io.connector.google;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SpreadSheetHelper;

public class GoogleCreateSheetReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GoogleCreateSheetReactor.class);

	public GoogleCreateSheetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TITLESHEET_ID.getKey(), ReactorKeysEnum.SHEET_NAME.getKey(),
				ReactorKeysEnum.DATA.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String titleSheetID = this.keyValue.get(this.keysToGet[0]);
			String sheetName = this.keyValue.get(this.keysToGet[1]);
			String rawData = this.keyValue.get(this.keysToGet[2]);
			ObjectMapper mapper = new ObjectMapper();
			List<List<String>> data = mapper.readValue(rawData, List.class);
			String accessToken = getAccessToken();
			return SpreadSheetHelper.createNewSheet(titleSheetID, sheetName, accessToken, data);
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
		return "This reactor will create new sheet in existing google spreadsheet and data in it";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TITLESHEET_NAME.getKey())) {
			return "TitleSheet name of the Google spread sheet" + ReactorKeysEnum.TITLESHEET_NAME.getKey();
		} else if (key.equals(ReactorKeysEnum.SHEET_NAME.getKey())) {
			return "Sheet name from Google spreadsheet" + ReactorKeysEnum.SHEET_NAME.getKey();
		} else if (key.equals(ReactorKeysEnum.DATA.getKey())) {
			return "Data to be updated in Google spreadsheet" + ReactorKeysEnum.DATA.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
