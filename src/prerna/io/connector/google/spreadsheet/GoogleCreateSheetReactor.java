package prerna.io.connector.google.spreadsheet;

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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SpreadSheetHelper;
import prerna.io.connector.google.GoogleLoginUtils;

public class GoogleCreateSheetReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GoogleCreateSheetReactor.class);

	public GoogleCreateSheetReactor() {
		this.keysToGet = new String[] { "titleSheetID", "sheetName", "data" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			this.organizeKeys();
			String titleSheetID = this.keyValue.get(this.keysToGet[0]);
			String sheetName = this.keyValue.get(this.keysToGet[1]);
			String rawData = this.keyValue.get(this.keysToGet[2]);
			ObjectMapper mapper = new ObjectMapper();
			List<List<String>> data = mapper.readValue(rawData, List.class);
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return SpreadSheetHelper.createNewSheet(titleSheetID, sheetName, accessToken, data);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred in creating sheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor will create new sheet in existing google spreadsheet and data in it";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("titleSheetID")) {
			return "TitleSheet id of the Google spread sheet";
		} else if (key.equals("sheetName")) {
			return "Sheet name from Google spreadsheet";
		} else if (key.equals("data")) {
			return "Data to be updated in Google spreadsheet";
		}
		return super.getDescriptionForKey(key);
	}

}
