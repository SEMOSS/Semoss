package prerna.io.connector.google.spreadsheet;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SpreadSheetHelper;

public class GoogleUpdateSheetReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GoogleUpdateSheetReactor.class);

	public GoogleUpdateSheetReactor() {
		this.keysToGet = new String[] { "titleSheetID", "SheetID", "data" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			this.organizeKeys();
			String titleSheetID = this.keyValue.get(this.keysToGet[0]);
			String sheetID = this.keyValue.get(this.keysToGet[1]);
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			String rawData = this.keyValue.get(this.keysToGet[2]);
			ObjectMapper mapper = new ObjectMapper();
			List<List<String>> data = mapper.readValue(rawData, List.class);
			return SpreadSheetHelper.updateData(titleSheetID, sheetID, data, accessToken);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred in updating sheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used tp update Sheet in Google spread sheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("titleSheetID")) {
			return "TitleSheet name of the Google spread sheet";
		} else if (key.equals("sheetName")) {
			return "Sheet name from Google spreadsheet";
		} else if (key.equals("data")) {
			return "Data to be updated in Google spreadsheet";
		}
		return super.getDescriptionForKey(key);
	}

}
