package prerna.io.connector.google.spreadsheet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleSpreadsheetReadSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetReadSheetReactor.class);

	private static final String TITLESHEETID = "titleSheetID";
	private static final String SHEETID = "sheetID";

	public GoogleSpreadsheetReadSheetReactor() {
		this.keysToGet = new String[] { TITLESHEETID, SHEETID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String titleSheetID = this.keyValue.get(this.keysToGet[0]);
		if (titleSheetID == null || titleSheetID.isEmpty()) {
			throw new SemossPixelException("Title sheet ID is required to read sheet in spreadsheet");
		}
		String sheetID = this.keyValue.get(this.keysToGet[1]);
		if (sheetID == null || sheetID.isEmpty()) {
			throw new SemossPixelException("Sheet ID is required to read sheet in spreadsheet");
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return SpreadSheetHelper.readData(titleSheetID, sheetID, accessToken);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred in reading sheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to read the data present on Google spread sheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TITLESHEETID)) {
			return "TitleSheet id of the Google spread sheet";
		} else if (key.equals(SHEETID)) {
			return "Sheet id from Google spreadsheet";
		}
		return super.getDescriptionForKey(key);
	}

}
