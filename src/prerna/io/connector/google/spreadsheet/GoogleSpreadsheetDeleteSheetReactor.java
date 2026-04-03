package prerna.io.connector.google.spreadsheet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleSpreadsheetDeleteSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetDeleteSheetReactor.class);

	private static final String TITLESHEETID = "titleSheetID";
	private static final String SHEETID = "sheetID";

	public GoogleSpreadsheetDeleteSheetReactor() {
		this.keysToGet = new String[] { TITLESHEETID, SHEETID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String spreadsheetId = this.keyValue.get(this.keysToGet[0]);
		if (spreadsheetId == null || spreadsheetId.trim().isEmpty()) {
			throw new SemossPixelException("Spreadsheet ID is required to delete a sheet in a Google spreadsheet");
		}
		String sheetId = this.keyValue.get(this.keysToGet[1]);
		if (sheetId == null || sheetId.trim().isEmpty()) {
			throw new SemossPixelException("Sheet ID is required to delete a sheet in a Google spreadsheet");
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return GoogleSpreadsheetHelper.deleteSheet(spreadsheetId, sheetId, accessToken);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to delete Google sheet {} from spreadsheet: {}", sheetId, spreadsheetId, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected failure while deleting Google sheet {} from spreadsheet: {}", sheetId,
					spreadsheetId, e);
			throw new SemossPixelException("An error occurred in deleting sheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor deletes a sheet from a Google spreadsheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TITLESHEETID)) {
			return "Spreadsheet ID of the Google spreadsheet";
		} else if (key.equals(SHEETID)) {
			return "Sheet ID from the Google spreadsheet";
		}
		return super.getDescriptionForKey(key);
	}
}
