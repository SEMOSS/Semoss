package prerna.io.connector.google.spreadsheet;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleSpreadsheetUpdateSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetUpdateSheetReactor.class);

	private static final String TITLESHEETID = "titleSheetID";
	private static final String SHEETID = "sheetID";
	private static final String DATA = "data";

	public GoogleSpreadsheetUpdateSheetReactor() {
		this.keysToGet = new String[] { TITLESHEETID, SHEETID, DATA };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String spreadsheetId = this.keyValue.get(this.keysToGet[0]);
		if (spreadsheetId == null || spreadsheetId.trim().isEmpty()) {
			throw new SemossPixelException("Spreadsheet ID is required to update a sheet in a Google spreadsheet");
		}
		String sheetId = this.keyValue.get(this.keysToGet[1]);
		if (sheetId == null || sheetId.trim().isEmpty()) {
			throw new SemossPixelException("Sheet ID is required to update a sheet in a Google spreadsheet");
		}
		String rawData = this.keyValue.get(this.keysToGet[2]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			List<List<String>> data = GoogleSpreadsheetHelper.parseSheetData(rawData);
			return GoogleSpreadsheetHelper.updateData(spreadsheetId, sheetId, data, accessToken);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to update Google sheet {} in spreadsheet: {}", sheetId, spreadsheetId, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected failure while updating Google sheet {} in spreadsheet: {}", sheetId,
					spreadsheetId, e);
			throw new SemossPixelException("An error occurred in updating sheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor replaces the data in a Google spreadsheet sheet with the provided tabular payload.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TITLESHEETID)) {
			return "Spreadsheet ID of the Google spreadsheet";
		} else if (key.equals(SHEETID)) {
			return "Sheet ID from the Google spreadsheet";
		} else if (key.equals(DATA)) {
			return "Optional JSON 2D array of row data to write into the Google spreadsheet; an empty payload clears the sheet";
		}
		return super.getDescriptionForKey(key);
	}
}
