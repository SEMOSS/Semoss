package prerna.io.connector.google.spreadsheet;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleSpreadsheetCreateSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetCreateSheetReactor.class);

	private static final String TITLESHEETID = "titleSheetID";
	private static final String SHEETNAME = "sheetName";
	private static final String DATA = "data";

	public GoogleSpreadsheetCreateSheetReactor() {
		this.keysToGet = new String[] { TITLESHEETID, SHEETNAME, DATA };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String spreadsheetId = this.keyValue.get(this.keysToGet[0]);
		if (spreadsheetId == null || spreadsheetId.trim().isEmpty()) {
			throw new SemossPixelException("Spreadsheet ID is required to create a sheet in a Google spreadsheet");
		}
		String sheetName = this.keyValue.get(this.keysToGet[1]);
		if (sheetName == null || sheetName.trim().isEmpty()) {
			throw new SemossPixelException("Sheet name is required to create a sheet in a Google spreadsheet");
		}
		String rawData = this.keyValue.get(this.keysToGet[2]);
		try {
			User user = this.insight.getUser();
			List<List<String>> data = GoogleSpreadsheetHelper.parseSheetData(rawData);
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return GoogleSpreadsheetHelper.createNewSheet(spreadsheetId, sheetName, accessToken, data);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to create Google sheet '{}' in spreadsheet: {}", sheetName, spreadsheetId, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected failure while creating Google sheet '{}' in spreadsheet: {}", sheetName,
					spreadsheetId, e);
			throw new SemossPixelException("An error occurred in creating sheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor creates a new sheet in an existing Google spreadsheet and optionally loads tabular data into it.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TITLESHEETID)) {
			return "Spreadsheet ID of the Google spreadsheet";
		} else if (key.equals(SHEETNAME)) {
			return "Sheet name to create in the Google spreadsheet";
		} else if (key.equals(DATA)) {
			return "Optional JSON 2D array of row data to write into the newly created Google sheet";
		}
		return super.getDescriptionForKey(key);
	}
}
