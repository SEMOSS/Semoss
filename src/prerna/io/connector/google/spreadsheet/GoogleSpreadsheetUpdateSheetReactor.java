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

public class GoogleSpreadsheetUpdateSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetUpdateSheetReactor.class);

	private static final String TITLESHEETID = "titleSheetID";
	private static final String SHEETID = "sheetID";
	private static final String DATA = "data";

	public GoogleSpreadsheetUpdateSheetReactor() {
		this.keysToGet = new String[] { TITLESHEETID, SHEETID, DATA };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String spreadsheetId = this.keyValue.get(this.keysToGet[0]);
		if (spreadsheetId == null || spreadsheetId.trim().isEmpty()) {
			throw new SemossPixelException("Spreadsheet ID is required to update a sheet in a Google spreadsheet");
		}
		String sheetID = this.keyValue.get(this.keysToGet[1]);
		if (sheetID == null || sheetID.trim().isEmpty()) {
			throw new SemossPixelException("Sheet ID is required to update a sheet in a Google spreadsheet");
		}
		String rawData = this.keyValue.get(this.keysToGet[2]);
		if (rawData == null || rawData.isEmpty()) {
			throw new SemossPixelException("Data is required to update a sheet in a Google spreadsheet");
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			ObjectMapper mapper = new ObjectMapper();
			List<List<String>> data = mapper.readValue(rawData, List.class);
			return GoogleSpreadsheetHelper.updateData(spreadsheetId, sheetID, data, accessToken);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred in updating sheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor updates data in a Google spreadsheet sheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TITLESHEETID)) {
			return "Spreadsheet ID of the Google spreadsheet";
		} else if (key.equals(SHEETID)) {
			return "Sheet ID from the Google spreadsheet";
		} else if (key.equals(DATA)) {
			return "Data to update in the Google spreadsheet";
		}
		return super.getDescriptionForKey(key);
	}
}
