package prerna.io.connector.google.spreadsheet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleSpreadsheetCreateMasterSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetCreateMasterSheetReactor.class);

	private static final String TITLESHEETNAME = "titleSheetName";

	public GoogleSpreadsheetCreateMasterSheetReactor() {
		this.keysToGet = new String[] { TITLESHEETNAME };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String spreadsheetName = this.keyValue.get(this.keysToGet[0]);
		if (spreadsheetName == null || spreadsheetName.trim().isEmpty()) {
			throw new SemossPixelException("Master sheet name is required to create a Google spreadsheet");
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return GoogleSpreadsheetHelper.createNewSpreadsheet(spreadsheetName, accessToken);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to create Google spreadsheet for name: {}", spreadsheetName, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected failure while creating Google spreadsheet for name: {}", spreadsheetName, e);
			throw new SemossPixelException(
					"An error occurred while creating the Google spreadsheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor creates a new Google spreadsheet for the provided master sheet name";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TITLESHEETNAME)) {
			return "Master sheet name of the Google spreadsheet";
		}
		return super.getDescriptionForKey(key);
	}
}