package prerna.io.connector.google.spreadsheet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleSpreadsheetCreateMainSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetCreateMainSheetReactor.class);

	private static final String TITLESHEETNAME = "titleSheetName";

	public GoogleSpreadsheetCreateMainSheetReactor() {
		this.keysToGet = new String[] { TITLESHEETNAME };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String titleSheetName = this.keyValue.get(this.keysToGet[0]);
		if (titleSheetName == null || titleSheetName.isEmpty()) {
			throw new SemossPixelException("Title sheet name is required to create main spreadsheet");
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return SpreadSheetHelper.createNewSpreadSheet(titleSheetName, accessToken);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred creating main spreadsheet. Error message: " + e.getMessage());
		}

	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to create a new google spreadsheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TITLESHEETNAME)) {
			return "TitleSheet name of the Google spread sheet";
		}
		return super.getDescriptionForKey(key);
	}
}
