package prerna.io.connector.google.spreadsheet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleSpreadsheetDeleteMainSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetDeleteMainSheetReactor.class);

	private static final String TITLESHEETID = "titleSheetID";

	public GoogleSpreadsheetDeleteMainSheetReactor() {
		this.keysToGet = new String[] { TITLESHEETID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String titleSheetID = this.keyValue.get(this.keysToGet[0]);
		if (titleSheetID == null || titleSheetID.isEmpty()) {
			throw new SemossPixelException("Title sheet ID is required to delete main spreadsheet");
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return SpreadSheetHelper.deleteTitleSheet(titleSheetID, accessToken);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred in deleting main spreadsheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to delete google spreadsheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TITLESHEETID)) {
			return "TitleSheet id of the Google spread sheet";
		}
		return super.getDescriptionForKey(key);
	}
}
