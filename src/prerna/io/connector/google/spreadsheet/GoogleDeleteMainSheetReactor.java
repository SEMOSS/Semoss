package prerna.io.connector.google.spreadsheet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleDeleteMainSheetReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GoogleDeleteMainSheetReactor.class);

	public GoogleDeleteMainSheetReactor() {
		this.keysToGet = new String[] { "titleSheetID" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			this.organizeKeys();
			String titleSheetID = this.keyValue.get(this.keysToGet[0]);
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return SpreadSheetHelper.deleteTitleSheet(titleSheetID, accessToken);
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
		if (key.equals("titleSheetID")) {
			return "TitleSheet id of the Google spread sheet";
		}
		return super.getDescriptionForKey(key);
	}
}
