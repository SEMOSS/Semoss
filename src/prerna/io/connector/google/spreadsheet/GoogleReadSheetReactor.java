package prerna.io.connector.google.spreadsheet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SpreadSheetHelper;

public class GoogleReadSheetReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GoogleReadSheetReactor.class);

	public GoogleReadSheetReactor() {
		this.keysToGet = new String[] { "titleSheetID", "SheetID" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			this.organizeKeys();
			String titleSheetID = this.keyValue.get(this.keysToGet[0]);
			String sheetID = this.keyValue.get(this.keysToGet[1]);
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return SpreadSheetHelper.readData(titleSheetID, sheetID, accessToken);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred in reading sheet. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is read the data present on Google spread sheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("titleSheetID")) {
			return "TitleSheet id of the Google spread sheet";
		} else if (key.equals("SheetID")) {
			return "Sheet id from Google spreadsheet";
		}
		return super.getDescriptionForKey(key);
	}

}
