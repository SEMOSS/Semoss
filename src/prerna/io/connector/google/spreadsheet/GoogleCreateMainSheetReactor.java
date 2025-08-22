package prerna.io.connector.google.spreadsheet;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SpreadSheetHelper;

public class GoogleCreateMainSheetReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(GoogleCreateMainSheetReactor.class);

	public GoogleCreateMainSheetReactor() {
		this.keysToGet = new String[] { "titleSheetName" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			this.organizeKeys();
			String titleSheetName = this.keyValue.get(this.keysToGet[0]);
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			return SpreadSheetHelper.createNewSpreadSheet(titleSheetName, accessToken);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred in creating main spreadsheet. Error message: " + e.getMessage());
		}

	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to create a new google spreadsheet";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("titleSheetName")) {
			return "TitleSheet name of the Google spread sheet";
		}
		return super.getDescriptionForKey(key);
	}

}
