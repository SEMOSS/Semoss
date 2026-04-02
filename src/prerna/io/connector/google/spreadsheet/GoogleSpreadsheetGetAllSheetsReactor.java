package prerna.io.connector.google.spreadsheet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleSpreadsheetGetAllSheetsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetGetAllSheetsReactor.class);

	public GoogleSpreadsheetGetAllSheetsReactor() {

	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		List<Map<String, Object>> spreadsheets = new ArrayList<>();
		try {
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			spreadsheets = GoogleSpreadsheetHelper.fetchSpreadsheetMetadata(accessToken);
			return new NounMetadata(spreadsheets, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred in getting sheet details. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns all spreadsheet titles and sheet names for the authenticated user.";
	}
}
