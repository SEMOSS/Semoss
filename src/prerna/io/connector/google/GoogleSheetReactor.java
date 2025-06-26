package prerna.io.connector.google;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SheetServiceUtil;
import prerna.util.SpreadSheetHelper;

public class GoogleSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSheetReactor.class);

	private static final String APPLICATION_NAME = "Google Sheets API Java";
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	private static final String SPREADSHEET_ID = "19AETLkT1QNKuI04dOhCntJ6eHprK2LvCoxt1Hxvbg1Q";
	private static final String RANGE = "Sheet1!A1";

	public GoogleSheetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.USERID.getKey(),
				ReactorKeysEnum.ROW_NO.getKey(), ReactorKeysEnum.COLUMN_NO.getKey(), ReactorKeysEnum.DATA.getKey(),
				ReactorKeysEnum.SHEET_NAME.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String command = this.keyValue.get(this.keysToGet[0]);
		String userId = this.keyValue.get(this.keysToGet[1]);
		String rowNo = this.keyValue.get(this.keysToGet[2]);
		String colNo = this.keyValue.get(this.keysToGet[3]);
		String data = this.keyValue.get(this.keysToGet[4]);
		String sheetName = this.keyValue.get(this.keysToGet[5]);
		Sheets sheetsService = null;
		try {
			sheetsService = SheetServiceUtil.getSheetsService();
			switch (command) {
			case "write":
				return SpreadSheetHelper.writeData(userId, rowNo, colNo, data, SPREADSHEET_ID, sheetName,
						sheetsService);

			case "update":
				return SpreadSheetHelper.updateData(userId, rowNo, colNo, data, SPREADSHEET_ID, sheetName,
						sheetsService);

			case "delete":
				return SpreadSheetHelper.deleteData(userId, rowNo, colNo, data, SPREADSHEET_ID, sheetName,
						sheetsService);

			case "read":
				return SpreadSheetHelper.readData(userId, rowNo, colNo, data, SPREADSHEET_ID, sheetName, sheetsService);

			case "truncate all user data":
				return SpreadSheetHelper.truncateDataFromDB(userId);

			case "delete user data for user":
				return SpreadSheetHelper.deleteDataForUser(userId);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Issue with input");
		}
		return new NounMetadata("Please provide valid command", PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}

}
