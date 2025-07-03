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

	public GoogleSheetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.USERID.getKey(),
				ReactorKeysEnum.ROW_NO.getKey(), ReactorKeysEnum.COLUMN_NO.getKey(), ReactorKeysEnum.DATA.getKey(),
				ReactorKeysEnum.SHEET_NAME.getKey(), ReactorKeysEnum.SPREADSHEET_ID.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
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
		String spreadSheetId = this.keyValue.get(this.keysToGet[6]);
		if (this.keyValue.get(this.keysToGet[2]) != null && this.keyValue.get(this.keysToGet[2]) != "") {
			rowNo = this.keyValue.get(this.keysToGet[2]);
		}
		if (this.keyValue.get(this.keysToGet[3]) != null && this.keyValue.get(this.keysToGet[3]) != "") {
			colNo = this.keyValue.get(this.keysToGet[3]);
		}
		if (this.keyValue.get(this.keysToGet[4]) != null && this.keyValue.get(this.keysToGet[4]) != "") {
			data = this.keyValue.get(this.keysToGet[4]);
		}
		if (this.keyValue.get(this.keysToGet[5]) != null && this.keyValue.get(this.keysToGet[5]) != "") {
			sheetName = this.keyValue.get(this.keysToGet[5]);
		}
		if (this.keyValue.get(this.keysToGet[6]) != null && this.keyValue.get(this.keysToGet[6]) != "") {
			spreadSheetId = this.keyValue.get(this.keysToGet[6]);
		}
		Sheets sheetsService = null;
		try {
			sheetsService = SheetServiceUtil.getSheetsService();
			switch (command.trim().toLowerCase()) {
			case "write":
				return SpreadSheetHelper.writeData(userId, rowNo, colNo, data, spreadSheetId, sheetName, sheetsService);
			case "update":
				return SpreadSheetHelper.updateData(userId, rowNo, colNo, data, spreadSheetId, sheetName,
						sheetsService);
			case "delete":
				return SpreadSheetHelper.deleteData(userId, rowNo, colNo, spreadSheetId, sheetName,
						sheetsService);
			case "read":
				return SpreadSheetHelper.readData(userId, rowNo, colNo, spreadSheetId, sheetName, sheetsService);

			case "truncate all user data":
				return SpreadSheetHelper.truncateDataFromDB(userId);

			case "delete user data for user":
				return SpreadSheetHelper.deleteDataForUser(userId);

			case "delete sheet":
				return SpreadSheetHelper.deleteSheet(userId, spreadSheetId, sheetName, sheetsService);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Issue with input");
		}
		return new NounMetadata("Please provide valid command", PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}

}
