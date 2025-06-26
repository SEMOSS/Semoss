package prerna.util;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ClearValuesRequest;
import com.google.api.services.sheets.v4.model.ValueRange;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SpreadSheetHelper {

	public static final String SPREADSHEET_DATABASE = "bcdb0a92-2a3b-4c73-bb79-5f5116bd6832";
	public static final String SPREADSHEET_UNIQUE_ID = "id";

	public static NounMetadata writeData(String userId, String rowNo, String colNo, String data, String spreadsheetId,
			String sheetName, Sheets sheetsService) {
		String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
		String range = sheetName + "!" + cell;
		String msg = null;
		ValueRange body = new ValueRange().setValues(Collections.singletonList(Collections.singletonList(data)));
		boolean writeSuccess = write(spreadsheetId, sheetsService, range, body);
		if (Boolean.TRUE.equals(writeSuccess)) {
			msg = data + " written successfully";
		} else {
			msg = data + " not written successfully";
		}

		return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static boolean write(String spreadsheetId, Sheets sheetsService, String range, ValueRange body) {
		try {
			sheetsService.spreadsheets().values().update(spreadsheetId, range, body).setValueInputOption("USER_ENTERED")
					.execute();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static NounMetadata updateData(String userId, String rowNo, String colNo, String data, String spreadsheetId,
			String sheetName, Sheets sheetsService) {
		String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
		String range = sheetName + "!" + cell;
		String msg = null;
		ValueRange body = new ValueRange().setValues(Collections.singletonList(Collections.singletonList(data)));
		boolean updateSuccess = update(spreadsheetId, sheetsService, range, body);
		if (Boolean.TRUE.equals(updateSuccess)) {
			msg = "Data Update successfully";
		} else {
			msg = "Data not updated successfully";
		}
		return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static Boolean update(String spreadsheetId, Sheets sheetsService, String range, ValueRange body) {
		try {
			sheetsService.spreadsheets().values().update(spreadsheetId, range, body).setValueInputOption("USER_ENTERED")
					.execute();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static NounMetadata deleteData(String userId, String rowNo, String colNo, String data, String spreadsheetId,
			String sheetName, Sheets sheetsService) {
		String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
		String range = sheetName + "!" + cell;
		String msg = null;
		boolean deleteSucess = delete(spreadsheetId, sheetsService, range);
		if (Boolean.TRUE.equals(deleteSucess)) {
			msg = data + " deleted successfully";
		} else {
			msg = data + " not deleted successfully";
		}
		return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static boolean delete(String spreadsheetId, Sheets sheetsService, String range) {
		try {
			sheetsService.spreadsheets().values().clear(spreadsheetId, range, new ClearValuesRequest()).execute();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static NounMetadata truncateDataFromDB(String userId) {
		Boolean truncateFlag = false;
		String tableName = null;
		String msg = null;
		try {
			IDatabaseEngine database = Utility.getDatabase(SPREADSHEET_DATABASE);
			long Uid;
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			List<String> checkUserId = checkUserId(userId);
			if (!checkUserId.contains(userId)) {
				msg = "User id " + userId + " is not present in DB";
				return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String truncateQuery = "Delete from " + tableName;
			database.removeData(truncateQuery);
			truncateFlag = true;
			Uid = Long.valueOf(userId);
			if (truncateFlag == true) {
				msg = "Table truncated succesfully by user with user id: " + userId;

			}
			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {

		}
		return null;
	}

	public static NounMetadata readData(String userId, String rowNo, String colNo, String data, String spreadsheetId,
			String sheetName, Sheets sheetsService) {
		String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
		String range = sheetName + "!" + cell;
		String msg = null;
		boolean readSuccess = read(spreadsheetId, sheetsService, range);
		if (Boolean.TRUE.equals(readSuccess)) {
			msg = "Data read successfully";
		} else {
			msg = "Data not read succesfully";
		}
		return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static Boolean read(String spreadsheetId, Sheets sheetsService, String range) {
		try {
			ValueRange response = sheetsService.spreadsheets().values().get(spreadsheetId, range).execute();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static NounMetadata deleteDataForUser(String userId) {
		Boolean truncateFlag = false;
		String msg = null;
		String error = null;
		try {
			String tableName = null;
			long Uid;
			IDatabaseEngine database = Utility.getDatabase(SPREADSHEET_DATABASE);
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			List<String> checkUserId = checkUserId(userId);
			if (!checkUserId.contains(userId)) {
				msg = "User id " + userId + " is not present in DB";
				return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String truncateQuery = "Delete from " + tableName + " WHERE " + SPREADSHEET_UNIQUE_ID + "='" + userId + "'";
			database.removeData(truncateQuery);
			truncateFlag = true;
			Uid = Long.valueOf(userId);
			if (truncateFlag == true) {
				msg = "Record deleted succesfully by user " + userId + " for user id " + userId;

			}
			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
			error = e.getMessage();
		}
		return new NounMetadata("Data not truncated with error message: " + error, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}

	private static List<String> checkUserId(String userId) {
		List<String> userIds = new ArrayList<String>();
		try {
			String tableName = null;
			String userID;
			IDatabaseEngine database = Utility.getDatabase(SPREADSHEET_DATABASE);
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			String query = " SELECT " + SPREADSHEET_UNIQUE_ID + " from " + tableName;
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					userID = rs.getString(SPREADSHEET_UNIQUE_ID);
					userIds.add(userID);
				}
			}
			return userIds;
		} catch (Exception e) {
			return userIds;
		}
	}

}
