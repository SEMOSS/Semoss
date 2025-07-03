package prerna.util;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.ClearValuesRequest;
import com.google.api.services.sheets.v4.model.DeleteSheetRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SpreadSheetHelper {

	public static final String SPREADSHEET_DATABASE = "6abf12ab-ae96-4edd-a1af-b56b9a37634d";
	public static final String SPREADSHEET_UNIQUE_ID = "id";

	private static final Logger classLogger = LogManager.getLogger(SpreadSheetHelper.class);

	public static NounMetadata writeData(String userId, String rowNo, String colNo, String data, String spreadsheetId,
			String sheetName, Sheets sheetsService) {
		String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
		String range = sheetName + "!" + cell;
		String msg = null;
		ValueRange body = new ValueRange().setValues(Collections.singletonList(Collections.singletonList(data)));
		String missingFields = findMissingFields(rowNo, colNo, data, spreadsheetId, sheetName);
		if (!missingFields.isEmpty()) {
			return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
		boolean writeSuccess = write(spreadsheetId, sheetsService, range, body);
		if (Boolean.TRUE.equals(writeSuccess)) {
			msg = "Data written successfully";
		} else {
			msg = "Data not written successfully";
		}

		return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static String findMissingFields(String rowNo, String colNo, String data, String spreadsheetId,
			String sheetName) {
		StringBuilder errorBuilder = new StringBuilder();
		if (rowNo == null || rowNo.isEmpty()) {
			errorBuilder.append("rowNo, ");
		}
		if (colNo == null || colNo.isEmpty()) {
			errorBuilder.append("colNo, ");
		}
		if (data == null || data.isEmpty()) {
			errorBuilder.append("data, ");
		}
		if (sheetName == null || sheetName.isEmpty()) {
			errorBuilder.append("sheetName, ");
		}
		if (spreadsheetId == null || spreadsheetId.isEmpty()) {
			errorBuilder.append("spreadsheetId, ");
		}
		String error = errorBuilder.toString().replaceAll("$", "");
		return error;
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
		String missingFields = findMissingFields(rowNo, colNo, data, spreadsheetId, sheetName);
		if (!missingFields.isEmpty()) {
			return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
		boolean updateSuccess = update(spreadsheetId, sheetsService, range, body);
		if (Boolean.TRUE.equals(updateSuccess)) {
			msg = "Data Updated successfully";
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

	public static NounMetadata deleteData(String userId, String rowNo, String colNo, String spreadsheetId,
			String sheetName, Sheets sheetsService) {
		String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
		String range = sheetName + "!" + cell;
		String msg = null;
		String missingFields = findMissingFields(rowNo, colNo,"not required", spreadsheetId, sheetName);
		if (!missingFields.isEmpty()) {
			return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
		boolean deleteSucess = delete(spreadsheetId, sheetsService, range);
		if (Boolean.TRUE.equals(deleteSucess)) {
			msg = "Data deleted successfully";
		} else {
			msg = "Data not deleted successfully";
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
			if (truncateFlag == true) {
				msg = "Table truncated succesfully by user with user id: " + userId;
			}
			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			msg = e.getMessage();
		}
		return new NounMetadata("Data not truncated with error message: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}

	public static NounMetadata readData(String userId, String rowNo, String colNo, String spreadsheetId,
			String sheetName, Sheets sheetsService) {
		String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
		String range = sheetName + "!" + cell;
		Object msg = null;
		String missingFields = findMissingFields(rowNo, colNo, "not required", spreadsheetId, sheetName);
		if (!missingFields.isEmpty()) {
			return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
		HashMap<Object, Boolean> hashMap = read(spreadsheetId, sheetsService, range);
		Entry<Object, Boolean> dataMap = hashMap.entrySet().iterator().next();
		Object dataObject = dataMap.getKey();
		Boolean flagValue = dataMap.getValue();
		if (Boolean.TRUE.equals(flagValue)) {
			msg = dataObject;
		} else {
			msg = "Data not read succesfully";
		}
		return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private static HashMap<Object, Boolean> read(String spreadsheetId, Sheets sheetsService, String range) {
		Boolean flag = false;
		HashMap<Object, Boolean> dataMap = new HashMap<Object, Boolean>();
		try {
			ValueRange response = sheetsService.spreadsheets().values().get(spreadsheetId, range).execute();
			List<List<Object>> values = response.getValues();
			String actualValue = (values != null) && !values.isEmpty() && !values.get(0).isEmpty()
					? values.get(0).get(0).toString()
					: "cell is empty";
			flag = true;
			dataMap.put(actualValue, flag);
		} catch (IOException e) {
			e.printStackTrace();
			dataMap.put("Data read successfully", flag);

		}
		return dataMap;
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

	public static NounMetadata deleteSheet(String userId, String spreadsheetId, String sheetName,
			Sheets sheetsService) {
		try {
			Spreadsheet spreadsheet = sheetsService.spreadsheets().get(spreadsheetId).execute();
			List<Sheet> sheets = spreadsheet.getSheets();
			Integer sheetIdToDelete = null;
			for (Sheet sheet : sheets) {
				if (sheet.getProperties().getTitle().equals(sheetName)) {
					sheetIdToDelete = sheet.getProperties().getSheetId();
					break;
				}
			}
			if (sheetIdToDelete == null) {
				String msg = "Sheet not found:";
				return new NounMetadata(msg + sheetIdToDelete, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}

			if (sheets.size() <= 1) {
				String msg = "Cannot delete the only remaining sheet";
				return new NounMetadata(msg + sheetIdToDelete, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			Request deleteSheetRequest = new Request()
					.setDeleteSheet(new DeleteSheetRequest().setSheetId(sheetIdToDelete));
			BatchUpdateSpreadsheetRequest batchUpdateSpreadsheetRequest = new BatchUpdateSpreadsheetRequest()
					.setRequests(java.util.Arrays.asList(deleteSheetRequest));
			sheetsService.spreadsheets().batchUpdate(spreadsheetId, batchUpdateSpreadsheetRequest).execute();
			return new NounMetadata("Sheet deleted successfully", PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in the class SpreadSheetHelper: " + e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
	}

}
