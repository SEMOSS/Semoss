package prerna.io.connector.google.spreadsheet;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;

public class SpreadSheetHelper {

	private static final String SPREADDRIVE_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'&fields=files(id,name)";
	private static final String SHEET_URL = "https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String GOOGLEDRIVE_URL = "https://www.googleapis.com/drive/v3/files/";
	private static final String TITLESHEET_NAMEURL = "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false";
	private static final Logger classLogger = LogManager.getLogger(SpreadSheetHelper.class);

	/**
	 * Converts column and row indices to a cell reference (e.g., 0,0 -> A1).
	 *
	 * @param col The column index (0-based).
	 * @param row The row index (0-based).
	 * @return The cell reference in A1 notation.
	 */
	private static String getCellReference(int col, int row) {
		StringBuilder colRef = new StringBuilder();
		int tempCol = col;
		do {
			colRef.insert(0, (char) ('A' + (tempCol % 26)));
			tempCol = tempCol / 26 - 1;
		} while (tempCol >= 0);
		return colRef.toString() + (row + 1);
	}

	/**
	 * Retrieves the spreadsheet ID for a given spreadsheet title.
	 *
	 * @param titleSheetName The title of the spreadsheet.
	 * @param accessToken    The Google API OAuth2 access token.
	 * @return The spreadsheet ID as a String, or an error message if not found.
	 */
	private static String getSpreadsheetIdByTitle(String titleSheetName, String accessToken) {
		try {
			String query = "name='" + titleSheetName + "' and mimeType='application/vnd.google-apps.spreadsheet'";
			String encodedQuery = URLEncoder.encode(query, "UTF-8");
			String urlStr = "https://www.googleapis.com/drive/v3/files" + "?q=" + encodedQuery
					+ "&fields=files(id,name)&spaces=drive";
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			String response = HttpHelperUtility.getRequest(urlStr, headers, null, null, null);
			JSONObject jsonResponse = new JSONObject(response);
			JSONArray files = jsonResponse.optJSONArray("files");
			if (files != null && files.length() > 0) {
				return files.getJSONObject(0).getString("id");
			} else {
				return "No spreadsheet found with that title";
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Updates all data in a sheet (by sheetId) in a Google Spreadsheet. Clears the
	 * sheet before writing new data.
	 *
	 * @param titlesheetid The spreadsheet ID.
	 * @param sheetId      The sheet/tab ID (as String).
	 * @param data         The data to write (List of rows, each a List of Strings).
	 * @param accessToken  The Google API OAuth2 access token.
	 * @return NounMetadata with {"success": true/false, "error": "..."} as result.
	 */
	public static NounMetadata updateData(String titlesheetid, String sheetId, List<List<String>> data,
			String accessToken) {
		try {
			// 1. Get the sheet name from the sheetId
			String metaUrl = SHEET_URL + titlesheetid + "?fields=sheets(properties(sheetId,title))";
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			String metaResponse = HttpHelperUtility.getRequest(metaUrl, headers, null, null, null);
			JSONObject metaJson = new JSONObject(metaResponse);
			JSONArray sheets = metaJson.getJSONArray("sheets");
			String sheetName = null;
			for (int i = 0; i < sheets.length(); i++) {
				JSONObject properties = sheets.getJSONObject(i).getJSONObject("properties");
				if (String.valueOf(properties.getInt("sheetId")).equals(sheetId)) {
					sheetName = properties.getString("title");
					break;
				}
			}
			Map<String, Object> response = new HashMap<>();
			if (sheetName == null) {
				response.put("success", false);
				response.put("error", "Sheet ID not found in spreadsheet.");
				return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			// 2. Clear the entire sheet first
			String clearRange = sheetName;
			String clearUrl = SHEET_URL + titlesheetid + "/values/" + URLEncoder.encode(clearRange, "UTF-8") + ":clear";
			HttpHelperUtility.postRequestStringBody(clearUrl, headers, "", null, null, null, null);
			// 3. Write new data starting from A1
			int numRows = data.size();
			int numCols = numRows > 0 ? data.get(0).size() : 0;
			String startCell = "A1";
			String endCell = getCellReference(numCols - 1, numRows - 1);
			String range = sheetName + "!" + startCell;
			if (numRows > 1 || numCols > 1) {
				range += ":" + endCell;
			}
			String url = SHEET_URL + titlesheetid + "/values/" + URLEncoder.encode(range, "UTF-8")
					+ "?valueInputOption=USER_ENTERED";
			JSONObject body = new JSONObject();
			body.put("values", new JSONArray(data));
			HttpHelperUtility.putRequestStringBody(url, headers, body.toString(), null, null, null, null);

			response.put("success", true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Reads all data from a given sheet in a Google Spreadsheet.
	 *
	 * @param spreadsheetId The spreadsheet ID.
	 * @param sheetId       The sheet/tab ID (as String).
	 * @param accessToken   The Google API OAuth2 access token.
	 * @return NounMetadata containing the sheet data as a List<List<String>>.
	 */
	public static NounMetadata readData(String spreadsheetId, String sheetId, String accessToken) {
		try {
			// 1. Get the sheet name from the sheetId
			String metaUrl = SHEET_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			String metaResponse = HttpHelperUtility.getRequest(metaUrl, headers, null, null, null);
			JSONObject metaJson = new JSONObject(metaResponse);
			JSONArray sheets = metaJson.getJSONArray("sheets");
			String sheetName = null;
			boolean sheetIdFound = false;
			for (int i = 0; i < sheets.length(); i++) {
				JSONObject properties = sheets.getJSONObject(i).getJSONObject("properties");
				if (String.valueOf(properties.getInt("sheetId")).equals(sheetId)) {
					sheetName = properties.getString("title");
					sheetIdFound = true;
					break;
				}
			}
			if (!sheetIdFound) {
				return new NounMetadata(
						"Error: The provided sheetId (" + sheetId + ") does not exist in spreadsheetId ("
								+ spreadsheetId + ").",
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			// 2. Read the data using the sheet name
			String url = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(sheetName, "UTF-8");
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			JSONObject json = new JSONObject(response);
			JSONArray values = json.optJSONArray("values");
			List<List<String>> result = new ArrayList<>();
			if (values != null) {
				for (int i = 0; i < values.length(); i++) {
					JSONArray row = values.getJSONArray(i);
					List<String> rowList = new ArrayList<>();
					for (int j = 0; j < row.length(); j++) {
						rowList.add(row.getString(j));
					}
					result.add(rowList);
				}
			}
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Deletes a specific sheet/tab from a Google Spreadsheet.
	 *
	 * @param titlesheetid The spreadsheet ID.
	 * @param sheetId      The sheet/tab ID (as String).
	 * @param accessToken  The Google API OAuth2 access token.
	 * @return NounMetadata with {"success": true/false, "error": "..."} as result.
	 */
	public static NounMetadata deleteSheet(String titlesheetid, String sheetId, String accessToken) {
		try {
			JSONObject deleteSheetRequest = new JSONObject();
			deleteSheetRequest.put("deleteSheet", new JSONObject().put("sheetId", Integer.parseInt(sheetId)));
			JSONArray requests = new JSONArray();
			requests.put(deleteSheetRequest);
			JSONObject body = new JSONObject();
			body.put("requests", requests);
			String batchUpdateUrl = SHEET_URL + titlesheetid + ":batchUpdate";
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			headers.put("Content-Type", "application/json; charset=UTF-8");
			HttpHelperUtility.postRequestStringBody(batchUpdateUrl, headers, body.toString(), null, null, null, null);

			Map<String, Object> response = new HashMap<>();
			response.put("success", true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Retrieves a list of all spreadsheet IDs accessible by the user.
	 *
	 * @param accessToken The Google API OAuth2 access token.
	 * @return List of spreadsheet IDs as Strings.
	 */
	public static List<String> getSpreadSheetId(String accessToken) {
		List<String> spreadsheetIds = new ArrayList<>();
		try {
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			String response = HttpHelperUtility.getRequest(SPREADDRIVE_URL, headers, null, null, null);
			JSONObject json = new JSONObject(response);
			JSONArray files = json.getJSONArray("files");
			for (int i = 0; i < files.length(); i++) {
				JSONObject file = files.getJSONObject(i);
				String spreadsheetId = file.getString("id");
				spreadsheetIds.add(spreadsheetId);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return spreadsheetIds;
	}

	/**
	 * Retrieves all sheet/tab names for the given list of spreadsheet IDs.
	 *
	 * @param accessToken    The Google API OAuth2 access token.
	 * @param spreadSheetIds List of spreadsheet IDs.
	 * @return List of sheet/tab names as Strings.
	 */
	public static List<String> getSheetName(String accessToken, List<String> spreadSheetIds) {
		List<String> allSheetNames = new ArrayList<>();
		try {
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			for (String spreadsheetId : spreadSheetIds) {
				String sheetsUrl = SHEET_URL + spreadsheetId;
				String response = HttpHelperUtility.getRequest(sheetsUrl, headers, null, null, null);
				JSONObject sheetsJson = new JSONObject(response);
				JSONArray sheets = sheetsJson.getJSONArray("sheets");
				for (int j = 0; j < sheets.length(); j++) {
					JSONObject sheet = sheets.getJSONObject(j).getJSONObject("properties");
					String sheetTitle = sheet.getString("title");
					allSheetNames.add(sheetTitle);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return allSheetNames;
	}

	/**
	 * Creates a new Google Spreadsheet with the given title, if it does not already
	 * exist.
	 *
	 * @param titleSheetName The desired spreadsheet title.
	 * @param accessToken    The Google API OAuth2 access token.
	 * @return NounMetadata containing a SpreadSheetResponse object with
	 *         spreadsheetId, sheetId, and status.
	 */
	public static NounMetadata createNewSpreadSheet(String titleSheetName, String accessToken) {
		try {
			SpreadSheetResponse resp = new SpreadSheetResponse();
			if (!validateTitleSheetName(titleSheetName, accessToken)) {
				resp.setSuccess(false);
				resp.setTitleSheetID(null);
				resp.setSheetID(null);
				return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String url = SHEET_URL;
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			headers.put("Content-Type", "application/json; charset=UTF-8");
			JSONObject properties = new JSONObject();
			properties.put("title", titleSheetName);
			JSONObject body = new JSONObject();
			body.put("properties", properties);
			String response = HttpHelperUtility.postRequestStringBody(url, headers, body.toString(), null, null, null,
					null);
			JSONObject jsonResponse = new JSONObject(response);
			if (jsonResponse.has("spreadsheetId")) {
				String spreadsheetId = jsonResponse.optString("spreadsheetId", "");
				String sheetId = null;
				if (jsonResponse.has("sheets")) {
					JSONArray sheets = jsonResponse.getJSONArray("sheets");
					if (sheets.length() > 0) {
						JSONObject firstSheet = sheets.getJSONObject(0);
						JSONObject propertiesObj = firstSheet.optJSONObject("properties");
						if (propertiesObj != null) {
							sheetId = String.valueOf(propertiesObj.optInt("sheetId", -1));
						}
					}
					resp.setTitleSheetID(spreadsheetId);
					resp.setSheetID(sheetId);
					resp.setSuccess(true);
				}
			} else {
				resp.setSuccess(false);
				resp.setTitleSheetID(null);
				resp.setSheetID(null);
			}
			return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Validates whether a spreadsheet with the given title already exists.
	 *
	 * @param titleSheetName The spreadsheet title to check.
	 * @param accessToken    The Google API OAuth2 access token.
	 * @return true if the title is available, false if it already exists.
	 */
	private static boolean validateTitleSheetName(String titleSheetName, String accessToken) {
		try {
			String query = TITLESHEET_NAMEURL;
			String encodedQuery = URLEncoder.encode(query, "UTF-8");
			String urlStr = "https://www.googleapis.com/drive/v3/files" + "?q=" + encodedQuery
					+ "&fields=files(name)&spaces=drive";
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			String response = HttpHelperUtility.getRequest(urlStr, headers, null, null, null);
			JSONObject jsonResponse = new JSONObject(response);
			JSONArray files = jsonResponse.optJSONArray("files");
			if (files != null) {
				for (int i = 0; i < files.length(); i++) {
					JSONObject file = files.getJSONObject(i);
					if (file.getString("name").equalsIgnoreCase(titleSheetName)) {
						return false;
					}
				}
			}
			return true;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Creates a new sheet/tab in an existing spreadsheet and writes data to it.
	 * Throws an error if a sheet with the same name already exists.
	 *
	 * @param titlesheetid The spreadsheet ID.
	 * @param sheetName    The new sheet/tab name.
	 * @param accessToken  The Google API OAuth2 access token.
	 * @param data         The data to write (List of rows, each a List of Strings).
	 * @return NounMetadata containing a SpreadSheetResponse object with
	 *         spreadsheetId, sheetId, and status.
	 */
	public static NounMetadata createNewSheet(String titlesheetid, String sheetName, String accessToken,
			List<List<String>> data) {
		SpreadSheetResponse resp = new SpreadSheetResponse();
		try {
			if (titlesheetid == null || titlesheetid.isEmpty()) {
				resp.setSuccess(false);
				resp.setTitleSheetID(null);
				resp.setSheetID(null);
				return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			// 1. Get spreadsheet metadata to check for existing sheet
			String getUrl = SHEET_URL + titlesheetid + "?fields=sheets.properties";
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			String response = HttpHelperUtility.getRequest(getUrl, headers, null, null, null);
			JSONObject jsonResponse = new JSONObject(response);
			JSONArray sheets = jsonResponse.optJSONArray("sheets");
			if (sheets != null) {
				for (int i = 0; i < sheets.length(); i++) {
					JSONObject properties = sheets.getJSONObject(i).getJSONObject("properties");
					if (sheetName.equals(properties.optString("title"))) {
						throw new SemossPixelException(NounMetadata.getErrorNounMessage("A sheet with the name '"
								+ sheetName + "' already exists. Two sheets cannot have the same name."));
					}
				}
			}
			// 2. Create the new sheet
			String batchUpdateUrl = SHEET_URL + titlesheetid + ":batchUpdate";
			headers.put("Content-Type", "application/json; charset=UTF-8");
			JSONObject addSheetRequest = new JSONObject();
			JSONObject sheetProperties = new JSONObject();
			sheetProperties.put("title", sheetName);
			addSheetRequest.put("addSheet", new JSONObject().put("properties", sheetProperties));
			JSONArray requests = new JSONArray();
			requests.put(addSheetRequest);
			JSONObject body = new JSONObject();
			body.put("requests", requests);
			String batchResponse = HttpHelperUtility.postRequestStringBody(batchUpdateUrl, headers, body.toString(),
					null, null, null, null);
			jsonResponse = new JSONObject(batchResponse);
			String sheetId = null;
			if (jsonResponse.has("replies")) {
				JSONArray replies = jsonResponse.getJSONArray("replies");
				if (replies.length() > 0) {
					JSONObject addSheetReply = replies.getJSONObject(0).optJSONObject("addSheet");
					if (addSheetReply != null && addSheetReply.has("properties")) {
						sheetId = String.valueOf(addSheetReply.getJSONObject("properties").optInt("sheetId", -1));
					}
				}
			}
			resp.setTitleSheetID(titlesheetid);
			resp.setSheetID(sheetId);
			resp.setSuccess(true);
			// Wait for sheet to be available
			Thread.sleep(1000);
			// 3. Add data to the sheet
			int numRows = data.size();
			int numCols = numRows > 0 ? data.get(0).size() : 0;
			String startCell = "A1";
			String endCell = getCellReference(numCols - 1, numRows - 1);
			String range = sheetName + "!" + startCell;
			if (numRows > 1 || numCols > 1) {
				range += ":" + endCell;
			}
			String updateUrl = SHEET_URL + titlesheetid + "/values/" + URLEncoder.encode(range, "UTF-8")
					+ "?valueInputOption=USER_ENTERED";
			JSONObject dataBody = new JSONObject();
			dataBody.put("values", new JSONArray(data));
			Map<String, String> updateHeaders = new HashMap<>();
			updateHeaders.put("Authorization", "Bearer " + accessToken);
			updateHeaders.put("Content-Type", "application/json; charset=UTF-8");
			HttpHelperUtility.putRequestStringBody(updateUrl, updateHeaders, dataBody.toString(), null, null, null,
					null);
			resp.setSuccess(true);
			return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Deletes an entire Google Spreadsheet by its spreadsheet ID.
	 *
	 * @param titlesheetid The spreadsheet ID.
	 * @param accessToken  The Google API OAuth2 access token.
	 * @return NounMetadata with {"success": true/false, "error": "..."} as result.
	 */
	public static NounMetadata deleteTitleSheet(String titlesheetid, String accessToken) {
		try {
			String urlStr = GOOGLEDRIVE_URL + titlesheetid;
			Map<String, String> headers = new HashMap<>();
			headers.put("Authorization", "Bearer " + accessToken);
			HttpHelperUtility.deleteRequestStringBody(urlStr, headers, null, null, null);
			Map<String, Object> response = new HashMap<>();
			response.put("success", true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}
}
