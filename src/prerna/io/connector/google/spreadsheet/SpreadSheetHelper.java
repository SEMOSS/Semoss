package prerna.io.connector.google.spreadsheet;

import java.net.URLEncoder;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.SocketTimeoutException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
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

	private static final Logger classLogger = LogManager.getLogger(SpreadSheetHelper.class);

	// API URLs
	private static final String SPREADDRIVE_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'&fields=files(id,name)";
	private static final String SHEET_URL = "https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String GOOGLEDRIVE_URL = "https://www.googleapis.com/drive/v3/files/";
	private static final String TITLESHEET_NAMEURL = "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false";

	// HTTP header keys and values
	private static final String AUTHORIZATION = "Authorization";
	private static final String BEARER = "Bearer ";
	private static final String CONTENT_TYPE = "Content-Type";
	private static final String APPLICATION_JSON_UTF8 = "application/json; charset=UTF-8";

	// JSON field keys - used across multiple methods
	private static final String FILES = "files";
	private static final String ID = "id";
	private static final String NAME = "name";
	private static final String SHEETS = "sheets";
	private static final String PROPERTIES = "properties";
	private static final String SHEET_ID = "sheetId";
	private static final String TITLE = "title";
	private static final String VALUES = "values";
	private static final String SPREADSHEET_ID = "spreadsheetId";
	private static final String REQUESTS = "requests";
	private static final String REPLIES = "replies";
	private static final String SUCCESS = "success";
	private static final String VALUE_INPUT_OPTION = "?valueInputOption=USER_ENTERED";

	private SpreadSheetHelper() {

	}

	/**
	 *
	 * @param col
	 * @param row
	 * @return
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
	 *
	 * @param titleSheetName
	 * @param accessToken
	 * @return
	 */
	private static String getSpreadsheetIdByTitle(String titleSheetName, String accessToken) {
		final String NO_SPREADSHEET_FOUND = "No spreadsheet found with that title";

		try {
			String query = "name='" + titleSheetName + "' and mimeType='application/vnd.google-apps.spreadsheet'";
			String encodedQuery = URLEncoder.encode(query, "UTF-8");
			String urlStr = "https://www.googleapis.com/drive/v3/files" + "?q=" + encodedQuery
					+ "&fields=files(id,name)&spaces=drive";
			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BEARER + accessToken);
			String response = HttpHelperUtility.getRequest(urlStr, headers, null, null, null);
			JSONObject jsonResponse = new JSONObject(response);
			JSONArray files = jsonResponse.optJSONArray(FILES);
			if (files != null && files.length() > 0) {
				return files.getJSONObject(0).getString(ID);
			} else {
				return NO_SPREADSHEET_FOUND;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 *
	 * @param titlesheetid
	 * @param sheetId
	 * @param data
	 * @param accessToken
	 * @return
	 */
	public static NounMetadata updateData(String titlesheetid, String sheetId, List<List<String>> data,
			String accessToken) {
		final String ERROR = "error";
		final String SHEET_ID_NOT_FOUND = "Sheet ID not found in spreadsheet.";

		try {
			// 1. Get the sheet name from the sheetId
			String metaUrl = SHEET_URL + titlesheetid + "?fields=sheets(properties(sheetId,title))";
			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BEARER + accessToken);
			String metaResponse = HttpHelperUtility.getRequest(metaUrl, headers, null, null, null);
			JSONObject metaJson = new JSONObject(metaResponse);
			JSONArray sheets = metaJson.getJSONArray(SHEETS);
			String sheetName = null;
			for (int i = 0; i < sheets.length(); i++) {
				JSONObject properties = sheets.getJSONObject(i).getJSONObject(PROPERTIES);
				if (String.valueOf(properties.getInt(SHEET_ID)).equals(sheetId)) {
					sheetName = properties.getString(TITLE);
					break;
				}
			}
			Map<String, Object> response = new HashMap<>();
			if (sheetName == null) {
				response.put(SUCCESS, false);
				response.put(ERROR, SHEET_ID_NOT_FOUND);
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
			String url = SHEET_URL + titlesheetid + "/values/" + URLEncoder.encode(range, "UTF-8") + VALUE_INPUT_OPTION;
			JSONObject body = new JSONObject();
			body.put(VALUES, new JSONArray(data));
			HttpHelperUtility.putRequestStringBody(url, headers, body.toString(), null, null, null, null);

			response.put(SUCCESS, true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 *
	 * @param spreadsheetId
	 * @param sheetId
	 * @param accessToken
	 * @return
	 */
	public static NounMetadata readData(String spreadsheetId, String sheetId, String accessToken) {
		try {
			// 1. Get the sheet name from the sheetId
			String metaUrl = SHEET_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BEARER + accessToken);
			String metaResponse = HttpHelperUtility.getRequest(metaUrl, headers, null, null, null);
			JSONObject metaJson = new JSONObject(metaResponse);
			JSONArray sheets = metaJson.getJSONArray(SHEETS);
			String sheetName = null;
			boolean sheetIdFound = false;
			for (int i = 0; i < sheets.length(); i++) {
				JSONObject properties = sheets.getJSONObject(i).getJSONObject(PROPERTIES);
				if (String.valueOf(properties.getInt(SHEET_ID)).equals(sheetId)) {
					sheetName = properties.getString(TITLE);
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
			JSONArray values = json.optJSONArray(VALUES);
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
	 *
	 * @param titlesheetid
	 * @param sheetId
	 * @param accessToken
	 * @return
	 */
	public static NounMetadata deleteSheet(String titlesheetid, String sheetId, String accessToken) {
		final String DELETE_SHEET = "deleteSheet";

		try {
			JSONObject deleteSheetRequest = new JSONObject();
			deleteSheetRequest.put(DELETE_SHEET, new JSONObject().put(SHEET_ID, Integer.parseInt(sheetId)));
			JSONArray requests = new JSONArray();
			requests.put(deleteSheetRequest);
			JSONObject body = new JSONObject();
			body.put(REQUESTS, requests);
			String batchUpdateUrl = SHEET_URL + titlesheetid + ":batchUpdate";
			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BEARER + accessToken);
			headers.put(CONTENT_TYPE, APPLICATION_JSON_UTF8);
			HttpHelperUtility.postRequestStringBody(batchUpdateUrl, headers, body.toString(), null, null, null, null);

			Map<String, Object> response = new HashMap<>();
			response.put(SUCCESS, true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 *
	 * @param accessToken
	 * @return
	 */
	public static List<String> getSpreadSheetId(String accessToken) {
		List<String> spreadsheetIds = new ArrayList<>();
		try {
			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BEARER + accessToken);
			String response = HttpHelperUtility.getRequest(SPREADDRIVE_URL, headers, null, null, null);
			JSONObject json = new JSONObject(response);
			JSONArray files = json.getJSONArray(FILES);
			for (int i = 0; i < files.length(); i++) {
				JSONObject file = files.getJSONObject(i);
				String spreadsheetId = file.getString(ID);
				spreadsheetIds.add(spreadsheetId);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return spreadsheetIds;
	}

	/**
	 *
	 * @param accessToken
	 * @param spreadSheetIds
	 * @return
	 */
	public static List<String> getSheetName(String accessToken, List<String> spreadSheetIds) {
		List<String> allSheetNames = new ArrayList<>();
		try {
			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BEARER + accessToken);
			for (String spreadsheetId : spreadSheetIds) {
				String sheetsUrl = SHEET_URL + spreadsheetId;
				String response = HttpHelperUtility.getRequest(sheetsUrl, headers, null, null, null);
				JSONObject sheetsJson = new JSONObject(response);
				JSONArray sheets = sheetsJson.getJSONArray(SHEETS);
				for (int j = 0; j < sheets.length(); j++) {
					JSONObject sheet = sheets.getJSONObject(j).getJSONObject(PROPERTIES);
					String sheetTitle = sheet.getString(TITLE);
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
	 *
	 * @param titleSheetName
	 * @param accessToken
	 * @return
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
			headers.put(AUTHORIZATION, BEARER + accessToken);
			headers.put(CONTENT_TYPE, APPLICATION_JSON_UTF8);
			JSONObject properties = new JSONObject();
			properties.put(TITLE, titleSheetName);
			JSONObject body = new JSONObject();
			body.put(PROPERTIES, properties);
			String response = HttpHelperUtility.postRequestStringBody(url, headers, body.toString(), null, null, null,
					null);
			JSONObject jsonResponse = new JSONObject(response);
			if (jsonResponse.has(SPREADSHEET_ID)) {
				String spreadsheetId = jsonResponse.optString(SPREADSHEET_ID, "");
				String sheetId = null;
				if (jsonResponse.has(SHEETS)) {
					JSONArray sheets = jsonResponse.getJSONArray(SHEETS);
					if (sheets.length() > 0) {
						JSONObject firstSheet = sheets.getJSONObject(0);
						JSONObject propertiesObj = firstSheet.optJSONObject(PROPERTIES);
						if (propertiesObj != null) {
							sheetId = String.valueOf(propertiesObj.optInt(SHEET_ID, -1));
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
	 *
	 * @param titleSheetName
	 * @param accessToken
	 * @return
	 */
	private static boolean validateTitleSheetName(String titleSheetName, String accessToken) {
		try {
			String query = TITLESHEET_NAMEURL;
			String encodedQuery = URLEncoder.encode(query, "UTF-8");
			String urlStr = "https://www.googleapis.com/drive/v3/files" + "?q=" + encodedQuery
					+ "&fields=files(name)&spaces=drive";
			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BEARER + accessToken);
			String response = HttpHelperUtility.getRequest(urlStr, headers, null, null, null);
			JSONObject jsonResponse = new JSONObject(response);
			JSONArray files = jsonResponse.optJSONArray(FILES);
			if (files != null) {
				for (int i = 0; i < files.length(); i++) {
					JSONObject file = files.getJSONObject(i);
					if (file.getString(NAME).equalsIgnoreCase(titleSheetName)) {
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
	 *
	 * @param titlesheetid
	 * @param sheetName
	 * @param accessToken
	 * @param data
	 * @return
	 */
	public static NounMetadata createNewSheet(String titlesheetid, String sheetName, String accessToken,
			List<List<String>> data) {
		final String ADD_SHEET = "addSheet";

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
			headers.put(AUTHORIZATION, BEARER + accessToken);
			String response = HttpHelperUtility.getRequest(getUrl, headers, null, null, null);
			JSONObject jsonResponse = new JSONObject(response);
			JSONArray sheets = jsonResponse.optJSONArray(SHEETS);
			if (sheets != null) {
				for (int i = 0; i < sheets.length(); i++) {
					JSONObject properties = sheets.getJSONObject(i).getJSONObject(PROPERTIES);
					if (sheetName.equals(properties.optString(TITLE))) {
						throw new SemossPixelException(NounMetadata.getErrorNounMessage("A sheet with the name '"
								+ sheetName + "' already exists. Two sheets cannot have the same name."));
					}
				}
			}
			// 2. Create the new sheet
			String batchUpdateUrl = SHEET_URL + titlesheetid + ":batchUpdate";
			headers.put(CONTENT_TYPE, APPLICATION_JSON_UTF8);
			JSONObject addSheetRequest = new JSONObject();
			JSONObject sheetProperties = new JSONObject();
			sheetProperties.put(TITLE, sheetName);
			addSheetRequest.put(ADD_SHEET, new JSONObject().put(PROPERTIES, sheetProperties));
			JSONArray requests = new JSONArray();
			requests.put(addSheetRequest);
			JSONObject body = new JSONObject();
			body.put(REQUESTS, requests);
			String batchResponse = HttpHelperUtility.postRequestStringBody(batchUpdateUrl, headers, body.toString(),
					null, null, null, null);
			jsonResponse = new JSONObject(batchResponse);
			String sheetId = null;
			if (jsonResponse.has(REPLIES)) {
				JSONArray replies = jsonResponse.getJSONArray(REPLIES);
				if (replies.length() > 0) {
					JSONObject addSheetReply = replies.getJSONObject(0).optJSONObject(ADD_SHEET);
					if (addSheetReply != null && addSheetReply.has(PROPERTIES)) {
						sheetId = String.valueOf(addSheetReply.getJSONObject(PROPERTIES).optInt(SHEET_ID, -1));
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
					+ VALUE_INPUT_OPTION;
			JSONObject dataBody = new JSONObject();
			dataBody.put(VALUES, new JSONArray(data));
			Map<String, String> updateHeaders = new HashMap<>();
			updateHeaders.put(AUTHORIZATION, BEARER + accessToken);
			updateHeaders.put(CONTENT_TYPE, APPLICATION_JSON_UTF8);
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
	 *
	 * @param titlesheetid
	 * @param accessToken
	 * @return
	 */
	public static NounMetadata deleteTitleSheet(String titlesheetid, String accessToken) {
		try {
			String urlStr = GOOGLEDRIVE_URL + titlesheetid;
			Map<String, String> headers = new HashMap<>();
			headers.put(AUTHORIZATION, BEARER + accessToken);
			HttpHelperUtility.deleteRequestStringBody(urlStr, headers, null, null, null);
			Map<String, Object> response = new HashMap<>();
			response.put(SUCCESS, true);
			return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * @param accessToken
	 * @return
	 */
	public static List<Map<String, Object>> fetchSpreadsheetMetadata(String accessToken) {
		final String SPREADSHEET_TITLE = "spreadsheetTitle";
		final String TITLE_ID = "TitleId";
		final String SHEET_NAMES = "sheetNames";

		List<Map<String, Object>> spreadsheets = new ArrayList<>();
		try {
			JSONObject driveResponse = httpGetJson(SPREADDRIVE_URL, accessToken);
			JSONArray files = driveResponse.getJSONArray(FILES);

			for (int i = 0; i < files.length(); i++) {
				JSONObject file = files.getJSONObject(i);
				String spreadsheetTitle = file.getString(NAME);
				String spreadsheetId = file.getString(ID);

				// Fetch all sheet names and IDs for this spreadsheet
				String sheetsUrl = SHEET_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
				JSONObject sheetsResponse = httpGetJson(sheetsUrl, accessToken);
				JSONArray sheets = sheetsResponse.getJSONArray(SHEETS);

				List<Map<String, Object>> sheetNameList = new ArrayList<>();
				for (int j = 0; j < sheets.length(); j++) {
					JSONObject properties = sheets.getJSONObject(j).getJSONObject(PROPERTIES);
					Map<String, Object> sheetInfo = new HashMap<>();
					sheetInfo.put(ID, String.valueOf(properties.getInt(SHEET_ID)));
					sheetInfo.put(NAME, properties.getString(TITLE));
					sheetNameList.add(sheetInfo);
				}

				Map<String, Object> spreadsheetInfo = new HashMap<>();
				spreadsheetInfo.put(SPREADSHEET_TITLE, spreadsheetTitle);
				spreadsheetInfo.put(TITLE_ID, spreadsheetId);
				spreadsheetInfo.put(SHEET_NAMES, sheetNameList);

				spreadsheets.add(spreadsheetInfo);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred in fetching spreadsheet metadata. Error message: " + e.getMessage());
		}
		return spreadsheets;
	}

	/**
	 * 
	 * @param googledriveUrl
	 * @param accessToken
	 * @return
	 * @throws IOException
	 */
	public static JSONObject httpGetJson(String googledriveUrl, String accessToken) throws IOException {
		final String REQUEST_METHOD_GET = "GET";
		final int CONNECT_TIMEOUT_MS = 10000;
		final int READ_TIMEOUT_MS = 30000;
		final String NO_ERROR_DETAILS = "No error details available.";
		final String TYPE_KEY = "type";
		final String MESSAGE_KEY = "message";
		final String SOCKET_TIMEOUT_TYPE = "SocketTimeoutException";
		final String IO_EXCEPTION_TYPE = "IOException";

		StringBuilder response = new StringBuilder();
		HttpURLConnection conn = null;
		BufferedReader in = null;

		try {
			URL url = new URL(googledriveUrl);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(REQUEST_METHOD_GET);
			conn.setRequestProperty(AUTHORIZATION, BEARER + accessToken);

			// Set timeouts
			conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
			conn.setReadTimeout(READ_TIMEOUT_MS);

			int responseCode = conn.getResponseCode();
			if (responseCode != HttpURLConnection.HTTP_OK) {
				StringBuilder errorMsg = new StringBuilder();
				InputStream errorStream = conn.getErrorStream();
				if (errorStream != null) {
					try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream))) {
						String line;
						while ((line = errorReader.readLine()) != null) {
							errorMsg.append(line);
						}
					}
				} else {
					errorMsg.append(NO_ERROR_DETAILS);
				}
				classLogger.error("HTTP GET failed: {} - {}", responseCode, errorMsg.toString());
				throw new IOException(
						"HTTP GET failed with code: " + responseCode + " and body: " + errorMsg.toString());
			}

			in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			return new JSONObject(response.toString());

		} catch (SocketTimeoutException e) {
			classLogger.error(Constants.STACKTRACE, e);
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put(TYPE_KEY, SOCKET_TIMEOUT_TYPE);
			retMap.put(MESSAGE_KEY, "HTTP GET timed out");
			throw new IOException(SOCKET_TIMEOUT_TYPE + ": " + retMap.toString(), e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put(TYPE_KEY, IO_EXCEPTION_TYPE);
			retMap.put(MESSAGE_KEY, e.getMessage());
			throw new IOException(IO_EXCEPTION_TYPE + ": " + retMap, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IOException("Unexpected error during HTTP GET", e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (Exception ignore) {
				}
			}
			if (conn != null) {
				conn.disconnect();
			}
		}
	}
}
