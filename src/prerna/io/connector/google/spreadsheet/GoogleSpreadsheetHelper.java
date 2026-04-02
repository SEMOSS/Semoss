package prerna.io.connector.google.spreadsheet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

public class GoogleSpreadsheetHelper {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetHelper.class);

	private static final String SPREADDRIVE_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet' and trashed=false&fields=files(id,name)&pageSize=1000";
	private static final String SHEET_URL = "https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String GOOGLEDRIVE_URL = "https://www.googleapis.com/drive/v3/files/";

	private static final String AUTHORIZATION = "Authorization";
	private static final String BEARER = "Bearer ";
	private static final String CONTENT_TYPE = "Content-Type";
	private static final String APPLICATION_JSON_UTF8 = "application/json; charset=UTF-8";
	private static final String UTF_8 = StandardCharsets.UTF_8.name();

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
	private static final String ERROR = "error";
	private static final String TITLE_SHEET_ID = "titleSheetID";
	private static final String RESPONSE_SHEET_ID = "sheetID";
	private static final String VALUE_INPUT_OPTION = "?valueInputOption=USER_ENTERED";

	private GoogleSpreadsheetHelper() {

	}

	private static String getCellReference(int col, int row) {
		StringBuilder colRef = new StringBuilder();
		int tempCol = col;
		do {
			colRef.insert(0, (char) ('A' + (tempCol % 26)));
			tempCol = tempCol / 26 - 1;
		} while (tempCol >= 0);
		return colRef.toString() + (row + 1);
	}

	private static String encode(String value) throws IOException {
		return URLEncoder.encode(value, UTF_8);
	}

	private static String escapeDriveQueryValue(String value) {
		return value.replace("\\", "\\\\").replace("'", "\\'");
	}

	private static Map<String, String> buildAuthorizationHeaders(String accessToken) {
		Map<String, String> headers = new HashMap<>();
		headers.put(AUTHORIZATION, BEARER + accessToken);
		return headers;
	}

	private static Map<String, String> buildJsonHeaders(String accessToken) {
		Map<String, String> headers = buildAuthorizationHeaders(accessToken);
		headers.put(CONTENT_TYPE, APPLICATION_JSON_UTF8);
		return headers;
	}

	private static Map<String, Object> buildOperationResponse(boolean success, String spreadsheetId, String sheetId,
			String errorMessage) {
		Map<String, Object> response = new HashMap<>();
		response.put(SUCCESS, success);
		response.put(TITLE_SHEET_ID, spreadsheetId);
		response.put(RESPONSE_SHEET_ID, sheetId);
		if (errorMessage != null && !errorMessage.isEmpty()) {
			response.put(ERROR, errorMessage);
		}
		return response;
	}

	private static boolean hasSheetData(List<List<String>> data) {
		return data != null && !data.isEmpty() && getColumnCount(data) > 0;
	}

	private static int getColumnCount(List<List<String>> data) {
		int maxColumns = 0;
		if (data == null) {
			return maxColumns;
		}

		for (List<String> row : data) {
			if (row != null && row.size() > maxColumns) {
				maxColumns = row.size();
			}
		}
		return maxColumns;
	}

	private static String extractSheetId(JSONObject jsonResponse) {
		if (jsonResponse == null || !jsonResponse.has(SHEETS)) {
			return null;
		}

		JSONArray sheets = jsonResponse.optJSONArray(SHEETS);
		if (sheets == null || sheets.length() == 0) {
			return null;
		}

		JSONObject firstSheet = sheets.getJSONObject(0);
		JSONObject properties = firstSheet.optJSONObject(PROPERTIES);
		if (properties == null || !properties.has(SHEET_ID)) {
			return null;
		}

		return String.valueOf(properties.optInt(SHEET_ID, -1));
	}

	private static String getSheetNameById(String spreadsheetId, String sheetId, String accessToken) {
		try {
			String metaUrl = SHEET_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
			String metaResponse = HttpHelperUtility.getRequest(metaUrl, buildAuthorizationHeaders(accessToken), null,
					null, null);
			JSONObject metaJson = new JSONObject(metaResponse);
			JSONArray sheets = metaJson.optJSONArray(SHEETS);
			if (sheets == null) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("No sheets found for the provided spreadsheet ID."));
			}

			for (int i = 0; i < sheets.length(); i++) {
				JSONObject properties = sheets.getJSONObject(i).getJSONObject(PROPERTIES);
				if (String.valueOf(properties.getInt(SHEET_ID)).equals(sheetId)) {
					return properties.getString(TITLE);
				}
			}

			throw new SemossPixelException(NounMetadata
					.getErrorNounMessage("The provided sheet ID does not exist in the selected spreadsheet."));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	private static void updateSheetValues(String spreadsheetId, String sheetName, List<List<String>> data,
			String accessToken) throws IOException {
		int numRows = data.size();
		int numCols = getColumnCount(data);
		String startCell = "A1";
		String endCell = getCellReference(numCols - 1, numRows - 1);
		String range = sheetName + "!" + startCell;
		if (numRows > 1 || numCols > 1) {
			range += ":" + endCell;
		}

		String updateUrl = SHEET_URL + spreadsheetId + "/values/" + encode(range) + VALUE_INPUT_OPTION;
		JSONObject body = new JSONObject();
		body.put(VALUES, new JSONArray(data));
		HttpHelperUtility.putRequestStringBody(updateUrl, buildJsonHeaders(accessToken), body.toString(), null, null,
				null, null);
	}

	public static NounMetadata updateData(String spreadsheetId, String sheetId, List<List<String>> data,
			String accessToken) {
		try {
			String sheetName = getSheetNameById(spreadsheetId, sheetId, accessToken);
			String clearUrl = SHEET_URL + spreadsheetId + "/values/" + encode(sheetName) + ":clear";
			HttpHelperUtility.postRequestStringBody(clearUrl, buildJsonHeaders(accessToken), "{}", null, null, null,
					null);

			if (hasSheetData(data)) {
				updateSheetValues(spreadsheetId, sheetName, data, accessToken);
			}

			return new NounMetadata(buildOperationResponse(true, spreadsheetId, sheetId, null),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	public static NounMetadata readData(String spreadsheetId, String sheetId, String accessToken) {
		try {
			String sheetName = getSheetNameById(spreadsheetId, sheetId, accessToken);
			String url = SHEET_URL + spreadsheetId + "/values/" + encode(sheetName);
			String response = HttpHelperUtility.getRequest(url, buildAuthorizationHeaders(accessToken), null, null, null);
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
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	public static NounMetadata deleteSheet(String spreadsheetId, String sheetId, String accessToken) {
		final String DELETE_SHEET = "deleteSheet";

		try {
			JSONObject deleteSheetRequest = new JSONObject();
			deleteSheetRequest.put(DELETE_SHEET, new JSONObject().put(SHEET_ID, Integer.parseInt(sheetId)));
			JSONArray requests = new JSONArray();
			requests.put(deleteSheetRequest);
			JSONObject body = new JSONObject();
			body.put(REQUESTS, requests);
			String batchUpdateUrl = SHEET_URL + spreadsheetId + ":batchUpdate";
			HttpHelperUtility.postRequestStringBody(batchUpdateUrl, buildJsonHeaders(accessToken), body.toString(),
					null, null, null, null);

			return new NounMetadata(buildOperationResponse(true, spreadsheetId, sheetId, null),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	public static NounMetadata createNewSpreadsheet(String spreadsheetName, String accessToken) {
		try {
			String normalizedSpreadsheetName = spreadsheetName == null ? null : spreadsheetName.trim();
			if (normalizedSpreadsheetName == null || normalizedSpreadsheetName.isEmpty()) {
				return new NounMetadata(
						buildOperationResponse(false, null, null, "Spreadsheet name is required to create a spreadsheet."),
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			if (!validateSpreadsheetName(normalizedSpreadsheetName, accessToken)) {
				return new NounMetadata(buildOperationResponse(false, null, null,
						"A Google spreadsheet with the same name already exists."),
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			JSONObject properties = new JSONObject();
			properties.put(TITLE, normalizedSpreadsheetName);
			JSONObject body = new JSONObject();
			body.put(PROPERTIES, properties);

			String response = HttpHelperUtility.postRequestStringBody(SHEET_URL, buildJsonHeaders(accessToken),
					body.toString(), null, null, null, null);
			JSONObject jsonResponse = new JSONObject(response);
			String spreadsheetId = jsonResponse.optString(SPREADSHEET_ID, null);
			String sheetId = extractSheetId(jsonResponse);
			boolean success = spreadsheetId != null && !spreadsheetId.isEmpty();

			return new NounMetadata(
					buildOperationResponse(success, success ? spreadsheetId : null, sheetId,
							success ? null : "Unable to create the Google spreadsheet."),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	private static boolean validateSpreadsheetName(String spreadsheetName, String accessToken) {
		try {
			String query = "name='" + escapeDriveQueryValue(spreadsheetName)
					+ "' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false";
			String urlStr = GOOGLEDRIVE_URL + "?q=" + encode(query) + "&fields=files(id)&spaces=drive&pageSize=1";
			String response = HttpHelperUtility.getRequest(urlStr, buildAuthorizationHeaders(accessToken), null, null,
					null);
			JSONArray files = new JSONObject(response).optJSONArray(FILES);
			return files == null || files.length() == 0;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	public static NounMetadata createNewSheet(String spreadsheetId, String sheetName, String accessToken,
			List<List<String>> data) {
		final String ADD_SHEET = "addSheet";

		try {
			String normalizedSheetName = sheetName == null ? null : sheetName.trim();
			if (spreadsheetId == null || spreadsheetId.isEmpty()) {
				return new NounMetadata(buildOperationResponse(false, null, null,
						"Spreadsheet ID is required to create a sheet."), PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			if (normalizedSheetName == null || normalizedSheetName.isEmpty()) {
				return new NounMetadata(buildOperationResponse(false, spreadsheetId, null,
						"Sheet name is required to create a sheet."), PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}

			String getUrl = SHEET_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
			String response = HttpHelperUtility.getRequest(getUrl, buildAuthorizationHeaders(accessToken), null, null,
					null);
			JSONObject jsonResponse = new JSONObject(response);
			JSONArray sheets = jsonResponse.optJSONArray(SHEETS);
			if (sheets != null) {
				for (int i = 0; i < sheets.length(); i++) {
					JSONObject properties = sheets.getJSONObject(i).getJSONObject(PROPERTIES);
					if (normalizedSheetName.equalsIgnoreCase(properties.optString(TITLE))) {
						throw new SemossPixelException(NounMetadata.getErrorNounMessage(
								"A sheet with the same name already exists in the selected spreadsheet."));
					}
				}
			}

			String batchUpdateUrl = SHEET_URL + spreadsheetId + ":batchUpdate";
			JSONObject addSheetRequest = new JSONObject();
			JSONObject sheetProperties = new JSONObject();
			sheetProperties.put(TITLE, normalizedSheetName);
			addSheetRequest.put(ADD_SHEET, new JSONObject().put(PROPERTIES, sheetProperties));
			JSONArray requests = new JSONArray();
			requests.put(addSheetRequest);
			JSONObject body = new JSONObject();
			body.put(REQUESTS, requests);

			String batchResponse = HttpHelperUtility.postRequestStringBody(batchUpdateUrl, buildJsonHeaders(accessToken),
					body.toString(), null, null, null, null);
			jsonResponse = new JSONObject(batchResponse);
			String createdSheetId = null;
			if (jsonResponse.has(REPLIES)) {
				JSONArray replies = jsonResponse.getJSONArray(REPLIES);
				if (replies.length() > 0) {
					JSONObject addSheetReply = replies.getJSONObject(0).optJSONObject(ADD_SHEET);
					if (addSheetReply != null && addSheetReply.has(PROPERTIES)) {
						createdSheetId = String.valueOf(addSheetReply.getJSONObject(PROPERTIES).optInt(SHEET_ID, -1));
					}
				}
			}

			if (createdSheetId == null || createdSheetId.isEmpty() || "-1".equals(createdSheetId)) {
				return new NounMetadata(buildOperationResponse(false, spreadsheetId, null,
						"Sheet creation succeeded without a valid sheet ID response."),
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			if (hasSheetData(data)) {
				updateSheetValues(spreadsheetId, normalizedSheetName, data, accessToken);
			}

			return new NounMetadata(buildOperationResponse(true, spreadsheetId, createdSheetId, null),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	public static NounMetadata deleteSpreadsheet(String spreadsheetId, String accessToken) {
		try {
			String urlStr = GOOGLEDRIVE_URL + spreadsheetId;
			HttpHelperUtility.deleteRequestStringBody(urlStr, buildAuthorizationHeaders(accessToken), null, null, null);
			return new NounMetadata(buildOperationResponse(true, spreadsheetId, null, null),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	public static List<Map<String, Object>> fetchSpreadsheetMetadata(String accessToken) {
		final String SPREADSHEET_TITLE = "spreadsheetTitle";
		final String TITLE_ID = "TitleId";
		final String SHEET_NAMES = "sheetNames";

		List<Map<String, Object>> spreadsheets = new ArrayList<>();
		try {
			JSONObject driveResponse = httpGetJson(SPREADDRIVE_URL, accessToken);
			JSONArray files = driveResponse.optJSONArray(FILES);
			if (files == null) {
				return spreadsheets;
			}

			for (int i = 0; i < files.length(); i++) {
				JSONObject file = files.getJSONObject(i);
				String spreadsheetTitle = file.getString(NAME);
				String spreadsheetId = file.getString(ID);

				String sheetsUrl = SHEET_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
				JSONObject sheetsResponse = httpGetJson(sheetsUrl, accessToken);
				JSONArray sheets = sheetsResponse.optJSONArray(SHEETS);

				List<Map<String, Object>> sheetNameList = new ArrayList<>();
				if (sheets != null) {
					for (int j = 0; j < sheets.length(); j++) {
						JSONObject properties = sheets.getJSONObject(j).getJSONObject(PROPERTIES);
						Map<String, Object> sheetInfo = new HashMap<>();
						sheetInfo.put(ID, String.valueOf(properties.getInt(SHEET_ID)));
						sheetInfo.put(NAME, properties.getString(TITLE));
						sheetNameList.add(sheetInfo);
					}
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
			conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
			conn.setReadTimeout(READ_TIMEOUT_MS);

			int responseCode = conn.getResponseCode();
			if (responseCode != HttpURLConnection.HTTP_OK) {
				StringBuilder errorMsg = new StringBuilder();
				InputStream errorStream = conn.getErrorStream();
				if (errorStream != null) {
					try (BufferedReader errorReader = new BufferedReader(
							new InputStreamReader(errorStream, StandardCharsets.UTF_8))) {
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

			in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			return new JSONObject(response.toString());
		} catch (SocketTimeoutException e) {
			classLogger.error(Constants.STACKTRACE, e);
			Map<String, Object> retMap = new HashMap<>();
			retMap.put(TYPE_KEY, SOCKET_TIMEOUT_TYPE);
			retMap.put(MESSAGE_KEY, "HTTP GET timed out");
			throw new IOException(SOCKET_TIMEOUT_TYPE + ": " + retMap.toString(), e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			Map<String, Object> retMap = new HashMap<>();
			retMap.put(TYPE_KEY, IO_EXCEPTION_TYPE);
			retMap.put(MESSAGE_KEY, e.getMessage());
			throw new IOException(IO_EXCEPTION_TYPE + ": " + retMap.toString(), e);
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