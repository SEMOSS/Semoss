package prerna.io.connector.google.spreadsheet;

import java.io.IOException;
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

public class GoogleSpreadsheetHelper {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpreadsheetHelper.class);

	private static final String SHEET_URL = "https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String GOOGLEDRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files";
	private static final String GOOGLEDRIVE_FILE_URL = GOOGLEDRIVE_FILES_URL + "/";

	private static final String AUTHORIZATION = "Authorization";
	private static final String BEARER = "Bearer ";
	private static final String CONTENT_TYPE = "Content-Type";
	private static final String APPLICATION_JSON_UTF8 = "application/json; charset=UTF-8";
	private static final String UTF_8 = StandardCharsets.UTF_8.name();
	private static final int DRIVE_PAGE_SIZE = 1000;

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
	private static final String LEGACY_TITLE_ID = "TitleId";
	private static final String NEXT_PAGE_TOKEN = "nextPageToken";
	private static final String VALUE_INPUT_OPTION = "?valueInputOption=USER_ENTERED";

	private GoogleSpreadsheetHelper() {

	}

	/**
	 * Converts zero-based column and row indexes into an A1-style cell reference.
	 *
	 * @param col zero-based column index.
	 * @param row zero-based row index.
	 * @return A1 cell reference such as {@code A1} or {@code AB12}.
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
	 * URL-encodes a query-parameter value using UTF-8.
	 *
	 * @param value raw query parameter value.
	 * @return encoded value safe for query-string usage.
	 * @throws IOException if encoding fails.
	 */
	private static String encodeQueryParameter(String value) throws IOException {
		return URLEncoder.encode(value, UTF_8);
	}

	/**
	 * URL-encodes a path segment using UTF-8 while preserving spaces as
	 * {@code %20}.
	 *
	 * @param value raw path segment value.
	 * @return encoded value safe for URL path usage.
	 * @throws IOException if encoding fails.
	 */
	private static String encodePathSegment(String value) throws IOException {
		return URLEncoder.encode(value, UTF_8).replace("+", "%20");
	}

	/**
	 * Wraps a sheet name in an A1-compatible sheet reference and escapes quotes.
	 *
	 * @param sheetName raw sheet name.
	 * @return quoted sheet reference for use in A1 ranges.
	 */
	private static String toA1SheetReference(String sheetName) {
		return "'" + sheetName.replace("'", "''") + "'";
	}

	/**
	 * Escapes a value for use in a Google Drive search query.
	 *
	 * @param value raw query value.
	 * @return escaped value safe for Drive query filters.
	 */
	private static String escapeDriveQueryValue(String value) {
		return value.replace("\\", "\\\\").replace("'", "\\'");
	}

	/**
	 * Builds an authorization header map for Google API requests.
	 *
	 * @param accessToken OAuth access token for the current Google user.
	 * @return header map containing the bearer token.
	 */
	private static Map<String, String> buildAuthorizationHeaders(String accessToken) {
		Map<String, String> headers = new HashMap<>();
		headers.put(AUTHORIZATION, BEARER + accessToken);
		return headers;
	}

	/**
	 * Builds JSON request headers for Google API calls.
	 *
	 * @param accessToken OAuth access token for the current Google user.
	 * @return header map containing authorization and JSON content type.
	 */
	private static Map<String, String> buildJsonHeaders(String accessToken) {
		Map<String, String> headers = buildAuthorizationHeaders(accessToken);
		headers.put(CONTENT_TYPE, APPLICATION_JSON_UTF8);
		return headers;
	}

	/**
	 * Creates a normalized response payload for spreadsheet operations.
	 *
	 * @param success       whether the operation completed successfully.
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @param sheetId       Google sheet identifier.
	 * @param errorMessage  optional error message when the operation fails.
	 * @return response map used by spreadsheet reactors.
	 */
	private static Map<String, Object> buildOperationResponse(boolean success, String spreadsheetId, String sheetId,
			String errorMessage) {
		Map<String, Object> response = new HashMap<>();
		response.put(SUCCESS, success);
		response.put(SPREADSHEET_ID, spreadsheetId);
		response.put(SHEET_ID, sheetId);
		response.put(TITLE_SHEET_ID, spreadsheetId);
		response.put(RESPONSE_SHEET_ID, sheetId);
		if (errorMessage != null && !errorMessage.isEmpty()) {
			response.put(ERROR, errorMessage);
		}
		return response;
	}

	/**
	 * Trims a text value and normalizes blank input to {@code null} semantics.
	 *
	 * @param value raw text value.
	 * @return trimmed value, or {@code null} when the input is {@code null}.
	 */
	private static String normalizeValue(String value) {
		return value == null ? null : value.trim();
	}

	/**
	 * Validates that a required text input is present and non-blank.
	 *
	 * @param value     raw text value.
	 * @param fieldName field label used in validation errors.
	 * @return trimmed non-empty value.
	 * @throws SemossPixelException if the value is missing or blank.
	 */
	private static String requireText(String value, String fieldName) {
		String normalizedValue = normalizeValue(value);
		if (normalizedValue == null || normalizedValue.isEmpty()) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(fieldName + " is required."));
		}
		return normalizedValue;
	}

	/**
	 * Validates and returns a required Google access token.
	 *
	 * @param accessToken OAuth access token.
	 * @return validated non-empty access token.
	 */
	private static String requireAccessToken(String accessToken) {
		return requireText(accessToken, "Google access token");
	}

	/**
	 * Builds the Sheets metadata URL for a spreadsheet.
	 *
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @return metadata URL limited to sheet IDs and titles.
	 */
	private static String getSheetMetadataUrl(String spreadsheetId) {
		return SHEET_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
	}

	/**
	 * Executes a Google API GET request and parses the JSON response.
	 *
	 * @param url         target Google API URL.
	 * @param accessToken OAuth access token for the current Google user.
	 * @return parsed JSON response object.
	 */
	private static JSONObject getJsonResponse(String url, String accessToken) {
		String response = HttpHelperUtility.getRequest(url, buildAuthorizationHeaders(accessToken), null, null, null);
		return new JSONObject(response);
	}

	/**
	 * Builds a paginated Drive file-list URL for spreadsheet discovery.
	 *
	 * @param pageToken optional Drive pagination token.
	 * @return fully constructed Drive list URL.
	 * @throws IOException if query encoding fails.
	 */
	private static String buildSpreadsheetListUrl(String pageToken) throws IOException {
		String query = "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false";
		StringBuilder urlBuilder = new StringBuilder(GOOGLEDRIVE_FILES_URL).append("?q=")
				.append(encodeQueryParameter(query))
				.append("&fields=nextPageToken,files(id,name)&spaces=drive&pageSize=").append(DRIVE_PAGE_SIZE);
		if (pageToken != null && !pageToken.isEmpty()) {
			urlBuilder.append("&pageToken=").append(encodeQueryParameter(pageToken));
		}
		return urlBuilder.toString();
	}

	/**
	 * Parses a raw JSON payload into spreadsheet row and cell values.
	 *
	 * @param rawData JSON text expected to be a 2D array of row values.
	 * @return parsed row data, or an empty list when the input is blank.
	 * @throws SemossPixelException if the payload is not valid 2D tabular JSON.
	 */
	public static List<List<String>> parseSheetData(String rawData) {
		List<List<String>> parsedData = new ArrayList<>();
		String normalizedRawData = normalizeValue(rawData);
		if (normalizedRawData == null || normalizedRawData.isEmpty()) {
			return parsedData;
		}

		try {
			JSONArray rows = new JSONArray(normalizedRawData);
			for (int i = 0; i < rows.length(); i++) {
				Object rowValue = rows.get(i);
				if (!(rowValue instanceof JSONArray)) {
					throw new SemossPixelException(
							NounMetadata.getErrorNounMessage("Sheet data must be a JSON array of row arrays."));
				}

				JSONArray row = (JSONArray) rowValue;
				List<String> parsedRow = new ArrayList<>();
				for (int j = 0; j < row.length(); j++) {
					Object cellValue = row.get(j);
					if (cellValue instanceof JSONArray || cellValue instanceof JSONObject) {
						throw new SemossPixelException(
								NounMetadata.getErrorNounMessage("Sheet data cells must be scalar values or null."));
					}
					parsedRow.add(cellValue == JSONObject.NULL ? "" : String.valueOf(cellValue));
				}
				parsedData.add(parsedRow);
			}
			return parsedData;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to parse Google spreadsheet row payload", e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Sheet data must be valid JSON formatted as a 2D array."));
		}
	}

	/**
	 * Checks whether the provided sheet payload contains at least one writable
	 * cell.
	 *
	 * @param data parsed sheet data.
	 * @return {@code true} when the payload contains at least one column value.
	 */
	private static boolean hasSheetData(List<List<String>> data) {
		return data != null && !data.isEmpty() && getColumnCount(data) > 0;
	}

	/**
	 * Determines the maximum number of columns present in a sheet payload.
	 *
	 * @param data parsed sheet data.
	 * @return maximum row width across the payload.
	 */
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

	/**
	 * Extracts the first sheet ID from a spreadsheet creation response.
	 *
	 * @param jsonResponse spreadsheet API response.
	 * @return first sheet ID when available, otherwise {@code null}.
	 */
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

		int sheetId = properties.optInt(SHEET_ID, -1);
		return sheetId >= 0 ? String.valueOf(sheetId) : null;
	}

	/**
	 * Resolves a Google sheet ID to its current sheet title.
	 *
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @param sheetId       Google sheet identifier.
	 * @param accessToken   OAuth access token for Google APIs.
	 * @return sheet title corresponding to the provided sheet ID.
	 * @throws SemossPixelException if the spreadsheet or sheet cannot be resolved.
	 */
	private static String getSheetNameById(String spreadsheetId, String sheetId, String accessToken) {
		try {
			String normalizedSpreadsheetId = requireText(spreadsheetId, "Spreadsheet ID");
			String normalizedSheetId = requireText(sheetId, "Sheet ID");
			String normalizedAccessToken = requireAccessToken(accessToken);
			JSONObject metaJson = getJsonResponse(getSheetMetadataUrl(normalizedSpreadsheetId), normalizedAccessToken);
			JSONArray sheets = metaJson.optJSONArray(SHEETS);
			if (sheets == null) {
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("No sheets found for the provided spreadsheet ID."));
			}

			for (int i = 0; i < sheets.length(); i++) {
				JSONObject properties = sheets.getJSONObject(i).getJSONObject(PROPERTIES);
				if (String.valueOf(properties.getInt(SHEET_ID)).equals(normalizedSheetId)) {
					return properties.getString(TITLE);
				}
			}

			throw new SemossPixelException(NounMetadata
					.getErrorNounMessage("The provided sheet ID does not exist in the selected spreadsheet."));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to resolve Google sheet {} in spreadsheet: {}", sheetId, spreadsheetId, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Writes tabular values into a target Google sheet using the Sheets values API.
	 *
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @param sheetName     target sheet title.
	 * @param data          tabular row payload to write.
	 * @param accessToken   OAuth access token for Google APIs.
	 * @throws IOException if the HTTP request cannot be encoded or sent.
	 */
	private static void updateSheetValues(String spreadsheetId, String sheetName, List<List<String>> data,
			String accessToken) throws IOException {
		String normalizedSpreadsheetId = requireText(spreadsheetId, "Spreadsheet ID");
		String normalizedSheetName = requireText(sheetName, "Sheet name");
		String normalizedAccessToken = requireAccessToken(accessToken);
		int numRows = data.size();
		int numCols = getColumnCount(data);
		String startCell = "A1";
		String endCell = getCellReference(numCols - 1, numRows - 1);
		String range = toA1SheetReference(normalizedSheetName) + "!" + startCell;
		if (numRows > 1 || numCols > 1) {
			range += ":" + endCell;
		}

		String updateUrl = SHEET_URL + normalizedSpreadsheetId + "/values/" + encodePathSegment(range)
				+ VALUE_INPUT_OPTION;
		JSONObject body = new JSONObject();
		body.put(VALUES, new JSONArray(data));
		HttpHelperUtility.putRequestStringBody(updateUrl, buildJsonHeaders(normalizedAccessToken), body.toString(),
				null, null, null, null);
	}

	/**
	 * Replaces all values in a Google sheet with the provided tabular payload.
	 *
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @param sheetId       Google sheet identifier.
	 * @param data          replacement tabular data; empty input clears the sheet.
	 * @param accessToken   OAuth access token for Google APIs.
	 * @return operation result payload containing status and identifiers.
	 */
	public static NounMetadata updateData(String spreadsheetId, String sheetId, List<List<String>> data,
			String accessToken) {
		try {
			String normalizedSpreadsheetId = requireText(spreadsheetId, "Spreadsheet ID");
			String normalizedSheetId = requireText(sheetId, "Sheet ID");
			String normalizedAccessToken = requireAccessToken(accessToken);
			String sheetName = getSheetNameById(normalizedSpreadsheetId, normalizedSheetId, normalizedAccessToken);
			String clearUrl = SHEET_URL + normalizedSpreadsheetId + "/values/"
					+ encodePathSegment(toA1SheetReference(sheetName)) + ":clear";
			HttpHelperUtility.postRequestStringBody(clearUrl, buildJsonHeaders(normalizedAccessToken), "{}", null, null,
					null, null);

			if (hasSheetData(data)) {
				updateSheetValues(normalizedSpreadsheetId, sheetName, data, normalizedAccessToken);
			}

			return new NounMetadata(buildOperationResponse(true, normalizedSpreadsheetId, normalizedSheetId, null),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update Google sheet {} in spreadsheet: {}", sheetId, spreadsheetId, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Reads all currently populated values from a Google sheet.
	 *
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @param sheetId       Google sheet identifier.
	 * @param accessToken   OAuth access token for Google APIs.
	 * @return tabular sheet contents represented as a 2D string list.
	 */
	public static NounMetadata readData(String spreadsheetId, String sheetId, String accessToken) {
		try {
			String normalizedSpreadsheetId = requireText(spreadsheetId, "Spreadsheet ID");
			String normalizedSheetId = requireText(sheetId, "Sheet ID");
			String normalizedAccessToken = requireAccessToken(accessToken);
			String sheetName = getSheetNameById(normalizedSpreadsheetId, normalizedSheetId, normalizedAccessToken);
			String url = SHEET_URL + normalizedSpreadsheetId + "/values/"
					+ encodePathSegment(toA1SheetReference(sheetName));
			String response = HttpHelperUtility.getRequest(url, buildAuthorizationHeaders(normalizedAccessToken), null,
					null, null);
			JSONObject json = new JSONObject(response);
			JSONArray values = json.optJSONArray(VALUES);
			List<List<String>> result = new ArrayList<>();
			if (values != null) {
				for (int i = 0; i < values.length(); i++) {
					JSONArray row = values.getJSONArray(i);
					List<String> rowList = new ArrayList<>();
					for (int j = 0; j < row.length(); j++) {
						Object cellValue = row.get(j);
						rowList.add(cellValue == JSONObject.NULL ? "" : String.valueOf(cellValue));
					}
					result.add(rowList);
				}
			}
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to read Google sheet {} from spreadsheet: {}", sheetId, spreadsheetId, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Deletes a specific sheet tab from a Google spreadsheet.
	 *
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @param sheetId       Google sheet identifier.
	 * @param accessToken   OAuth access token for Google APIs.
	 * @return operation result payload containing status and identifiers.
	 */
	public static NounMetadata deleteSheet(String spreadsheetId, String sheetId, String accessToken) {
		final String DELETE_SHEET = "deleteSheet";

		try {
			String normalizedSpreadsheetId = requireText(spreadsheetId, "Spreadsheet ID");
			String normalizedSheetId = requireText(sheetId, "Sheet ID");
			String normalizedAccessToken = requireAccessToken(accessToken);
			JSONObject deleteSheetRequest = new JSONObject();
			deleteSheetRequest.put(DELETE_SHEET, new JSONObject().put(SHEET_ID, Integer.parseInt(normalizedSheetId)));
			JSONArray requests = new JSONArray();
			requests.put(deleteSheetRequest);
			JSONObject body = new JSONObject();
			body.put(REQUESTS, requests);
			String batchUpdateUrl = SHEET_URL + normalizedSpreadsheetId + ":batchUpdate";
			HttpHelperUtility.postRequestStringBody(batchUpdateUrl, buildJsonHeaders(normalizedAccessToken),
					body.toString(), null, null, null, null);

			return new NounMetadata(buildOperationResponse(true, normalizedSpreadsheetId, normalizedSheetId, null),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (NumberFormatException e) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("Sheet ID must be numeric."));
		} catch (Exception e) {
			classLogger.error("Failed to delete Google sheet {} from spreadsheet: {}", sheetId, spreadsheetId, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Creates a new Google spreadsheet for the current user.
	 *
	 * @param spreadsheetName desired spreadsheet title.
	 * @param accessToken     OAuth access token for Google APIs.
	 * @return operation result payload containing status and identifiers.
	 */
	public static NounMetadata createNewSpreadsheet(String spreadsheetName, String accessToken) {
		try {
			String normalizedSpreadsheetName = spreadsheetName == null ? null : spreadsheetName.trim();
			String normalizedAccessToken = requireAccessToken(accessToken);
			if (normalizedSpreadsheetName == null || normalizedSpreadsheetName.isEmpty()) {
				return new NounMetadata(
						buildOperationResponse(false, null, null,
								"Spreadsheet name is required to create a spreadsheet."),
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			if (!validateSpreadsheetName(normalizedSpreadsheetName, normalizedAccessToken)) {
				return new NounMetadata(
						buildOperationResponse(false, null, null,
								"A Google spreadsheet with the same name already exists."),
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			JSONObject properties = new JSONObject();
			properties.put(TITLE, normalizedSpreadsheetName);
			JSONObject body = new JSONObject();
			body.put(PROPERTIES, properties);

			String response = HttpHelperUtility.postRequestStringBody(SHEET_URL,
					buildJsonHeaders(normalizedAccessToken), body.toString(), null, null, null, null);
			JSONObject jsonResponse = new JSONObject(response);
			String spreadsheetId = jsonResponse.optString(SPREADSHEET_ID, null);
			String sheetId = extractSheetId(jsonResponse);
			boolean success = spreadsheetId != null && !spreadsheetId.isEmpty();

			return new NounMetadata(
					buildOperationResponse(success, success ? spreadsheetId : null, sheetId,
							success ? null : "Unable to create the Google spreadsheet."),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to create Google spreadsheet for name: {}", spreadsheetName, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Checks whether a spreadsheet with the requested title already exists.
	 *
	 * @param spreadsheetName candidate spreadsheet title.
	 * @param accessToken     OAuth access token for Google APIs.
	 * @return {@code true} when the title is available; otherwise {@code false}.
	 */
	private static boolean validateSpreadsheetName(String spreadsheetName, String accessToken) {
		try {
			requireAccessToken(accessToken);
			String query = "name='" + escapeDriveQueryValue(spreadsheetName)
					+ "' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false";
			String urlStr = GOOGLEDRIVE_FILES_URL + "?q=" + encodeQueryParameter(query)
					+ "&fields=files(id)&spaces=drive&pageSize=1";
			String response = HttpHelperUtility.getRequest(urlStr, buildAuthorizationHeaders(accessToken), null, null,
					null);
			JSONArray files = new JSONObject(response).optJSONArray(FILES);
			return files == null || files.length() == 0;
		} catch (Exception e) {
			classLogger.error("Failed to validate Google spreadsheet title availability for name: {}", spreadsheetName,
					e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Creates a new sheet tab in an existing Google spreadsheet.
	 *
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @param sheetName     desired sheet title.
	 * @param accessToken   OAuth access token for Google APIs.
	 * @param data          optional initial tabular data to write after creation.
	 * @return operation result payload containing status and identifiers.
	 */
	public static NounMetadata createNewSheet(String spreadsheetId, String sheetName, String accessToken,
			List<List<String>> data) {
		final String ADD_SHEET = "addSheet";

		try {
			String normalizedSpreadsheetId = normalizeValue(spreadsheetId);
			String normalizedSheetName = normalizeValue(sheetName);
			String normalizedAccessToken = requireAccessToken(accessToken);
			if (normalizedSpreadsheetId == null || normalizedSpreadsheetId.isEmpty()) {
				return new NounMetadata(
						buildOperationResponse(false, null, null, "Spreadsheet ID is required to create a sheet."),
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			if (normalizedSheetName == null || normalizedSheetName.isEmpty()) {
				return new NounMetadata(
						buildOperationResponse(false, spreadsheetId, null, "Sheet name is required to create a sheet."),
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			JSONObject jsonResponse = getJsonResponse(getSheetMetadataUrl(normalizedSpreadsheetId),
					normalizedAccessToken);
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

			String batchUpdateUrl = SHEET_URL + normalizedSpreadsheetId + ":batchUpdate";
			JSONObject addSheetRequest = new JSONObject();
			JSONObject sheetProperties = new JSONObject();
			sheetProperties.put(TITLE, normalizedSheetName);
			addSheetRequest.put(ADD_SHEET, new JSONObject().put(PROPERTIES, sheetProperties));
			JSONArray requests = new JSONArray();
			requests.put(addSheetRequest);
			JSONObject body = new JSONObject();
			body.put(REQUESTS, requests);

			String batchResponse = HttpHelperUtility.postRequestStringBody(batchUpdateUrl,
					buildJsonHeaders(normalizedAccessToken), body.toString(), null, null, null, null);
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
				return new NounMetadata(
						buildOperationResponse(false, normalizedSpreadsheetId, null,
								"Sheet creation succeeded without a valid sheet ID response."),
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			if (hasSheetData(data)) {
				updateSheetValues(normalizedSpreadsheetId, normalizedSheetName, data, normalizedAccessToken);
			}

			return new NounMetadata(buildOperationResponse(true, normalizedSpreadsheetId, createdSheetId, null),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create Google sheet '{}' in spreadsheet: {}", sheetName, spreadsheetId, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Deletes a Google spreadsheet by removing the underlying Drive file.
	 *
	 * @param spreadsheetId Google spreadsheet identifier.
	 * @param accessToken   OAuth access token for Google APIs.
	 * @return operation result payload containing status and identifiers.
	 */
	public static NounMetadata deleteSpreadsheet(String spreadsheetId, String accessToken) {
		try {
			String normalizedSpreadsheetId = requireText(spreadsheetId, "Spreadsheet ID");
			String normalizedAccessToken = requireAccessToken(accessToken);
			String urlStr = GOOGLEDRIVE_FILE_URL + normalizedSpreadsheetId;
			HttpHelperUtility.deleteRequestStringBody(urlStr, buildAuthorizationHeaders(normalizedAccessToken), null,
					null, null);
			return new NounMetadata(buildOperationResponse(true, normalizedSpreadsheetId, null, null),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to delete Google spreadsheet: {}", spreadsheetId, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	/**
	 * Lists spreadsheets available to the current user along with their sheet tabs.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @return list of spreadsheet metadata maps including IDs, titles, and sheet
	 *         names.
	 */
	public static List<Map<String, Object>> fetchSpreadsheetMetadata(String accessToken) {
		final String SPREADSHEET_TITLE = "spreadsheetTitle";
		final String SHEET_NAMES = "sheetNames";

		List<Map<String, Object>> spreadsheets = new ArrayList<>();
		try {
			String normalizedAccessToken = requireAccessToken(accessToken);
			String nextPageToken = null;
			do {
				JSONObject driveResponse = getJsonResponse(buildSpreadsheetListUrl(nextPageToken),
						normalizedAccessToken);
				JSONArray files = driveResponse.optJSONArray(FILES);
				if (files != null) {
					for (int i = 0; i < files.length(); i++) {
						JSONObject file = files.getJSONObject(i);
						String spreadsheetTitle = file.optString(NAME, null);
						String spreadsheetId = normalizeValue(file.optString(ID, null));
						if (spreadsheetId == null || spreadsheetId.isEmpty()) {
							continue;
						}

						JSONObject sheetsResponse = getJsonResponse(getSheetMetadataUrl(spreadsheetId),
								normalizedAccessToken);
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
						spreadsheetInfo.put(SPREADSHEET_ID, spreadsheetId);
						spreadsheetInfo.put(TITLE_SHEET_ID, spreadsheetId);
						spreadsheetInfo.put(LEGACY_TITLE_ID, spreadsheetId);
						spreadsheetInfo.put(SHEET_NAMES, sheetNameList);
						spreadsheets.add(spreadsheetInfo);
					}
				}
				nextPageToken = normalizeValue(driveResponse.optString(NEXT_PAGE_TOKEN, null));
			} while (nextPageToken != null && !nextPageToken.isEmpty());
		} catch (Exception e) {
			classLogger.error("Failed to fetch Google spreadsheet metadata", e);
			throw new SemossPixelException(
					"An error occurred in fetching spreadsheet metadata. Error message: " + e.getMessage());
		}
		return spreadsheets;
	}
}