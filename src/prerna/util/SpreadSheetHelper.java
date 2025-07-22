package prerna.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.FileList;

import prerna.reactor.model.SpreadSheetResponse;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SpreadSheetHelper {

	private static final String SPREADDRIVE_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'&fields=files(id,name)";
	private static final String SHEET_URL = "https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String GOOGLEDRIVE_URL = "https://www.googleapis.com/drive/v3/files/";
	private static final String TITLESHEET_NAMEURL = "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false";
	private static final Logger classLogger = LogManager.getLogger(SpreadSheetHelper.class);

	/**
	 * To write data in spreadsheet given by user
	 *
	 * @param titleSheetName
	 * @param sheetName
	 * @param data
	 * @param accessToken
	 * @return
	 */
	public static NounMetadata writeData(String titleSheetName, String sheetName, List<List<String>> data,
			String accessToken) {
		try {
			String msg = null;

			// Resolve spreadsheetId from titleSheetName
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);

			// Convert Java List<List<String>> to JSONArray
			JSONArray dataArray = new JSONArray(data);

			// Determine number of rows and columns
			int numRows = dataArray.length();
			int numCols = numRows > 0 ? dataArray.getJSONArray(0).length() : 0;

			// Calculate the range, e.g., "Sheet1!A1:C4"
			String startCell = "A1";
			String endCell = getCellReference(numCols, numRows);
			String range = sheetName + "!" + startCell + ":" + endCell;

			String url = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(range, "UTF-8")
					+ "?valueInputOption=USER_ENTERED";

			// Build the JSON body
			JSONObject body = new JSONObject();
			body.put("values", dataArray);

			// PUT call to spreadsheet API (update)
			HashMap<String, Object> responseMessage = sendPutRequest(url, body.toString(), accessToken);

			Object responseBody = responseMessage.get("ResponseBody");
			int responseCode = (int) responseMessage.get("ResponseCode");
			JSONObject jsonResponse = new JSONObject(responseBody.toString());

			if (responseCode >= 200 && responseCode < 300) {
				if (jsonResponse.has("updatedCells") && jsonResponse.getInt("updatedCells") > 0) {
					msg = "Data written successfully";
				} else {
					msg = "No cells were updated. Response: " + jsonResponse.toString();
				}
			} else {
				msg = "Failed to write data. HTTP code: " + responseCode + ". Response: " + jsonResponse.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
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

	/**
	 * To send post reques
	 *
	 * @param url
	 * @param jsonBody
	 * @param accessToken
	 * @return
	 */
	private static HashMap<String, Object> sendPostRequest(String url, String jsonBody, String accessToken) {
		HashMap<String, Object> hashMap = new HashMap<>();
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json");

			conn.setDoOutput(true);

			if (jsonBody != null && !jsonBody.isEmpty()) {
				byte[] input = jsonBody.getBytes("UTF-8");
				conn.setRequestProperty("Content-Length", String.valueOf(input.length));
				try (OutputStream os = conn.getOutputStream()) {
					os.write(input);
				}
			} else {
				conn.setRequestProperty("Content-Length", "0");
				try (OutputStream os = conn.getOutputStream()) {
				}
			}

			int responseCode = conn.getResponseCode();
			hashMap.put("ResponseCode", responseCode);

			InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();

			StringBuilder response = new StringBuilder();
			if (is != null) {
				try (BufferedReader in = new BufferedReader(new InputStreamReader(is))) {
					String inputLine;
					while ((inputLine = in.readLine()) != null) {
						response.append(inputLine);
					}
				}
			}
			hashMap.put("ResponseBody", response.toString());

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			hashMap.put("ResponseBody", "Exception: " + e.getMessage());
			hashMap.put("ResponseCode", -1);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return hashMap;
	}

	/**
	 * To return spreadsheet id by title given by user on spreadsheet
	 *
	 * @param titleSheetName
	 * @param accessToken
	 * @return spreadsheetid
	 */
	private static String getSpreadsheetIdByTitle(String titleSheetName, String accessToken) {
		try {
			String query = "name='" + titleSheetName + "' and mimeType='application/vnd.google-apps.spreadsheet'";
			String encodedQuery = URLEncoder.encode(query, "UTF-8");
			String urlStr = "https://www.googleapis.com/drive/v3/files" + "?q=" + encodedQuery
					+ "&fields=files(id,name)" + "&spaces=drive";

			URL url = new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);

			int responseCode = conn.getResponseCode();
			InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();

			BufferedReader in = new BufferedReader(new InputStreamReader(is));
			String inputLine;
			StringBuilder response = new StringBuilder();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			if (responseCode < 200 || responseCode >= 300) {
				classLogger.info("Error response: " + response.toString());
			}

			JSONObject jsonResponse = new JSONObject(response.toString());
			JSONArray files = jsonResponse.optJSONArray("files");
			if (files != null && files.length() > 0) {
				return files.getJSONObject(0).getString("id");
			} else {
				return "No spreadsheet found with that title";
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String msg = "Exception in getSpreadsheetIdByTitle() method with error: " + e.getMessage();
			return msg;
		}

	}

	/**
	 * To update data in spreadsheet using titlesheetid and sheetId.
	 *
	 * @param titlesheetid The spreadsheet ID.
	 * @param sheetId      The sheet/tab ID (as String).
	 * @param data         The data to write (List<List<String>>).
	 * @param accessToken  The Google API access token.
	 * @return NounMetadata with the result message.
	 */
	public static NounMetadata updateData(String titlesheetid, String sheetId, List<List<String>> data,
			String accessToken) {
		try {
			// 1. Get the sheet name from the sheetId
			String metaUrl = SHEET_URL + titlesheetid + "?fields=sheets(properties(sheetId,title))";
			String metaResponse = sendGetRequest(metaUrl, accessToken);
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

			if (sheetName == null) {
				return new NounMetadata("Sheet ID not found in spreadsheet.", PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}

			// 2. Clear the entire sheet first
			String clearRange = sheetName; // This clears the whole sheet
			String clearUrl = SHEET_URL + titlesheetid + "/values/" + URLEncoder.encode(clearRange, "UTF-8") + ":clear";
			sendPostRequest(clearUrl, "", accessToken);

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

			HashMap<String, Object> responseMessage = sendPutRequest(url, body.toString(), accessToken);

			Object responseBody = responseMessage.get("ResponseBody");
			int responseCode = (int) responseMessage.get("ResponseCode");

			JSONObject jsonResponse = new JSONObject(responseBody.toString());

			String msg;
			if (responseCode >= 200 && responseCode < 300) {
				if (jsonResponse.has("updatedCells") && jsonResponse.getInt("updatedCells") > 0) {
					msg = "All data replaced successfully";
				} else if (jsonResponse.has("updatedCells")) {
					msg = "No cells were updated. Response: " + jsonResponse.toString();
				} else {
					msg = "Response did not contain 'updatedCells'. Full response: " + jsonResponse.toString();
				}
			} else {
				msg = "Failed to update data. HTTP code: " + responseCode + ". Response: " + jsonResponse.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To send get request
	 *
	 * @param url
	 * @param accessToken
	 */
	private static String sendGetRequest(String url, String accessToken) {
		StringBuilder content = new StringBuilder();
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			int responseCode = conn.getResponseCode();
			InputStream is = (responseCode == 200) ? conn.getInputStream() : conn.getErrorStream();
			BufferedReader in = new BufferedReader(new InputStreamReader(is));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				content.append(inputLine);
			}
			in.close();
			conn.disconnect();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return content.toString();
	}

	/**
	 * To send put request
	 *
	 * @param url
	 * @param jsonBody
	 * @param accessToken
	 */
	private static HashMap<String, Object> sendPutRequest(String url, String jsonBody, String accessToken) {
		HashMap<String, Object> hashMap = new HashMap<>();
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("PUT");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setDoOutput(true);

			try (OutputStream os = conn.getOutputStream()) {
				os.write(jsonBody.getBytes("UTF-8"));
			}

			int responseCode = conn.getResponseCode();
			hashMap.put("ResponseCode", responseCode);

			InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();

			StringBuilder response = new StringBuilder();
			if (is != null) {
				try (BufferedReader in = new BufferedReader(new InputStreamReader(is))) {
					String inputLine;
					while ((inputLine = in.readLine()) != null) {
						response.append(inputLine);
					}
				}
			}
			hashMap.put("ResponseBody", response.toString());

			if (responseCode < 200 || responseCode >= 300) {
				throw new IOException("PUT request failed: " + responseCode + " - " + response.toString());
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			hashMap.put("ResponseBody", "Exception: " + e.getMessage());
			hashMap.put("ResponseCode", -1);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return hashMap;
	}

	/**
	 * Reads data from a Google Sheet using spreadsheetId and sheetId. Checks if the
	 * sheetId is valid before reading data.
	 */
	public static NounMetadata readData(String spreadsheetId, String sheetId, String accessToken) {
		try {
			// 1. Get the sheet name from the sheetId
			String metaUrl = SHEET_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
			SpreadSheetHelper helper = new SpreadSheetHelper();
			String metaResponse = SpreadSheetHelper.sendGetRequest(metaUrl, accessToken);
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
			String response = SpreadSheetHelper.sendGetRequest(url, accessToken);
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
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To delete a sheet from a spreadsheet using titlesheetid and sheetId.
	 *
	 * @param titlesheetid The spreadsheet ID.
	 * @param sheetId      The sheet/tab ID (as Integer or String).
	 * @param accessToken  The Google API access token.
	 * @return NounMetadata with the result message.
	 */
	public static NounMetadata deleteSheet(String titlesheetid, String sheetId, String accessToken) {
		try {

			// Prepare batchUpdate request body
			JSONObject deleteSheetRequest = new JSONObject();
			deleteSheetRequest.put("deleteSheet", new JSONObject().put("sheetId", Integer.parseInt(sheetId)));

			JSONArray requests = new JSONArray();
			requests.put(deleteSheetRequest);

			JSONObject body = new JSONObject();
			body.put("requests", requests);

			// Send batchUpdate request
			String batchUpdateUrl = SHEET_URL + titlesheetid + ":batchUpdate";
			URL batchUrl = new URL(batchUpdateUrl);
			HttpURLConnection batchConn = (HttpURLConnection) batchUrl.openConnection();
			batchConn.setRequestMethod("POST");
			batchConn.setRequestProperty("Authorization", "Bearer " + accessToken);
			batchConn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			batchConn.setDoOutput(true);

			OutputStream os = batchConn.getOutputStream();
			os.write(body.toString().getBytes("UTF-8"));
			os.close();

			int batchResponseCode = batchConn.getResponseCode();
			InputStream batchIs = (batchResponseCode >= 200 && batchResponseCode < 300) ? batchConn.getInputStream()
					: batchConn.getErrorStream();

			BufferedReader batchIn = new BufferedReader(new InputStreamReader(batchIs));
			String inputLine;
			StringBuilder batchResponse = new StringBuilder();
			while ((inputLine = batchIn.readLine()) != null) {
				batchResponse.append(inputLine);
			}
			batchIn.close();

			String msg;
			if (batchResponseCode >= 200 && batchResponseCode < 300) {
				msg = "Sheet with ID '" + sheetId + "' deleted successfully from spreadsheet '" + titlesheetid + "'.";
			} else {
				msg = "Failed to delete sheet. HTTP code: " + batchResponseCode + ". Response: "
						+ batchResponse.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To return list of spreadsheets
	 *
	 * @param accessToken
	 * @param spreadSheetIds
	 */
	public static List<String> getSheetName(String accessToken, List<String> spreadSheetIds) {
		List<String> allSheetNames = new ArrayList<>();
		try {
			for (String spreadsheetId : spreadSheetIds) {
				String sheetsUrl = SHEET_URL + spreadsheetId;
				HttpURLConnection sheetsConn = (HttpURLConnection) new URL(sheetsUrl).openConnection();
				sheetsConn.setRequestMethod("GET");
				sheetsConn.setRequestProperty("Authorization", "Bearer " + accessToken);

				BufferedReader sheetsIn = new BufferedReader(new InputStreamReader(sheetsConn.getInputStream()));
				StringBuilder sheetsResponse = new StringBuilder();
				String sheetsInputLine;
				while ((sheetsInputLine = sheetsIn.readLine()) != null) {
					sheetsResponse.append(sheetsInputLine);
				}
				sheetsIn.close();

				JSONObject sheetsJson = new JSONObject(sheetsResponse.toString());
				JSONArray sheets = sheetsJson.getJSONArray("sheets");
				for (int j = 0; j < sheets.length(); j++) {
					JSONObject sheet = sheets.getJSONObject(j).getJSONObject("properties");
					String sheetTitle = sheet.getString("title");
					allSheetNames.add(sheetTitle);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return allSheetNames;
	}

	/**
	 * To return list of spreadsheetIds
	 *
	 * @param accessToken
	 * @return
	 */
	public static List<String> getSpreadSheetId(String accessToken) {
		List<String> spreadsheetIds = new ArrayList<>();
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(SPREADDRIVE_URL).openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			StringBuilder response = new StringBuilder();
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			JSONObject json = new JSONObject(response.toString());
			JSONArray files = json.getJSONArray("files");
			for (int i = 0; i < files.length(); i++) {
				JSONObject file = files.getJSONObject(i);
				String spreadsheetId = file.getString("id");
				spreadsheetIds.add(spreadsheetId);
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return spreadsheetIds;
	}

	/**
	 * Creates a new Google Spreadsheet with the given title, if it does not already
	 * exist.
	 *
	 * @param titleSheetName The desired spreadsheet title.
	 * @param accessToken    The OAuth2 access token for Google Sheets API.
	 * @return NounMetadata containing a SpreadSheetResponse object with
	 *         spreadsheetId, sheetId, and status.
	 */
	public static NounMetadata createnewSpreadSheet(String titleSheetName, String accessToken) {
		try {
			boolean isTitleSheetNamePresent = validateTitleSheetName(titleSheetName, accessToken);
			if (!isTitleSheetNamePresent) {
				String msg = "A Spreadsheet with this title already exists";
				SpreadSheetResponse resp = new SpreadSheetResponse();
				resp.setStatus(false);
				resp.setTitleSheetID(null);
				resp.setSheetID(null);
				return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			URL url = new URL(SHEET_URL);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			conn.setDoOutput(true);

			JSONObject properties = new JSONObject();
			properties.put("title", titleSheetName);

			JSONObject body = new JSONObject();
			body.put("properties", properties);

			OutputStream os = conn.getOutputStream();
			os.write(body.toString().getBytes("UTF-8"));
			os.close();

			int responseCode = conn.getResponseCode();
			InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();

			BufferedReader in = new BufferedReader(new InputStreamReader(is));
			String inputLine;
			StringBuilder response = new StringBuilder();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			JSONObject jsonResponse = new JSONObject(response.toString());
			SpreadSheetResponse resp = new SpreadSheetResponse();

			if (responseCode >= 200 && responseCode < 300) {
				String spreadsheetId = jsonResponse.optString("spreadsheetId", "");
				// Get the first sheet's ID if available
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
				}
				resp.setTitleSheetID(spreadsheetId);
				resp.setSheetID(sheetId);
				resp.setStatus(true);
			} else {
				resp.setStatus(false);
				resp.setTitleSheetID(null);
				resp.setSheetID(null);
			}

			return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			SpreadSheetResponse resp = new SpreadSheetResponse();
			resp.setStatus(false);
			resp.setTitleSheetID(null);
			resp.setSheetID(null);
			return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
	}

	/**
	 * To validate title sheet name from spreadsheet
	 *
	 * @param titleSheetName
	 * @param accessToken
	 */
	private static boolean validateTitleSheetName(String titleSheetName, String accessToken) {
		try {
			Drive driveService = getDriveService(accessToken);
			String query = TITLESHEET_NAMEURL;
			FileList result = driveService.files().list().setQ(query).setFields("files(name)").execute();

			for (com.google.api.services.drive.model.File file : result.getFiles()) {
				if (file.getName().equalsIgnoreCase(titleSheetName)) {
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false; // Or true, depending on your error policy
		}
	}

	private static Drive getDriveService(String accessToken) {
		try {
			return new Drive.Builder(GoogleNetHttpTransport.newTrustedTransport(), JacksonFactory.getDefaultInstance(),
					request -> request.getHeaders().setAuthorization("Bearer " + accessToken))
					.setApplicationName("Your Application Name").build();
		} catch (GeneralSecurityException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		}
	}

	/**
	 * To create a new sheet (if not present) and add data to it using titlesheetid.
	 * Throws an error if a sheet with the new name already exists.
	 *
	 * @param titlesheetid The spreadsheet ID.
	 * @param sheetName    The sheet name.
	 * @param accessToken  The Google API access token.
	 * @param data         The data to add to the sheet.
	 * @return NounMetadata containing a SpreadSheetResponse object with
	 *         spreadsheetId, sheetId, and status.
	 */
	public static NounMetadata createnewSheet(String titlesheetid, String sheetName, String accessToken,
			List<List<String>> data) {

		SpreadSheetResponse resp = new SpreadSheetResponse();

		try {
			if (titlesheetid == null || titlesheetid.isEmpty()) {
				resp.setStatus(false);
				resp.setTitleSheetID(null);
				resp.setSheetID(null);
				return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			// 1. Get spreadsheet metadata to check for existing sheet
			String getUrl = SHEET_URL + titlesheetid + "?fields=sheets.properties";
			URL url = new URL(getUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);

			int responseCode = conn.getResponseCode();
			InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();
			BufferedReader in = new BufferedReader(new InputStreamReader(is));
			String inputLine;
			StringBuilder response = new StringBuilder();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			JSONObject jsonResponse = new JSONObject(response.toString());
			JSONArray sheets = jsonResponse.optJSONArray("sheets");

			if (sheets != null) {
				for (int i = 0; i < sheets.length(); i++) {
					JSONObject properties = sheets.getJSONObject(i).getJSONObject("properties");
					if (sheetName.equals(properties.optString("title"))) {
						// Sheet with the same name exists, throw error
						throw new IllegalArgumentException("A sheet with the name '" + sheetName
								+ "' already exists. Two sheets cannot have the same name.");
					}
				}
			}

			// 2. Create the new sheet
			String batchUpdateUrl = SHEET_URL + titlesheetid + ":batchUpdate";
			url = new URL(batchUpdateUrl);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			conn.setDoOutput(true);

			JSONObject addSheetRequest = new JSONObject();
			JSONObject sheetProperties = new JSONObject();
			sheetProperties.put("title", sheetName);
			addSheetRequest.put("addSheet", new JSONObject().put("properties", sheetProperties));

			JSONArray requests = new JSONArray();
			requests.put(addSheetRequest);

			JSONObject body = new JSONObject();
			body.put("requests", requests);

			OutputStream os = conn.getOutputStream();
			os.write(body.toString().getBytes("UTF-8"));
			os.close();

			responseCode = conn.getResponseCode();
			is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();

			in = new BufferedReader(new InputStreamReader(is));
			response = new StringBuilder();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			jsonResponse = new JSONObject(response.toString());
			String sheetId = null;
			if (responseCode >= 200 && responseCode < 300) {
				// Extract the new sheetId from the response
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
				resp.setStatus(true);
			} else {
				resp.setStatus(false);
				resp.setTitleSheetID(titlesheetid);
				resp.setSheetID(null);
				return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

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

			HttpURLConnection updateConn = (HttpURLConnection) new URL(updateUrl).openConnection();
			updateConn.setRequestMethod("PUT");
			updateConn.setRequestProperty("Authorization", "Bearer " + accessToken);
			updateConn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			updateConn.setDoOutput(true);

			OutputStream updateOs = updateConn.getOutputStream();
			updateOs.write(dataBody.toString().getBytes("UTF-8"));
			updateOs.close();

			int updateResponseCode = updateConn.getResponseCode();
			InputStream updateIs = (updateResponseCode >= 200 && updateResponseCode < 300) ? updateConn.getInputStream()
					: updateConn.getErrorStream();
			BufferedReader updateIn = new BufferedReader(new InputStreamReader(updateIs));
			StringBuilder updateResponse = new StringBuilder();
			while ((inputLine = updateIn.readLine()) != null) {
				updateResponse.append(inputLine);
			}
			updateIn.close();

			if (updateResponseCode >= 200 && updateResponseCode < 300) {
				resp.setStatus(true);
			} else {
				resp.setStatus(false);
			}

			return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			resp.setStatus(false);
			resp.setTitleSheetID(null);
			resp.setSheetID(null);
			return new NounMetadata(resp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
	}

	/**
	 * To delete a spreadsheet using its titlesheetid (spreadsheet ID).
	 *
	 * @param titlesheetid The spreadsheet ID.
	 * @param accessToken  The Google API access token.
	 * @return NounMetadata with the result message.
	 */
	public static NounMetadata deleteTitleSheet(String titlesheetid, String accessToken) {
		String msg = null;
		try {
			// Delete the spreadsheet using the Drive API
			String urlStr = GOOGLEDRIVE_URL + titlesheetid;
			URL url = new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("DELETE");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);

			int responseCode = conn.getResponseCode();

			if (responseCode == 204) { // 204 No Content means success
				msg = "Spreadsheet with ID '" + titlesheetid + "' deleted successfully.";
			} else {
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
				String inputLine;
				StringBuilder response = new StringBuilder();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
				msg = "Failed to delete spreadsheet. HTTP code: " + responseCode + ". Response: " + response.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			msg = e.getMessage();
			return new NounMetadata("Spreadsheet not deleted. Error: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	public static NounMetadata deleteAll(String titleSheetName, String sheetName, String accessToken) {
		try {
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			String url = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(sheetName, "UTF-8") + ":clear";
			sendPostRequest(url, "", accessToken);
			return new NounMetadata("All Cells cleared successfully", PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Spreadsheet not deleted. Error: " + e.getMessage(),
					PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
	}

}