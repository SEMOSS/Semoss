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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.FileList;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SpreadSheetHelper {

	private static final String SPREADDRIVE_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'&fields=files(id,name)";
	private static final String SHEET_URL = "https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String GOOGLEDRIVE_URL = "https://www.googleapis.com/drive/v3/files/";
	private static final String USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo";
	private static final String TITLESHEET_NAMEURL = "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false";
	private static final Logger classLogger = LogManager.getLogger(SpreadSheetHelper.class);

	/**
	 * To write data in spreadsheet given by user
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
			if (conn != null)
				conn.disconnect();
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
	 * To update data in spreadsheet
	 * @param titleSheetName
	 * @param sheetName
	 * @param data
	 * @param accessToken
	 * @return
	 */
	public static NounMetadata updateData(String titleSheetName, String sheetName, List<List<String>> data, String accessToken) {
	    try {
	        String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);

	        if (spreadsheetId == null) {
	            return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
	                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	        }

	        // 1. Clear the entire sheet first
	        String clearRange = sheetName; // This clears the whole sheet
	        String clearUrl = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(clearRange, "UTF-8") + ":clear";
	        sendPostRequest(clearUrl, "", accessToken);

	        // 2. Write new data starting from A1
	        int numRows = data.size();
	        int numCols = numRows > 0 ? data.get(0).size() : 0;
	        String startCell = "A1";
	        String endCell = getCellReference(numCols - 1, numRows - 1);
	        String range = sheetName + "!" + startCell;
	        if (numRows > 1 || numCols > 1) {
	            range += ":" + endCell;
	        }

	        String url = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(range, "UTF-8")
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
	 * To find cell
	 * @param spreadsheetId
	 * @param sheetName
	 * @param data
	 * @param accessToken
	 * @return CellRef
	 */
	private CellRef findCell(String spreadsheetId, String sheetName, List<List<String>> data, String accessToken) {
		try {
			String url = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(sheetName, "UTF-8");

			String response = sendGetRequest(url, accessToken);

			JSONObject json = new JSONObject(response);
			JSONArray values = json.optJSONArray("values");
			if (values == null)
				return null;

			// Flatten the search data for easy matching
			Set<String> searchSet = new HashSet<String>();
			for (List<String> row : data) {
				searchSet.addAll(row);
			}

			for (int i = 0; i < values.length(); i++) {
				JSONArray row = values.getJSONArray(i);
				for (int j = 0; j < row.length(); j++) {
					if (searchSet.contains(row.getString(j))) {
						return new CellRef(i, j);
					}
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	/**
	 * To send get request
	 * @param url
	 * @param accessToken
	 */
	private String sendGetRequest(String url, String accessToken) {
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
			if (conn != null)
				conn.disconnect();
		}
		return hashMap;
	}

	/**
	 * To read data from spreadsheet
	 * @param titleSheetName
	 * @param sheetName
	 * @param accessToken
	 */
	public static NounMetadata readData(String titleSheetName, String sheetName, String accessToken) {
		try {
			String msg = null;
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			String url = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(sheetName, "UTF-8");
			SpreadSheetHelper helper = new SpreadSheetHelper();
			String response = helper.sendGetRequest(url, accessToken);
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
	 * To delete sheet from spreadsheet
	 * @param titleSheetName
	 * @param accessToken
	 * @param sheetName
	 */
	public static NounMetadata deleteSheet(String titleSheetName, String sheetName, String accessToken) {
		try {
			IDatabaseEngine securityDb = Utility.getDatabase(Constants.SECURITY_DB);
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			if (spreadsheetId == null) {
				return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String getSheetsUrl = SHEET_URL + spreadsheetId + "?fields=sheets.properties";
			URL url = new URL(getSheetsUrl);
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
			Integer sheetId = null;
			if (sheets != null) {
				for (int i = 0; i < sheets.length(); i++) {
					JSONObject properties = sheets.getJSONObject(i).getJSONObject("properties");
					if (sheetName.equals(properties.getString("title"))) {
						sheetId = properties.getInt("sheetId");
						break;
					}
				}
			}
			if (sheetId == null) {
				return new NounMetadata("Sheet not found: " + sheetName, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}

			String batchUpdateUrl = SHEET_URL + spreadsheetId + ":batchUpdate";
			URL batchUrl = new URL(batchUpdateUrl);
			HttpURLConnection batchConn = (HttpURLConnection) batchUrl.openConnection();
			batchConn.setRequestMethod("POST");
			batchConn.setRequestProperty("Authorization", "Bearer " + accessToken);
			batchConn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			batchConn.setDoOutput(true);

			JSONObject deleteSheetRequest = new JSONObject();
			deleteSheetRequest.put("deleteSheet", new JSONObject().put("sheetId", sheetId));

			JSONArray requests = new JSONArray();
			requests.put(deleteSheetRequest);

			JSONObject body = new JSONObject();
			body.put("requests", requests);

			OutputStream os = batchConn.getOutputStream();
			os.write(body.toString().getBytes("UTF-8"));
			os.close();

			int batchResponseCode = batchConn.getResponseCode();
			InputStream batchIs = (batchResponseCode >= 200 && batchResponseCode < 300) ? batchConn.getInputStream()
					: batchConn.getErrorStream();

			BufferedReader batchIn = new BufferedReader(new InputStreamReader(batchIs));
			StringBuilder batchResponse = new StringBuilder();
			while ((inputLine = batchIn.readLine()) != null) {
				batchResponse.append(inputLine);
			}
			batchIn.close();

			String msg;
			if (batchResponseCode >= 200 && batchResponseCode < 300) {
				msg = "Sheet '" + sheetName + "' deleted successfully.";
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
				while ((sheetsInputLine = sheetsIn.readLine()) != null)
					sheetsResponse.append(sheetsInputLine);
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
			while ((inputLine = in.readLine()) != null)
				response.append(inputLine);
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
	 * To create new spreadsheet
	 * 
	 * @param accessToken
	 * @param titleSheetName
	 */
	public static NounMetadata createnewSpreadSheet(String titleSheetName, String accessToken) {
		try {
			boolean isTitleSheetNamePresent = validateTitleSheetName(titleSheetName, accessToken);
			if (Boolean.FALSE.equals(isTitleSheetNamePresent)) {
				String msg = "A Spreadsheet with this title already exist";
				return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
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
			String msg;
			if (responseCode >= 200 && responseCode < 300) {
				String spreadsheetId = jsonResponse.optString("spreadsheetId", "");
				msg = "Spreadsheet created: " + spreadsheetId;
			} else {
				msg = "Failed to create spreadsheet. HTTP code: " + responseCode + ". Response: "
						+ jsonResponse.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To validate title sheet name from spreadsheet
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
	 * To create new sheet
	 * @param data 
	 * @param accessToken 
	 * @param sheetName 
	 * @param titleSheetName 
	 */
	public static NounMetadata createnewSheet(String titleSheetName, String sheetName, String accessToken, List<List<String>> data) {
	    try {
	        String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
	        if (spreadsheetId == null) {
	            return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
	                    PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	        }

	        // 1. Create the new sheet
	        String batchUpdateUrl = SHEET_URL + spreadsheetId + ":batchUpdate";
	        URL url = new URL(batchUpdateUrl);
	        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
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

	        int responseCode = conn.getResponseCode();
	        InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();

	        BufferedReader in = new BufferedReader(new InputStreamReader(is));
	        String inputLine;
	        StringBuilder response = new StringBuilder();
	        while ((inputLine = in.readLine()) != null) {
	            response.append(inputLine);
	        }
	        in.close();

	        JSONObject jsonResponse = new JSONObject(response.toString());
	        String msg;
	        if (responseCode >= 200 && responseCode < 300) {
	            msg = "Sheet '" + sheetName + "' created successfully in spreadsheet '" + titleSheetName + "'.";
	        } else {
	            msg = "Failed to create sheet. HTTP code: " + responseCode + ". Response: " + jsonResponse.toString();
	            return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	        }

	        // 2. Add data to the new sheet
	        // Wait a moment to ensure the sheet is available (optional, but sometimes needed)
	        Thread.sleep(1000);

	        // Prepare the range (e.g., Sheet1!A1:D10)
	        int numRows = data.size();
	        int numCols = numRows > 0 ? data.get(0).size() : 0;
	        String startCell = "A1";
	        String endCell = getCellReference(numCols - 1, numRows - 1);
	        String range = sheetName + "!" + startCell;
	        if (numRows > 1 || numCols > 1) {
	            range += ":" + endCell;
	        }

	        String updateUrl = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(range, "UTF-8")
	                + "?valueInputOption=USER_ENTERED";
	        JSONObject dataBody = new JSONObject();
	        dataBody.put("values", new JSONArray(data));

	        // Send the PUT request to add data
	        HttpURLConnection updateConn = (HttpURLConnection) new URL(updateUrl).openConnection();
	        updateConn.setRequestMethod("PUT");
	        updateConn.setRequestProperty("Authorization", "Bearer " + accessToken);
	        updateConn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
	        updateConn.setDoOutput(true);

	        OutputStream updateOs = updateConn.getOutputStream();
	        updateOs.write(dataBody.toString().getBytes("UTF-8"));
	        updateOs.close();

	        int updateResponseCode = updateConn.getResponseCode();
	        InputStream updateIs = (updateResponseCode >= 200 && updateResponseCode < 300) ? updateConn.getInputStream() : updateConn.getErrorStream();
	        BufferedReader updateIn = new BufferedReader(new InputStreamReader(updateIs));
	        StringBuilder updateResponse = new StringBuilder();
	        while ((inputLine = updateIn.readLine()) != null) {
	            updateResponse.append(inputLine);
	        }
	        updateIn.close();

	        JSONObject updateJsonResponse = new JSONObject(updateResponse.toString());
	        if (updateResponseCode >= 200 && updateResponseCode < 300) {
	            msg += " Data added successfully to new sheet.";
	        } else {
	            msg += " Failed to add data to new sheet. HTTP code: " + updateResponseCode + ". Response: " + updateJsonResponse.toString();
	        }

	        return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

	    } catch (Exception e) {
	        classLogger.error(Constants.STACKTRACE, e);
	        return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
	                PixelOperationType.OPERATION);
	    }
	}
	/**
	 * To get email from access token
	 * 
	 * @param accessToken
	 * @return
	 */
	private static String getEmailFromAccessToken(String accessToken) {
		try {
			String urlStr = USERINFO_URL;
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

			JSONObject jsonResponse = new JSONObject(response.toString());
			if (jsonResponse.has("email")) {
				return jsonResponse.getString("email");
			} else {
				return "Email not found in token response.";
			}
		} catch (Exception e) {
			return "Exception: " + e.getMessage();
		}
	}

	/**
	 * To delete title spreadsheet
	 * 
	 * @param accessToken
	 * @param titleSheetName
	 */
	public static NounMetadata deleteTitleSheet(String titleSheetName, String accessToken) {
		String msg = null;
		try {
			// 1. Find the spreadsheet ID by title
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			if (spreadsheetId == null) {
				msg = "Spreadsheet not found for title: " + titleSheetName;
				return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			// 2. Delete the spreadsheet using the Drive API
			String urlStr = GOOGLEDRIVE_URL + spreadsheetId;
			URL url = new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("DELETE");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);

			int responseCode = conn.getResponseCode();

			if (responseCode == 204) { // 204 No Content means success
				msg = "Spreadsheet '" + titleSheetName + "' deleted successfully.";
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

	private static class CellRef {
		int row, col;

		CellRef(int row, int col) {
			this.row = row;
			this.col = col;
		}

		String toA1() {
			return getColumnLetter(col + 1) + (row + 1);
		}

		static String getColumnLetter(int col) {
			StringBuilder sb = new StringBuilder();
			while (col > 0) {
				int rem = (col - 1) % 26;
				sb.insert(0, (char) ('A' + rem));
				col = (col - 1) / 26;
			}
			return sb.toString();
		}
	}

	public static NounMetadata deleteAll(String titleSheetName, String sheetName, String accessToken) {
		try {
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			String url = SHEET_URL + spreadsheetId + "/values/" + URLEncoder.encode(sheetName, "UTF-8") + ":clear";
			String jsonBody = "{\"values\":[]}";
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