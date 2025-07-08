package prerna.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SpreadSheetHelper {

	private static final String SPREADDRIVE_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'&fields=files(id,name)";
	private static final String SHEET_URL = "https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String GOOGLEDRIVE_URL = "https://www.googleapis.com/drive/v3/files/";
	private static final String USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo";
	private static final Logger classLogger = LogManager.getLogger(SpreadSheetHelper.class);

	/**
	 * To write data in spreadsheet given by user
	 * @param titleSheetName
	 * @param sheetName
	 * @param rowNo
	 * @param colNo
	 * @param data
	 * @param accessToken
	 * @return
	 */
	public static NounMetadata writeData(String titleSheetName, String sheetName, String rowNo, String colNo,
			String data, String accessToken) {
		try {
			String msg = null;
			String missingFields = findMissingFields(titleSheetName, sheetName, rowNo, colNo, data, accessToken);
			if (!missingFields.isEmpty()) {
				return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			// 1. Resolve spreadsheetId from titleSheetName
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			if (spreadsheetId == null || spreadsheetId.isEmpty()) {
				return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
			String range = sheetName + "!" + cell;
			String urlStr = SHEET_URL + spreadsheetId + "/values/" + range + "?valueInputOption=RAW";
			URL url = new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("PUT");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			conn.setDoOutput(true);

			// Prepare the values as a 2D array
			JSONArray values = new JSONArray();
			JSONArray row = new JSONArray();
			row.put(data);
			values.put(row);

			JSONObject body = new JSONObject();
			body.put("range", range);
			body.put("majorDimension", "ROWS");
			body.put("values", values);

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

	/**
	 * To return spreadsheet id by title given by user on spreadsheet
	 * @param titleSheetName
	 * @param accessToken
	 * @return spreadsheetid
	 */
	private static String getSpreadsheetIdByTitle(String titleSheetName, String accessToken) {
		try {
		    // 1. Build the full query string (not just the title)
		    String query = "name='" + titleSheetName + "' and mimeType='application/vnd.google-apps.spreadsheet'";
		    // 2. Encode the entire query string
		    String encodedQuery = URLEncoder.encode(query, "UTF-8");
		    // 3. Construct the URL with the encoded query
		    String urlStr = "https://www.googleapis.com/drive/v3/files"
		            + "?q=" + encodedQuery
		            + "&fields=files(id,name)"
		            + "&spaces=drive";

		    URL url = new URL(urlStr);
		    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		    conn.setRequestMethod("GET");
		    conn.setRequestProperty("Authorization", "Bearer " + accessToken);

		    int responseCode = conn.getResponseCode();
		    InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();

		    BufferedReader in = new BufferedReader(new InputStreamReader(is));
		    String inputLine;
		    StringBuilder response = new StringBuilder();
		    while ((inputLine = in.readLine()) != null) {
		        response.append(inputLine);
		    }
		    in.close();

		    // Debug: print the response if not 2xx
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
	 * To return error with missing fields
	 * @param titleSheetName
	 * @param sheetName
	 * @param rowNo
	 * @param colNo
	 * @param data
	 * @param accessToken
	 * @return String with missing fields
	 */
	private static String findMissingFields(String titleSheetName, String sheetName, String rowNo, String colNo,
			String data, String accessToken) {
		StringBuilder errorBuilder = new StringBuilder();
		if (titleSheetName == null || titleSheetName.isEmpty()) {
			errorBuilder.append("titleSheetName, ");
		}
		if (sheetName == null || sheetName.isEmpty()) {
			errorBuilder.append("sheetName, ");
		}
		if (rowNo == null || rowNo.isEmpty()) {
			errorBuilder.append("rowNo, ");
		}
		if (colNo == null || colNo.isEmpty()) {
			errorBuilder.append("colNo, ");
		}
		if (data == null || data.isEmpty()) {
			errorBuilder.append("data, ");
		}
		if (accessToken == null || accessToken.isEmpty()) {
			errorBuilder.append("accessToken, ");
		}
		String error = errorBuilder.toString().replaceAll("$", "");
		return error;
	}

	/**
	 * To update data in spreadsheet
	 * @param titleSheetName
	 * @param sheetName
	 * @param rowNo
	 * @param colNo
	 * @param data
	 * @param accessToken
	 * @return 
	 */
	public static NounMetadata updateData(String titleSheetName, String sheetName, String rowNo, String colNo,
			String data, String accessToken) {
		try {
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			if (spreadsheetId == null) {
				return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
			String range = sheetName + "!" + cell;
			String missingFields = findMissingFields(titleSheetName, sheetName, rowNo, colNo, "Not required", "Not required");
			if (!missingFields.isEmpty()) {
				return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			String urlStr = SHEET_URL + spreadsheetId + "/values/" + range + "?valueInputOption=RAW";
			URL url = new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("PUT");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			conn.setDoOutput(true);

			// Prepare the values as a 2D array
			JSONArray values = new JSONArray();
			JSONArray row = new JSONArray();
			row.put(data);
			values.put(row);

			JSONObject body = new JSONObject();
			body.put("range", range);
			body.put("majorDimension", "ROWS");
			body.put("values", values);

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
				if (jsonResponse.has("updatedCells") && jsonResponse.getInt("updatedCells") > 0) {
					msg = "Data updated successfully";
				} else {
					msg = "No cells were updated. Response: " + jsonResponse.toString();
				}
			} else {
				msg = "Failed to update data. HTTP code: " + responseCode + ". Response: " + jsonResponse.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}
	/**
	 * To delete data in spreadsheet
	 * @param titleSheetName
	 * @param sheetName
	 * @param rowNo
	 * @param colNo
	 * @param accessToken
	 * @return 
	 */
	public static NounMetadata deleteData(String titleSheetName, String sheetName, String rowNo, String colNo,
			String accessToken) {
		try {
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			if (spreadsheetId == null) {
				return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
			String range = sheetName + "!" + cell;
			String missingFields = findMissingFields(titleSheetName, "Not required", "Not required", "Not required", "Not required", accessToken);
			if (!missingFields.isEmpty()) {
				return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			String urlStr = SHEET_URL + spreadsheetId + "/values/" + range + ":clear";
			URL url = new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			conn.setDoOutput(true);

			// The clear endpoint expects an empty JSON object as the body
			OutputStream os = conn.getOutputStream();
			os.write("{}".getBytes("UTF-8"));
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

			String msg;
			if (responseCode >= 200 && responseCode < 300) {
				msg = "Cell cleared successfully";
			} else {
				msg = "Failed to clear cell. HTTP code: " + responseCode + ". Response: " + response.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To read data from spreadsheet
	 * @param titleSheetName
	 * @param sheetName
	 * @param rowNo
	 * @param colNo
	 * @param data
	 * @param accessToken
	 * @return 
	 */
	public static NounMetadata readData(String titleSheetName, String sheetName, String rowNo, String colNo,
			String data, String accessToken) {
		try {
			String missingFields = findMissingFields(titleSheetName, sheetName, rowNo, colNo, data, accessToken);
			if (!missingFields.isEmpty()) {
				return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			if (spreadsheetId == null) {
				return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String cell = SheetServiceUtil.getA1Notation(Integer.parseInt(rowNo), Integer.parseInt(colNo));
			String range = sheetName + "!" + cell;
			String urlStr = SHEET_URL + spreadsheetId + "/values/" + range;
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
			JSONArray values = jsonResponse.optJSONArray("values");
			String msg;
			if (values != null && values.length() > 0) {
				JSONArray row = values.getJSONArray(0);
				if (row.length() > 0) {
					msg = row.getString(0);
				} else {
					msg = "No data found in the specified cell.";
				}
			} else {
				msg = "No data found in the specified cell.";
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To delete sheet from spreadsheet
	 * @param titleSheetName
	 * @param sheetName
	 * @param accessToken
	 * @return 
	 */
	public static NounMetadata deleteSheet(String titleSheetName, String sheetName, String accessToken) {
		try {
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			if (spreadsheetId == null) {
				return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String missingFields = findMissingFields(titleSheetName, sheetName, "Not required", "Not required", "Not required", accessToken);
			if (!missingFields.isEmpty()) {
				return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			// 1. Get the sheet ID for the given sheet name
			String getSheetsUrl = "https://sheets.googleapis.com/v4/spreadsheets/" + spreadsheetId
					+ "?fields=sheets.properties";
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

			// 2. Prepare the batchUpdate request to delete the sheet
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
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To return list of spreadsheets
	 * @param accessToken
	 * @param spreadSheetIds
	 * @return 
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
	 * @param titleSheetName
	 * @param accessToken
	 * @return 
	 */
	public static NounMetadata createnewSpreadSheet(String titleSheetName, String accessToken) {
		try {
			String missingFields = findMissingFields(titleSheetName, "Not required", "Not required", "Not required", "Not required", accessToken);
			if (!missingFields.isEmpty()) {
				return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			URL url = new URL(SHEET_URL);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			conn.setDoOutput(true);
			// Prepare the request body with the desired title
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
				// Update the spreadsheetId in your database
				boolean updateSuccess = updateSpreadsheetIdInDatabase(spreadsheetId,accessToken);
				 if (updateSuccess) {
		                msg = "Spreadsheet created and spid updated in DB: " + spreadsheetId;
		            } else {
		                msg = "Spreadsheet created, but failed to update spid in DB: " + spreadsheetId;
		            }
			} else {
				msg = "Failed to create spreadsheet. HTTP code: " + responseCode + ". Response: "
						+ jsonResponse.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To update spreadsheet id in db
	 * @param spreadsheetId
	 * @param accessToken
	 * @return 
	 */
	private static boolean updateSpreadsheetIdInDatabase(String spreadsheetId, String accessToken) {
		try {
			String email = getEmailFromAccessToken(accessToken);
			AbstractSecurityUtils.updateSpId(spreadsheetId, email);
			return true;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}
	}

	/**
	 * To create new sheet
	 * @param spreadsheetId
	 * @param accessToken
	 * @return 
	 */
	public static NounMetadata createnewSheet(String titleSheetName, String sheetName, String accessToken) {
		try {
			String spreadsheetId = getSpreadsheetIdByTitle(titleSheetName, accessToken);
			if (spreadsheetId == null) {
				return new NounMetadata("Spreadsheet not found for title: " + titleSheetName,
						PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String missingFields = findMissingFields(titleSheetName,sheetName, "Not required", "Not required", "Not required", accessToken);
			if (!missingFields.isEmpty()) {
				return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}
			String batchUpdateUrl = SHEET_URL + spreadsheetId + ":batchUpdate";
			URL url = new URL(batchUpdateUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			conn.setDoOutput(true);

			// Prepare the batchUpdate request to add a new sheet
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
				msg = "Sheet '" + sheetName + "' created successfully in spreadsheet '" + titleSheetName + "'.";
			} else {
				msg = "Failed to create sheet. HTTP code: " + responseCode + ". Response: " + jsonResponse.toString();
			}

			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			return new NounMetadata("Exception: " + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To truncate all data from DB
	 * @param accessToken
	 * @return 
	 */
	public static NounMetadata truncateData(String accessToken) {
		String msg = null;
		try {
			IDatabaseEngine securityDb = Utility.getDatabase(Constants.SECURITY_DB);
			AbstractSecurityUtils.deleteGoogleUserDB();
			String email = getEmailFromAccessToken(accessToken);
			msg = "Table truncated successfully by user with email: " + email;
			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			msg = e.getMessage();
			return new NounMetadata("Data not truncated. Error: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To get email from access token
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
	 * To delete record of logged in  user id from db
	 * @param accessToken
	 * @return 
	 */
	public static NounMetadata deleteRecordUserId(String accessToken) {
		String msg = null;
		try {
			IDatabaseEngine securityDb = Utility.getDatabase(Constants.SECURITY_DB);
			List<String> tables = securityDb.getPixelConcepts();
			String email = getEmailFromAccessToken(accessToken);
			AbstractSecurityUtils.deleteGoogleUser(email);
			msg = "Data truncated successfully by user with email: " + email;
			return new NounMetadata(msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			msg = e.getMessage();
			return new NounMetadata("Data not truncated. Error: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	/**
	 * To delete title spreadsheet
	 * @param titleSheetName
	 * @param accessToken
	 * @return 
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
	        String missingFields = findMissingFields(titleSheetName,"Not required", "Not required", "Not required", "Not required", accessToken);
			if (!missingFields.isEmpty()) {
				return new NounMetadata(missingFields, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
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
	        msg = e.getMessage();
	        return new NounMetadata("Spreadsheet not deleted. Error: " + msg, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	    }
	}

}
