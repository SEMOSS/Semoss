package prerna.io.connector.google;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetAllGoogleSheetsReactor extends AbstractReactor{
	private static final String SPREADSHEET_DATABASE = "6abf12ab-ae96-4edd-a1af-b56b9a37634d";
	private static final String GOOGLEDRIVE_URL="https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'&fields=files(id,name)";
	private static final String GOOGLESHEETS_URL="https://sheets.googleapis.com/v4/spreadsheets/";
	private static final String USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo";
	private static final Logger classLogger = LogManager.getLogger(GetAllGoogleSheetsReactor.class);
	
	public GetAllGoogleSheetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey()};
		this.keyRequired = new int[] { 1 };
	}


	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String tableName = null;
		String userId=null;
		ResultSet rs=null;
		List<Map<String, Object>> spreadsheets = new ArrayList<>();
		try {
			String name = this.keyValue.get(this.keysToGet[0]);
			IDatabaseEngine database = Utility.getDatabase(SPREADSHEET_DATABASE);
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			String getUserIdQuery="select userid from "+tableName+" where name='"+name+"'";
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(getUserIdQuery);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				rs = (ResultSet) string;
				while (rs.next()) {
					userId = rs.getString("userid");
				}
			}
//			String accessToken=getAccessToken();
			String accessToken="ya29.a0AS3H6Ny6T6_dU_Msmltv38mAkyh3zh3801IgI6OtU1lAzbVxkDvWupl6XuB7pJOlZ-C6ARf7T-WFWXiXwhM3-w24kooowW2lwQbvKLejiiqer1rR0M7xxBktnxaDz-9Mc8XJEnLWfRsMZQgsTWFLfPCjQB2FYUxYKZZe4ZaCaCgYKAegSARcSFQHGX2MipNSkrjr_cIES-zX7A0ypSw0175";
			String emailToken=getEmailAccessToken(accessToken);
			if(emailToken.equals(userId)) {
				 spreadsheets = fetchSpreadsheetMetadata(accessToken);
				
			}else {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", "Google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			}
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}finally {
			if(rs!=null) {
				try {
					rs.close();
				}catch(SQLException ex) {
					classLogger.error(Constants.STACKTRACE, ex);
				}
			}
		}
		return new NounMetadata(spreadsheets, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}


	private List<Map<String, Object>> fetchSpreadsheetMetadata(String accessToken) {
	    List<Map<String, Object>> spreadsheets = new ArrayList<>();
	    JSONObject driveResponse = Utility.httpGetJson(GOOGLEDRIVE_URL, accessToken);
	    JSONArray files = driveResponse.getJSONArray("files");

	    for (int i = 0; i < files.length(); i++) {
	        JSONObject file = files.getJSONObject(i);
	        String spreadsheetId = file.getString("id");
	        String spreadsheetTitle = file.getString("name");

	        // Fetch all sheet names for this spreadsheet
	        String sheetsUrl = GOOGLESHEETS_URL + spreadsheetId;
	        JSONObject sheetsResponse = Utility.httpGetJson(sheetsUrl, accessToken);
	        JSONArray sheets = sheetsResponse.getJSONArray("sheets");
	        List<String> sheetNames = new ArrayList<>();
	        for (int j = 0; j < sheets.length(); j++) {
	            JSONObject sheet = sheets.getJSONObject(j).getJSONObject("properties");
	            String sheetTitle = sheet.getString("title");
	            sheetNames.add(sheetTitle);
	        }

	        // Store the info
	        Map<String, Object> spreadsheetInfo = new HashMap<>();
	        spreadsheetInfo.put("spreadsheetId", spreadsheetId);
	        spreadsheetInfo.put("spreadsheetTitle", spreadsheetTitle);
	        spreadsheetInfo.put("sheetNames", sheetNames);

	        spreadsheets.add(spreadsheetInfo);
	    }
	    return spreadsheets;
	}


	private String getEmailAccessToken(String accessToken) {
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


	private String getAccessToken() {
		String accessToken=null;
		User user = this.insight.getUser();
		try {
			if(user==null) {
				Map<String,Object> retMap=new HashMap<String, Object>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);	
			}else {
				AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken=msToken.getAccess_token();
			}
		}catch(Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}
		return accessToken;
	}

}
