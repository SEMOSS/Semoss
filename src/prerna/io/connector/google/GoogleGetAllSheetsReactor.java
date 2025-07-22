package prerna.io.connector.google;

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
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleGetAllSheetsReactor extends AbstractReactor {

    private static final String GOOGLEDRIVE_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'&fields=files(id,name)";
    private static final String GOOGLESHEETS_URL = "https://sheets.googleapis.com/v4/spreadsheets/";
    private static final Logger classLogger = LogManager.getLogger(GoogleGetAllSheetsReactor.class);

    @Override
    public NounMetadata execute() {
        this.organizeKeys();
        ResultSet rs = null;
        List<Map<String, Object>> spreadsheets = new ArrayList<>();
        try {
            String accessToken = getAccessToken();
            if (accessToken == null || accessToken.isEmpty()) {
                Map<String, Object> retMap = new HashMap<>();
                retMap.put("type", "Google");
                retMap.put("message", "Please login to your Google account");
                throwLoginError(retMap);
            }
            // Fetch spreadsheet metadata (titles and sheet names)
            spreadsheets = fetchSpreadsheetMetadata(accessToken);

        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    classLogger.error(Constants.STACKTRACE, ex);
                }
            }
        }
        return new NounMetadata(spreadsheets, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
    }

    @Override
    public String getReactorDescription() {
        return "This reactor will return all spreadsheet titles and sheet names for the authenticated user.";
    }

    /**
     * @param accessToken
     * @return a list of all spreadsheets, their titles, IDs, and sheet names/IDs for the user.
     */
    private List<Map<String, Object>> fetchSpreadsheetMetadata(String accessToken) {
        List<Map<String, Object>> spreadsheets = new ArrayList<>();
        try {
            JSONObject driveResponse = Utility.httpGetJson(GOOGLEDRIVE_URL, accessToken);
            JSONArray files = driveResponse.getJSONArray("files");

            for (int i = 0; i < files.length(); i++) {
                JSONObject file = files.getJSONObject(i);
                String spreadsheetTitle = file.getString("name");
                String spreadsheetId = file.getString("id");

                // Fetch all sheet names and IDs for this spreadsheet
                String sheetsUrl = GOOGLESHEETS_URL + spreadsheetId + "?fields=sheets(properties(sheetId,title))";
                JSONObject sheetsResponse = Utility.httpGetJson(sheetsUrl, accessToken);
                JSONArray sheets = sheetsResponse.getJSONArray("sheets");

                List<Map<String, Object>> sheetNameList = new ArrayList<>();
                for (int j = 0; j < sheets.length(); j++) {
                    JSONObject properties = sheets.getJSONObject(j).getJSONObject("properties");
                    Map<String, Object> sheetInfo = new HashMap<>();
                    sheetInfo.put("id", String.valueOf(properties.getInt("sheetId")));
                    sheetInfo.put("name", properties.getString("title"));
                    sheetNameList.add(sheetInfo);
                }

                Map<String, Object> spreadsheetInfo = new HashMap<>();
                spreadsheetInfo.put("spreadsheetTitle", spreadsheetTitle);
                spreadsheetInfo.put("TitleId", spreadsheetId);      // TitleId key as well
                spreadsheetInfo.put("sheetNames", sheetNameList);

                spreadsheets.add(spreadsheetInfo);
            }
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        }
        return spreadsheets;
    }

    /**
     * To get access token of google logged in user
     */
    private String getAccessToken() {
        String accessToken = null;
        User user = this.insight.getUser();
        try {
            if (user == null) {
                Map<String, Object> retMap = new HashMap<String, Object>();
                retMap.put("type", "google");
                retMap.put("message", "Please login to your Google account");
                throwLoginError(retMap);
            } else {
                AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE);
                accessToken = msToken.getAccess_token();
            }
        } catch (Exception e) {
            Map<String, Object> retMap = new HashMap<>();
            retMap.put("type", "google");
            retMap.put("message", "Please login to your Google account");
            throwLoginError(retMap);
        }
        return accessToken;
    }
}
