package prerna.sablecc2.reactor.qs.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;

import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ListGoogleSheetsReactor extends AbstractReactor {

	private final HttpTransport TRANSPORT = new NetHttpTransport();
	private final JacksonFactory JSON_FACTORY = new JacksonFactory();

	public ListGoogleSheetsReactor() {
		this.keysToGet = new String[] { "sheetId" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		List<String> sheetList = new ArrayList<String>();
		GoogleCredential gc = null;
		String sheetId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		if (user != null) {
			gc = (GoogleCredential) user.getAdditionalData("googleCredential");
		}

		if(gc == null) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}

		Sheets sheetsService = new Sheets.Builder(TRANSPORT, JSON_FACTORY, gc).setApplicationName("SEMOSS").build();
		Spreadsheet spreadsheet = null;
		try {
			spreadsheet = sheetsService.spreadsheets().get(sheetId).setIncludeGridData(false).execute();
			if (spreadsheet != null) {
				List<Sheet> sheets = spreadsheet.getSheets();
				for (int j = 0; j < sheets.size(); j++) {
					String sheetName = sheets.get(j).getProperties().getTitle();
					sheetList.add(sheetName);
				}
				return new NounMetadata(sheetList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.GOOGLE_SHEET_LIST);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to get sheet names");
		}

		throw new IllegalArgumentException("No files found. Please consider logging into a different account.");
	}

}
