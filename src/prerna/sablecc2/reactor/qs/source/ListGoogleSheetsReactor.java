package prerna.sablecc2.reactor.qs.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;

import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ListGoogleSheetsReactor extends AbstractReactor {
	private final HttpTransport TRANSPORT = new NetHttpTransport();
	private final JacksonFactory JSON_FACTORY = new JacksonFactory();

	public ListGoogleSheetsReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		List<HashMap<String, Object>> masterList = new ArrayList<HashMap<String, Object>>();
		GoogleCredential gc = null;
		User user = this.insight.getUser();
		if (user != null) {
			gc = (GoogleCredential) user.getAdditionalData("googleCredential");
		}
		if (gc != null) {
			// google drive api
			Drive driveService = new Drive.Builder(TRANSPORT, JSON_FACTORY, gc).setApplicationName("SEMOSS").build();

			try {
				FileList files = driveService.files().list()
						.setQ("mimeType contains '.spreadsheet' or mimeType contains 'text/csv' ").execute();
				List<File> fileList = files.getFiles();
				// TODO may not be null here - need to test
				// when theres not excels in the drive
				if (fileList != null) {
					Sheets sheetsService = new Sheets.Builder(TRANSPORT, JSON_FACTORY, gc).setApplicationName("SEMOSS")
							.build();
					for (int i = 0; i < fileList.size(); i++) {
						// {"id":"id###","kind":"drive#file","mimeType":"application/vnd.google-apps.spreadsheet","name":"name##"}
						File file = fileList.get(i);
						HashMap<String, Object> tempMap = new HashMap<String, Object>();
						String fileType = "";
						if (file.get("mimeType").equals("text/csv")) {
							fileType = "CSV";
						} else {
							fileType = ".spreadsheet";
						}
						tempMap.put("type", fileType);
						tempMap.put("id", file.get("id"));
						tempMap.put("name", file.get("name"));
						if (fileType != "CSV") {
							Spreadsheet spreadsheet = sheetsService.spreadsheets().get(file.get("id").toString())
									.execute();
							List<Sheet> sheets = spreadsheet.getSheets();
							List<String> tempSheetsNameList = new ArrayList<String>();
							for (int j = 0; j < sheets.size(); j++) {
								String sheetName = sheets.get(j).getProperties().getTitle();
								tempSheetsNameList.add(sheetName);
								tempMap.put("sheets", tempSheetsNameList);
							}
						}
						masterList.add(tempMap);
					}
				}
				return new NounMetadata(masterList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
			} catch (IOException e) {
				throw new IllegalArgumentException("Unable to retrieve files from Google Drive");
			}
		}

		throw new IllegalArgumentException("Please login");
	}

}
