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
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ListGoogleDriveFilesReactor extends AbstractReactor {

	private final HttpTransport TRANSPORT = new NetHttpTransport();
	private final JacksonFactory JSON_FACTORY = new JacksonFactory();

	public ListGoogleDriveFilesReactor() {
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

		if(gc == null) {
			SemossPixelException exception = new SemossPixelException();
			exception.setContinueThreadOfExecution(false);
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			exception.setAdditionalReturn(new NounMetadata(retMap, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR, PixelOperationType.ERROR));
			throw exception;
		}

		// google drive api
		Drive driveService = new Drive.Builder(TRANSPORT, JSON_FACTORY, gc).setApplicationName("SEMOSS").build();

		FileList files = null;
		try {
			files = driveService.files().list().setQ("mimeType contains '.spreadsheet' or mimeType contains 'text/csv' ").execute();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (files != null) {
			List<File> fileList = files.getFiles();
			// TODO may not be null here - need to test
			// when theres not excels in the drive
			if (fileList != null) {
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
					masterList.add(tempMap);
				}
			}
			return new NounMetadata(masterList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.GOOGLE_DRIVE_LIST);
		}

		throw new IllegalArgumentException("No files found. Please consider logging into a different account.");
	}

}