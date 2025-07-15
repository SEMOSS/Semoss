package prerna.io.connector.docs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsListReactor extends AbstractReactor{
	
	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String AppName = "Google Docs";
	
	@Override
	public NounMetadata execute() {
		try {
			String accessToken = getGoogleAccessToken();
			Drive getDriveService = getDriveServiceUsingToken(accessToken);

			List<String> docTitleList = getDocsTitleList(getDriveService);
			HashMap<String, Object> res = new HashMap<>();
			res.put("DocTitleList", docTitleList);
			return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Issue with input");
		}

	}
	
	public static List<String> getDocsTitleList(Drive driveService) {
		List<String> docList = new ArrayList<>();
		try {
			String query = "mimeType = 'application/vnd.google-apps.document'";
			
			FileList result = driveService.files().list().setQ(query).setFields("files(id, name)").execute();
			
			List<File> files = result.getFiles();
			
			if(files != null && !files.isEmpty()) {
				for(File file : files) {
					docList.add(file.getName());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return docList;
	}

	public static Drive getDriveServiceUsingToken(String token) throws Exception {
		HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {

			@Override
			public void initialize(HttpRequest request) throws IOException {
				request.getHeaders().setAuthorization("Bearer " + token);

			}
		};
		return new Drive.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY, requestInitializer)
				.setApplicationName(AppName).build();
	}

	public String getGoogleAccessToken() throws Exception {

		String accessToken = null;
		User user = this.insight.getUser();

		if (user == null) {
			throw new Exception("User not found in session.");
		}

		AccessToken googleToken = user.getAccessToken(AuthProvider.GOOGLE);

		if (googleToken == null) {
			throw new Exception("No Google Access Token fetched.");
		}
		accessToken = googleToken.getAccess_token();
		return accessToken;
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to get the list of Google document.";
	}

}
