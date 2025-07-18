package prerna.io.connector.docs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.docs.v1.Docs;
import com.google.api.services.docs.v1.model.Document;
import com.google.api.services.drive.Drive;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsCreateReactor extends AbstractReactor {

	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String AppName = "Google Docs";

	public GoogleDocsCreateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROMPT_TITLE.getKey(), ReactorKeysEnum.CONTENT.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String title = this.keyValue.get(this.keysToGet[0]);
		String content = null;

		if (this.keyValue.get(this.keysToGet[1]) != null && !this.keyValue.get(this.keysToGet[1]).isEmpty()) {
			content = this.keyValue.get(this.keysToGet[1]);
		}

		try {
			String accessToken = getGoogleAccessToken();
			Docs service = getDocsServiceUsingToken(accessToken);
			Drive getDriveService = getDriveServiceUsingToken(accessToken);

			boolean success = false;
			String DOCID = "docid";
			String SUCCESS = "success";
			Map<String, Object> res = new HashMap<>();
			try {
				Document doc = GoogleDocsHelper.createDoc(service, getDriveService, title, content);
				if (doc != null && doc.getDocumentId() != null) {
					success = true;
					res.put(DOCID, doc.getDocumentId());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			res.put(SUCCESS, success);
			return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
		    throw new SemossPixelException("Issue with input", e);
		}

	}

	public static Docs getDocsServiceUsingToken(String token) throws Exception {
		HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {

			@Override
			public void initialize(HttpRequest request) throws IOException {
				request.getHeaders().setAuthorization("Bearer " + token);

			}
		};
		return new Docs.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY, requestInitializer)
				.setApplicationName(AppName).build();
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
		return "This reactor is used to create the Google document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROMPT_TITLE.getKey())) {
			return "Title of the Document " + ReactorKeysEnum.PROMPT_TITLE.getKey();
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Content to be added to the document " + ReactorKeysEnum.CONTENT.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
