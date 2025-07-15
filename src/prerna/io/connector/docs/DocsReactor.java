package prerna.io.connector.docs;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

import java.io.*;
import java.util.*;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.docs.v1.Docs;
import com.google.api.services.docs.v1.model.Document;
import com.google.api.services.drive.Drive;

public class DocsReactor extends AbstractReactor {

	static RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String APPLICATION_NAME = "Google Docs Java";

	public DocsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.NAME.getKey(),
				ReactorKeysEnum.PROMPT_TITLE.getKey(), ReactorKeysEnum.CONTENT.getKey(),
				ReactorKeysEnum.INDEX.getKey(), ReactorKeysEnum.ENDINDEX.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String command = this.keyValue.get(this.keysToGet[0]);
		String name = null;
		String title = null;
		String content = null;
		Integer Startindex = null;
		Integer endindex = null;

		if (this.keyValue.get(this.keysToGet[1]) != null && this.keyValue.get(this.keysToGet[1]) != "") {
			name = this.keyValue.get(this.keysToGet[1]);
		}
		if (this.keyValue.get(this.keysToGet[2]) != null && this.keyValue.get(this.keysToGet[2]) != "") {
			title = this.keyValue.get(this.keysToGet[2]);
		}
		if (this.keyValue.get(this.keysToGet[3]) != null && this.keyValue.get(this.keysToGet[3]) != "") {
			content = this.keyValue.get(this.keysToGet[3]);
		}
		if (this.keyValue.get(this.keysToGet[4]) != null && this.keyValue.get(this.keysToGet[4]) != "") {
			Startindex = Integer.parseInt(this.keyValue.get(this.keysToGet[4]));
		}
		if (this.keyValue.get(this.keysToGet[5]) != null && this.keyValue.get(this.keysToGet[5]) != "") {
			endindex = Integer.parseInt(this.keyValue.get(this.keysToGet[5]));
		}

		try {
			String accessToken = getGoogleAccessToken();
			Docs service = getDocsServiceUsingToken(accessToken);
			Drive getDriveService = getDriveServiceUsingToken(accessToken);

			switch (command.trim().replaceAll("\\s+", " ").toLowerCase()) {
			case "createdoc":
				boolean success = false;
				String DOCID = "docid";
				String SUCCESS = "success";
				Map<String, Object> res = new HashMap<>();
				try {
					Document doc = DocsHelper.createDoc(service, title, content, name);
					if (doc != null && doc.getDocumentId() != null) {
						success = true;
						res.put(DOCID, doc.getDocumentId());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				res.put(SUCCESS, success);
				return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

			case "readdoc":
				String contentValue = DocsHelper.readDoc(service, title, name);
				return new NounMetadata(contentValue, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);

			case "updatedoc":
				boolean updateresult = DocsHelper.updateDoc(service, title, name, content, Startindex);
				return new NounMetadata(updateresult, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);

			case "deletedoc":
				boolean deleteresult = DocsHelper.deleteDoc(service, getDriveService, title, name, Startindex, endindex);
				return new NounMetadata(deleteresult, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);

			default:
				return new NounMetadata("Please provide valid command", PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);
			}

		} catch (Exception e) {
			throw new SemossPixelException("Issue with input");
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
				.setApplicationName(APPLICATION_NAME).build();
	}

	public static Drive getDriveServiceUsingToken(String token) throws Exception {
		HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {

			@Override
			public void initialize(HttpRequest request) throws IOException {
				request.getHeaders().setAuthorization("Bearer " + token);

			}
		};
		return new Drive.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY, requestInitializer)
				.setApplicationName(APPLICATION_NAME).build();
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
		return "This reactor is used to create, update, delete and read the document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "Command to perform CRUD operations " + ReactorKeysEnum.COMMAND.getKey();
		} else if (key.equals(ReactorKeysEnum.PROMPT_TITLE.getKey())) {
			return "Title of the Document " + ReactorKeysEnum.PROMPT_TITLE.getKey();
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Content to be added to the document " + ReactorKeysEnum.CONTENT.getKey();
		} else if (key.equals(ReactorKeysEnum.INDEX.getKey())) {
			return "Start index for the insertion or deletion of the content " + ReactorKeysEnum.INDEX.getKey();
		} else if (key.equals(ReactorKeysEnum.ENDINDEX.getKey())) {
			return "Index till which content of document to be deleted " + ReactorKeysEnum.ENDINDEX.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
