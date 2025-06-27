package prerna.io.connector.docs;

import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.*;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.docs.v1.Docs;
import com.google.api.services.docs.v1.DocsScopes;
import com.google.api.services.docs.v1.model.Document;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

public class DocsReactor extends AbstractReactor {

	private String SERVICE_ACCOUNT_KEY_FILE = null;
	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String APPLICATION_NAME = "Google Docs Java";
	private static final String EngineId = "26a0d483-a005-4885-8420-46c685c5ee52";

	public DocsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.ID.getKey(),
				ReactorKeysEnum.PROMPT_TITLE.getKey(), ReactorKeysEnum.DOCID.getKey(), ReactorKeysEnum.CONTENT.getKey(),
				ReactorKeysEnum.INDEX.getKey(), ReactorKeysEnum.ENDINDEX.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String command = this.keyValue.get(this.keysToGet[0]);
		String id = null;
		String title = null;
		String docid = null;
		String content = null;
		int Startindex = 1;
		int endindex = 1;

		if (this.keyValue.get(this.keysToGet[1]) != null && this.keyValue.get(this.keysToGet[1]) != "") {
			id = this.keyValue.get(this.keysToGet[1]);
		}
		if (this.keyValue.get(this.keysToGet[2]) != null && this.keyValue.get(this.keysToGet[2]) != "") {
			title = this.keyValue.get(this.keysToGet[2]);
		}
		if (this.keyValue.get(this.keysToGet[3]) != null && this.keyValue.get(this.keysToGet[3]) != "") {
			docid = this.keyValue.get(this.keysToGet[3]);
		}
		if (this.keyValue.get(this.keysToGet[4]) != null && this.keyValue.get(this.keysToGet[4]) != "") {
			content = this.keyValue.get(this.keysToGet[4]);
		}
		if (this.keyValue.get(this.keysToGet[5]) != null && this.keyValue.get(this.keysToGet[5]) != "") {
			Startindex = Integer.parseInt(this.keyValue.get(this.keysToGet[5]));
		}
		if (this.keyValue.get(this.keysToGet[6]) != null && this.keyValue.get(this.keysToGet[6]) != "") {
			endindex = Integer.parseInt(this.keyValue.get(this.keysToGet[6]));
		}

		try {
			int profileId = Integer.parseInt(id);
			SERVICE_ACCOUNT_KEY_FILE = getServiceDetails(profileId);
			InputStream serviceaccount = new ByteArrayInputStream(
					SERVICE_ACCOUNT_KEY_FILE.getBytes(StandardCharsets.UTF_8));
			GoogleCredentials credentials = GoogleCredentials.fromStream(serviceaccount)
					.createScoped(Arrays.asList(DocsScopes.DOCUMENTS));
			Docs service = new Docs.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY,
					new HttpCredentialsAdapter(credentials)).setApplicationName(APPLICATION_NAME).build();

			switch (command.trim().replaceAll("\\s+", " ").toLowerCase()) {
			case "createdoc":
				boolean success = false;
				String DOCID = "docid";
				String SUCCESS = "success";
				Map<String, Object> res = new HashMap<>();
				try {
					Document doc = DocsHelper.createDoc(service, title);
					if (doc != null && doc.getDocumentId() != null) {
						success = true;
						String tableName = null;
						try {
							IDatabaseEngine database = Utility.getDatabase(EngineId);
							List<String> tableNames = database.getPixelConcepts();
							for (String table : tableNames) {
								tableName = table;
							}
							String updatequery = "update " + tableName + " set docid = '" + doc.getDocumentId() + "'"
									+ " where id = " + id;
							database.execQuery(updatequery);
						} catch (Exception e) {
							e.printStackTrace();
						}
						res.put(DOCID, doc.getDocumentId());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				res.put(SUCCESS, success);
				return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

			case "readdoc":
				String contentValue = DocsHelper.readDoc(service, docid);
				return new NounMetadata(contentValue, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);

			case "updatedoc":
				boolean updateresult = DocsHelper.updateDoc(service, docid, content, Startindex);
				String updatetableName = null;
				try {
					IDatabaseEngine database = Utility.getDatabase(EngineId);
					List<String> tableNames = database.getPixelConcepts();
					for (String table : tableNames) {
						updatetableName = table;
					}
					String updatequery = "update " + updatetableName + " set lastupdateddate = '"
							+ System.currentTimeMillis() + "'" + " where id = " + id;
					database.execQuery(updatequery);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return new NounMetadata(updateresult, PixelDataType.CUSTOM_DATA_STRUCTURE,
						PixelOperationType.OPERATION);

			case "deletedoc":
				boolean deleteresult = DocsHelper.deleteDoc(service, docid, Startindex, endindex);
				String deletetableName = null;
				try {
					IDatabaseEngine database = Utility.getDatabase(EngineId);
					List<String> tableNames = database.getPixelConcepts();
					for (String table : tableNames) {
						deletetableName = table;
					}
					String updatequery = "update " + deletetableName + " set lastupdateddate = '"
							+ System.currentTimeMillis() + "'" + " where id = " + id;
					database.execQuery(updatequery);
				} catch (Exception e) {
					e.printStackTrace();
				}
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

	public static String getServiceDetails(int id) {
		try {
			String ResultSetObj = "RESULTSET_OBJECT";
			IDatabaseEngine database = Utility.getDatabase(EngineId);
			String query = "select servicejson from googledocsprofile where id = " + id;
			@SuppressWarnings("unchecked")
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object rsObj = hashmap.get(ResultSetObj);

			if (rsObj instanceof ResultSet) {
				ResultSet rs = (ResultSet) rsObj;
				if (rs.next()) {
					return rs.getString("servicejson");
				}
			}
			throw new Exception("Service Account not found");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to create, update, delete and read the document.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "Command to perform CRUD operations " + ReactorKeysEnum.COMMAND.getKey();
		} else if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Id " + ReactorKeysEnum.ID.getKey();
		} else if (key.equals(ReactorKeysEnum.PROMPT_TITLE.getKey())) {
			return "Title of the Document " + ReactorKeysEnum.PROMPT_TITLE.getKey();
		} else if (key.equals(ReactorKeysEnum.DOCID.getKey())) {
			return "Id of the Document created " + ReactorKeysEnum.DOCID.getKey();
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
