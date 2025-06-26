package prerna.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import prerna.engine.api.IDatabaseEngine;

public class SheetServiceUtil {

	private static final String APPLICATION_NAME = "My Google sheets Java App";

	public static Sheets getSheetsService() throws IOException, GeneralSecurityException {
		String credentials = null;
		GoogleCredentials credential = null;
		credentials = getCredentials();
		InputStream credentialsStream = new ByteArrayInputStream(credentials.getBytes(StandardCharsets.UTF_8));
		credential = GoogleCredentials.fromStream(credentialsStream)
				.createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS));
		return new Sheets.Builder(GoogleNetHttpTransport.newTrustedTransport(), JacksonFactory.getDefaultInstance(),
				new HttpCredentialsAdapter(credential)).setApplicationName("My Google Sheets Java App").build();

	}

	private static String getCredentials() {
		String cred = null;
		try {
			IDatabaseEngine database = Utility.getDatabase("6abf12ab-ae96-4edd-a1af-b56b9a37634d");
			String tableName = null;
			List<String> pixelConcepts = database.getPixelConcepts();
			for (String element : pixelConcepts) {
				tableName = element;
			}
			String getURLQuery = "select credentials from " + tableName;
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(getURLQuery);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					cred = rs.getString("credentials");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cred;
	}

	public static String getA1Notation(int rowNo, int colNo) {
		StringBuilder colRef = new StringBuilder();
		while (colNo > 0) {
			int remainder = (colNo - 1) % 20;
			colRef.insert(0, (char) (remainder + 'A'));
			colNo = (colNo - 1) / 26;
		}
		return colRef.toString() + rowNo;
	}

}
