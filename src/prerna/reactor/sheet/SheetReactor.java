package prerna.reactor;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets; 
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SheetReactor extends AbstractReactor {
	private static final String APPLICATION_NAME="Google Sheets API Java";
	private static final JsonFactory JSON_FACTORY=JacksonFactory.getDefaultInstance();
	private static final String SERVICE_ACCOUNT_KEY_FILE="C:/Users/dineshsharma/Downloads/inner-bonus-462610-j2-0a325b73f094.json";
	private static final String SPREADSHEET_ID="19AETLkT1QNKuI04dOhCntJ6eHprK2LvCoxt1Hxvbg1Q";
	private static final String RANGE="Sheet1!A1";

	@Override
	public NounMetadata execute() {
		try {
			Boolean success=false;
			String msg=null;
			HashMap<String, Boolean> hashMap=new HashMap<String, Boolean>();
			GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(SERVICE_ACCOUNT_KEY_FILE))
					.createScoped(Arrays.asList(SheetsScopes.SPREADSHEETS));
			Sheets service = new Sheets.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY,
					new HttpCredentialsAdapter(credentials)).setApplicationName(APPLICATION_NAME).build();
			ValueRange appendBody = new ValueRange().setValues(Arrays.asList(Arrays.asList("Name", "Role", "Location"),
					Arrays.asList("Dinesh Sharma", "Java Developer", "India")));
			AppendValuesResponse appendResult = service.spreadsheets().values()
					.append(SPREADSHEET_ID, RANGE, appendBody).setValueInputOption("RAW").execute();
			System.out.println("Data appended: " + appendResult.getUpdates().getUpdatedCells());
			ValueRange response = service.spreadsheets().values().get(SPREADSHEET_ID, "Sheet1!A1:C10").execute();
			List<List<Object>> values = response.getValues();
			if (values == null || values.isEmpty()) {
				System.out.println("No data found");
			} else {
				success=true;
				msg="Interacted with google sheet";
				hashMap.put(msg , success);
				
				
				for (List<Object> row : values) {
					System.out.println(row);
				}
			} 
			return new NounMetadata(hashMap, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			String error="Exception in interacting with google sheets: ";
			return new NounMetadata(error+e.getMessage(),PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

}
