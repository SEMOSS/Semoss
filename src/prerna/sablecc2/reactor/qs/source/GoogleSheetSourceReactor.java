package prerna.sablecc2.reactor.qs.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ValueRange;

import prerna.auth.User;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GoogleSheetSourceReactor extends AbstractQueryStructReactor {

	private final HttpTransport TRANSPORT = new NetHttpTransport();
	private final JacksonFactory JSON_FACTORY = new JacksonFactory();

	public GoogleSheetSourceReactor() {
		this.keysToGet = new String[]{"id", "sheetNames", "type"};
	}

	@Override
	protected QueryStruct2 createQueryStruct() {
		organizeKeys();
		String fileId = this.keyValue.get(this.keysToGet[0]);
		if (fileId == null || fileId.length() <= 0) {
			throw new IllegalArgumentException("Need to specify id");
		}
		String mimeType = this.keyValue.get(this.keysToGet[2]);
		if (mimeType == null || !(mimeType.equals(".spreadsheet") || mimeType.equals("CSV"))) {
			throw new IllegalArgumentException("Need to specify the supported Google Sheet file type (.spreadsheet, CSV)");
		}
		List<String> sheetNames = getSheetNames();
		String sheetName = null;
		if (mimeType.equals(".spreadsheet")) {
			// assume 1 sheet name for now
			if (sheetNames.isEmpty()) {
				throw new IllegalArgumentException("Need to specify sheetNames");
			}
			sheetName = sheetNames.get(0);
		}

		// get google credentials
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
			exception.setAdditionalReturn(new NounMetadata(retMap, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR));
			throw exception;
		}

		String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePath += "\\" + Utility.getRandomString(10) + ".csv";
		filePath = filePath.replace("\\", "/");

		// download csv from google drive
		if (mimeType.equals("CSV")) {
			Drive driveService = new Drive.Builder(TRANSPORT, JSON_FACTORY, gc).setApplicationName("SEMOSS").build();
			try {
				OutputStream os = new FileOutputStream(new File(filePath));
				driveService.files().get(fileId).executeMediaAndDownloadTo(os);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// download google sheet
		else {
			Sheets service = new Sheets.Builder(TRANSPORT, JSON_FACTORY, gc).setApplicationName("SEMOSS").build();
			try {
				ValueRange response = service.spreadsheets().values().get(fileId, sheetName).execute();
				List<List<Object>> values = response.getValues();
				// check if sheet has data
				if (values == null || values.size() == 0) {
					throw new IllegalArgumentException("No data found.");
				} else {
					// flush out sheet to file csv path
					PrintWriter out = new PrintWriter(filePath);
					for (List<?> row : values) {
						String csvRow = "";
						for (int i = 0; i < row.size(); i++) {
							csvRow += row.get(i).toString();
							if (i < row.size() - 1) {
								csvRow += ",";
							}
						}
						out.write(csvRow + System.getProperty("line.separator"));
					}
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// get datatypes
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(filePath);
		String[] colNames = helper.getHeaders();
		String[] types = helper.predictTypes();
		// clear the helper
		helper.clear();

		CsvQueryStruct qs = new CsvQueryStruct();
		qs.setSource(CsvQueryStruct.ORIG_SOURCE.API_CALL);
		Map<String, String> dataTypes = new HashMap<String, String>();
		int numHeaders = colNames.length;
		for (int i = 0; i < numHeaders; i++) {
			qs.addSelector("DND", colNames[i]);
			dataTypes.put(colNames[i], types[i]);
		}
		qs.merge(this.qs);
		qs.setCsvFilePath(filePath);
		qs.setDelimiter(',');
		qs.setColumnTypes(dataTypes);
		return qs;
	}

	private List<String> getSheetNames() {
		List<String> retList = new ArrayList<String>();
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				String attribute = noun.getValue().toString();
				retList.add(attribute);
			}
		}
		return retList;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("id")) {
			return "The Google Sheet id";
		} else if (key.equals("sheetNames")) {
			return "The sheet names within the Google Sheet";
		} else if (key.equals("type")) {
			return "";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
