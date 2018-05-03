package prerna.engine.impl.googlesheets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ValueRange;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.util.CsvFileIterator;
import prerna.engine.impl.AbstractEngine;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GoogleSheetsEngine extends AbstractEngine {

	/**
	 * The way this class works
	 * Is you sign into the google account
	 * And once you have
	 * We bring the csv and then load it into an R frame
	 * And use for the exec. queries, etc.
	 */
	
	private final HttpTransport TRANSPORT = new NetHttpTransport();
	private final JacksonFactory JSON_FACTORY = new JacksonFactory();
	
	private String rvarName = null;
	private RDataTable dt = new RDataTable();
	
	@Override
	public void openDB(String propFile) {
		// load in the metadata and the insights
		super.openDB(propFile);
		
		// write the google sheet locally
		String mimeType = this.prop.getProperty("MINE_TYPE");
		String fileId = this.prop.getProperty("FILE_ID");
		String sheetName = this.prop.getProperty("SHEET_NAME");
		
		String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePath += "\\" + Utility.getRandomString(10) + ".csv";
		filePath = filePath.replace("\\", "/");

		// TODO: how do i get this!!!
		GoogleCredential gc = null;

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
		
		
		// we just need to create a flat table
		// from the given information
		// then we need to create a prim key placeholder
		
		// this method returns the information
		// assuming that the engine is actually loaded
		SelectQueryStruct dbQs = getDatabaseQueryStruct();
		// but we will shift the info to be a CSV query struct
		CsvQueryStruct csvQs = new CsvQueryStruct();
		// this is a flat db, so only need selectors
		csvQs.setSelectors(dbQs.getSelectors());
		
		csvQs.setCsvFilePath(filePath);
		csvQs.setDelimiter(',');
		// need to get datatypes from OWL
		Map<String, String> dataTypes = new HashMap<String, String>();
		Map<String, SemossDataType> semossDataTypes = new HashMap<String, SemossDataType>();
		csvQs.setColumnTypes(dataTypes);
		
		CsvFileIterator fit = new CsvFileIterator(csvQs);
		rvarName = Utility.getRandomString(6);
		dt = new RDataTable(rvarName);
		dt.addRowsViaIterator(fit, rvarName, semossDataTypes);
		// this is the prim key we need to add
		dt.generateRowIdWithName();
	}
	
	////////////////////////////////////////////////////////////////
	
	@Override
	public Object execQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

}
