package prerna.util.anthem;

import java.util.HashMap;
import java.util.Map;

import prerna.poi.main.helper.ImportOptions;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.DIHelper;

public class KickoutWebWatcherFull extends AbstractKickoutWebWatcher {

	private String errorCodeDbName;
	private String errorCodeCsv;
	private String errorCodePropFile;
	
	private static final String ERROR_ID_KEY = "ERROR_ID";
	
	public KickoutWebWatcherFull() {
		errorCodeDbName = DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_ERROR_CODE_DB_NAME");
		errorCodeCsv = DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_ERROR_CODE_CSV");
		errorCodePropFile = DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_ERROR_CODE_PROP_FILE");
	}
	
	@Override
	protected String giveDbName() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_DB_NAME");
	}

	@Override
	protected String givePropFile() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_PROP_FILE");
	}
	
	@Override
	protected String giveProcessedDbName() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_PROCESSED_DB_NAME");
	}

	@Override
	protected String giveXlsToCsvScript() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_XLS_CSV_SCRIPT");
	}

	@Override
	protected String giveXlsTempDir() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_XLS_TEMP_DIR");
	}

	@Override
	protected String giveCsvTempDir() {
		return DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_CSV_TEMP_DIR");
	}

	@Override
	protected void initialize() {
		
		// First create a new database for error code description and severities
		// If it doesn't already exist
		// Returns true if the engine for the error code database exists
		if (!loadEngineIfExists(errorCodeDbName)) {
			
			// This is data that is not contained in the reports,
			// and is just a one-time add
			createErrorCodeDB();
		}	
	}
	
	@Override
	protected Map<String, String> populateValueMap(String zipFileName, String excelFileName) {
		
		// Grab the system name from the excel file
		String systemName = excelFileName.substring(11, 13);
		
		// Grab the date and time from the zip file
		String date = zipFileName.substring(12, 22); // yyyy-MM-dd
		String time = zipFileName.substring(23, 31).replace('.', ':'); // hh:mm:ss
		String timestamp = date + " " + time; // yyyy-MM-dd hh:mm:ss
		String[] parsedDate = date.split("-"); // Y, M, D
		String[] parsedTime = time.split(":"); // h, m, s

		// Map to store data not found in the csv but specified in the prop file
		Map<String, String> valueMap = new HashMap<String, String>();
		
		// Needed for concatenated unique id
		valueMap.put(ZIP_KEY, zipFileName);
		valueMap.put(XLS_SYS_KEY, systemName);
		valueMap.put(ERROR_ID_KEY, ZIP_KEY + "+" + XLS_SYS_KEY + "+" + ROW_KEY);
		
		// Needed for kickout time
		valueMap.put(TIMESTAMP_KEY, timestamp);
		
		// Needed for sql date and time objects
		valueMap.put(DATE_KEY, date);
		valueMap.put(TIME_KEY, time);
		
		// Needed for parsed date and time
		valueMap.put(YEAR_KEY, parsedDate[0]);
		valueMap.put(MONTH_KEY, parsedDate[1]);
		valueMap.put(DAY_KEY, parsedDate[2]);
		valueMap.put(HOUR_KEY, parsedTime[0]);
		valueMap.put(MINUTE_KEY, parsedTime[1]);
		valueMap.put(SECOND_KEY, parsedTime[2]);
		return valueMap;
	}

	@Override
	protected Map<String, String> populateTypeMap(String zipFileName, String excelFileName) {
		
		// Map to store the data types (defaults to string otherwise)
		// Uses SEMOSS UI types (see AbstractEngineCreator createSqlTypes)
		Map<String, String> typeMap = new HashMap<String, String>();
		
		// Needed for kickout time
		typeMap.put(TIMESTAMP_KEY, TIMESTAMP_TYPE);
		
		// Needed for sql date and time objects
		typeMap.put(DATE_KEY, DATE_TYPE);
		typeMap.put(TIME_KEY, TIME_TYPE);
		
		// Needed for parsed date and time
		typeMap.put(YEAR_KEY, YEAR_TYPE);
		typeMap.put(MONTH_KEY, MONTH_TYPE);
		typeMap.put(DAY_KEY, DAY_TYPE);
		typeMap.put(HOUR_KEY, HOUR_TYPE);
		typeMap.put(MINUTE_KEY, MINUTE_TYPE);
		typeMap.put(SECOND_KEY, SECOND_TYPE);
		
		return typeMap;
	}
	
	@Override
	protected void ingestMetadataIntoSeparateDB(Map<String, String> valueMap, Map<String, String> typeMap, boolean createIndexes) {
		// Nothing to do here, don't want to ingest metadata into a separate db
	}
		
	// Uses ImportDataProcessor for this database, as it needs to be exposed to the user
	private void createErrorCodeDB() {
		ImportDataProcessor importer = new ImportDataProcessor();
		ImportOptions options = new ImportOptions();
		options.setDbName(errorCodeDbName);
		options.setImportType(ImportOptions.IMPORT_TYPE.CSV);
		options.setRDBMSDriverType(dbType);
		options.setDbType(ImportOptions.DB_TYPE.RDBMS);		
		options.setBaseFolder(baseFolder);
		options.setAutoLoad(false);
		options.setAllowDuplicates(true);
		options.setFileLocation(errorCodeCsv);
		options.setPropertyFiles(errorCodePropFile);
		options.setImportMethod(ImportOptions.IMPORT_METHOD.CREATE_NEW);
		try {
			importer.runProcessor(options);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Failed to create " + errorCodeDbName);
		}
	}
	
	// Test case
	public static void main(String[] args) {
		DIHelper.getInstance().loadCoreProp(System.getProperty("user.dir") + "/RDF_Map.prop");		
		KickoutWebWatcherFull kww = new KickoutWebWatcherFull();
		kww.setFolderToWatch(DIHelper.getInstance().getProperty("AnthemWebWatcher_DIR"));
		kww.setExtension(DIHelper.getInstance().getProperty("AnthemWebWatcher_EXT"));
		kww.setMonitor(new Object());
		new Thread(kww).start();
	}

}
