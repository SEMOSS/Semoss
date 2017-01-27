package prerna.util.anthem;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import prerna.engine.api.IEngine;
import prerna.poi.main.helper.ImportOptions;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.AbstractFileWatcher;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.H2QueryUtil;
import prerna.util.sql.SQLQueryUtil;

public abstract class AbstractKickoutWebWatcher extends AbstractFileWatcher {

	private String dbName;
	private String propFile;
	private String processedDbName;
	private String xlsToCsvScript;
	private String xlsTempDir;
	private String csvTempDir;
	private Set<String> ignoreSystems;
	private Set<String> acceptZips;
	private Set<String> acceptReports;
	private long sizeCutoff; // In bytes
	private long processDelay; // In seconds
	private boolean holdOnIndexing;
	private boolean isMariaDB;
	private String processedDbUrl;
	
	// Allow subclasses to access the base folder and database type
	protected String baseFolder;
	
	// Type of database is used for everything but the processed database (H2)
	protected SQLQueryUtil.DB_TYPE dbType;

	private H2QueryUtil h2Util = new H2QueryUtil();
	private boolean dbExists = false;
	
	// Keep track of when to index during uploads, default to on all uploads
	private enum IndexEnum {INDEX_ON_ALL, INDEX_ON_NONE, INDEX_ON_LAST};
	private IndexEnum whatToIndex = IndexEnum.INDEX_ON_ALL;
	
	// Fields used to generate the processed database and its structure
	private static final String PROCESSED_DB_USER = "sa";
	private static final String PROCESSED_DB_PASS = "";
	private static final String ZIP_TABLE = "ZIP_FILE";
	private static final String ZIP_COLUMN = "ZIP_FILE_NAME";
	private static final String ZIP_TIME_COLUMN = "TIMESTAMP";
	private static final String ZIP_SIZE_COLUMN = "FILE_SIZE";
	private static final String REPORT_TABLE = "ERROR_REPORT";
	private static final String REPORT_COLUMN = "ERROR_REPORT_NAME";
	private static final String REPORT_TIME_COLUMN = "TIMESTAMP";
	private static final String REPORT_SIZE_COLUMN = "FILE_SIZE";
	private static final String REPORT_SYS_COLUMN = "SYSTEM_NAME";
	
	// Protected variables to define common column names,
	// so that all subclasses are on the same page (good for joins, traverse)
	// Let these first ones default to string type
	protected static final String ROW_KEY = "ROW"; // For ImportDataProcessor.setRowKey
	protected static final String ZIP_KEY = "ZIP";
	protected static final String XLS_SYS_KEY = "XLS_SYS"; // Source system from xls file name
	
	// Define proper types for date and time fields
	protected static final String TIMESTAMP_KEY = "KO_TIMESTAMP"; // yyyy-MM-dd hh:mm:ss
	protected static final String TIMESTAMP_TYPE = "STRING";
	protected static final String DATE_KEY = "KO_DATE"; // yyyy-MM-dd
	protected static final String DATE_TYPE = "DATE";
	protected static final String TIME_KEY = "KO_TIME"; // hh:mm:ss
	protected static final String TIME_TYPE = "STRING"; // TODO implement time type
	protected static final String YEAR_KEY = "KO_YEAR";
	protected static final String YEAR_TYPE = "NUMBER";
	protected static final String MONTH_KEY = "KO_MONTH";
	protected static final String MONTH_TYPE = "NUMBER";
	protected static final String DAY_KEY = "KO_DAY";
	protected static final String DAY_TYPE = "NUMBER";
	protected static final String HOUR_KEY = "KO_HOUR";
	protected static final String HOUR_TYPE = "NUMBER";
	protected static final String MINUTE_KEY = "KO_MINUTE";
	protected static final String MINUTE_TYPE = "NUMBER";
	protected static final String SECOND_KEY = "KO_SECOND";
	protected static final String SECOND_TYPE = "NUMBER";
		
	protected static final Logger logger = LogManager.getLogger(AbstractKickoutWebWatcher.class.getName());
	
	public AbstractKickoutWebWatcher() {
		
		// Enforce subclasses to give custom values for these
		dbName = giveDbName();
		propFile = givePropFile();
		processedDbName = giveProcessedDbName();
		xlsToCsvScript = giveXlsToCsvScript();
		xlsTempDir = giveXlsTempDir();
		csvTempDir = giveCsvTempDir();
		
		// Get properties that apply to all subclasses from RDF_Map.prop
		ignoreSystems = new HashSet<String>(Arrays.asList(DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_IGNORE_SYSTEMS").split(";")));
		acceptZips = new HashSet<String>(Arrays.asList(DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_ACCEPT_ZIP_WITH").split(";")));
		acceptReports = new HashSet<String>(Arrays.asList(DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_ACCEPT_REPORT_WITH").split(";")));
		sizeCutoff = Long.parseLong(DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_SIZE_CUTOFF"));
		processDelay = Long.parseLong(DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_PROCESS_DELAY"));
		holdOnIndexing = Boolean.parseBoolean(DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_HOLD_ON_INDEXING"));
		isMariaDB = Boolean.parseBoolean(DIHelper.getInstance().getProperty("ANTHEM_KICKOUT_MARIA_DB"));
		baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		processedDbUrl = h2Util.getConnectionURL(baseFolder, processedDbName);
		if (isMariaDB) {
			dbType = SQLQueryUtil.DB_TYPE.MARIA_DB;
		} else {
			dbType = SQLQueryUtil.DB_TYPE.H2_DB;
		}
	}
	
	protected abstract String giveDbName();
	protected abstract String givePropFile();
	protected abstract String giveProcessedDbName();
	protected abstract String giveXlsToCsvScript();
	protected abstract String giveXlsTempDir();
	protected abstract String giveCsvTempDir();
	
	@Override
	public void loadFirst() {
		
		// Get the driver for the connection to the processedDb
		try {
			Class.forName(H2QueryUtil.DATABASE_DRIVER);
		} catch (ClassNotFoundException e) {
			logger.error("H2 Connection Error: " + e.getMessage() + " for Connection URL: " + processedDbUrl);
			e.printStackTrace();
		}
		
		// Load the engine for the kickout database if it already exists
		// Returns true if it exists
		if (loadEngineIfExists(dbName)) {
			dbExists = true;
		} else {
			
			// If the engine does not exist,
			// it will need a new processed database to keep track of what files have been added
			createProcessedDB();
		}
		
		// Call the abstract method initialize, 
		// subclasses must define what else needs to be initialized before processing zip files
		initialize();
				
		// Process each zip file in the error kickout folder
		File kickout = new File(folderToWatch);
		String[] candidateZipFileNames = kickout.list();
		
		// Decide which zip files to process
		List<String> zipFileNames = new ArrayList<String>();
		for (String candidate : candidateZipFileNames) {
			if (needToProcess(candidate)) {
				zipFileNames.add(candidate);
			}
		}
		
		// Process zip files if there are any to process
		int numberToProcess = zipFileNames.size();
		if (numberToProcess > 0) {
			
			// If the user wants to wait on indexing until load first is completed,
			// Then don't create any indexes until the last zip file is being processed
			// Note that the default is INDEX_ON_ALL when there is no hold on indexing
			if (holdOnIndexing) {
				whatToIndex = IndexEnum.INDEX_ON_NONE;
			}
			for (int i = 0; i < numberToProcess - 1; i++) {
				process(zipFileNames.get(i));
			}
			if (holdOnIndexing) {
				whatToIndex = IndexEnum.INDEX_ON_LAST;
			}
			process(zipFileNames.get(numberToProcess - 1));
		}
	}

	// What needs to be initialized in loadFirst before processing any zip files
	protected abstract void initialize();
	
	// Check whether the smss file for the given database exists
	// If it does, hold the thread until the engine has been loaded
	protected boolean loadEngineIfExists(String dbName) {
		
		// Flag whether the smss file for the database exists
		File smssFile = new File(baseFolder + "\\db\\" + dbName + ".smss");
		if (smssFile.exists()) {
						
			// If the database exists, then wait for SEMOSS to load the engine
			// Otherwise will throw an error when trying to add to existing database
			boolean loadingEngine = true;
			while (loadingEngine) {
				logger.info(">>>>>Attempting to load the engine " + dbName);
				IEngine engine = Utility.getEngine(dbName);
				if(engine == null) {
					logger.info(">>>>>The engine " + dbName + " failed to load, waiting 30s before attempting again");
					try {
						Thread.sleep(30000); // Wait 30s
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					loadingEngine = false;
					logger.info(">>>>>The engine " + dbName + " has loaded");
				}
			}
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public void process(String zipFileName) {
		String zipFilePath = folderToWatch + "\\" + zipFileName;
		
		// Track how long it takes to process the zip file for testing purposes
		long start = System.currentTimeMillis();
		logger.info("Processing " + zipFileName);
					
		// Retrieve the file names from the zip file
		// Stop here if no file names are retrieved
		List<String> candidateExcelFileNames;
		try {
			candidateExcelFileNames = retrieveFileNames(zipFilePath);
		} catch (ZipException e) {
			e.printStackTrace();
			logger.error("Failed to retrieve file names from " + zipFileName);
			return;
		}
		
		// Decide which excel files to ingest
		List<String> excelFileNames = new ArrayList<String>();
		for (String candidate : candidateExcelFileNames) {
			
			// Check to make sure that the candidate has an extension,
			// Sometimes a folder shows up in the kickout reports
			if (candidate.contains(".")) {
				int extensionIndex = candidate.lastIndexOf(".");
				
				// Only consider files that are accepted and are not on the ignore list
				String systemName = candidate.substring(extensionIndex - 9, extensionIndex - 7);
				if (acceptReports.contains(candidate.substring(extensionIndex - 3, extensionIndex)) && !ignoreSystems.contains(systemName)) {
					long fileSize;
					
					// Try to retrieve the file size
					// If retrieving the file size fails, continue to the next candidate
					try {
						fileSize = retrieveFileSize(zipFilePath, candidate);
					} catch (ZipException e) {
						e.printStackTrace();
						logger.error("Failed to retrieve file size of " + candidate + " from " + zipFileName);
						continue;
					}
					if (fileSize > sizeCutoff) {
						excelFileNames.add(candidate);
					} else {
						logger.info("Will not process " + candidate + "; file size too small");
						logger.info("Size of excel file: " + fileSize + ", size cutoff: " + sizeCutoff);
					}
				}
			}
		}
		
		// If there are no excel files to process, then stop here
		// Don't insert the zip file name into processed db, as nothing was ingested
		// If the user changes what reports to accept in RDF_Map.prop later,
		// they may still be able to ingest reports from this zip
		if (excelFileNames.size() == 0) {
			logger.info("The zip file " + zipFileName + " contains no accepted KO reports (see RDF_Map.prop for accepted reports)");
			return;
		}
		
		// Save the name of the last excel file,
		// to check whether to index in the case of INDEX_ON_LAST
		String lastExcelFile = excelFileNames.get(excelFileNames.size() - 1);

		// Only insert the zip file name into the processed database once,
		// after successfully ingesting a report for the first time
		boolean zipAddedToProcessedDB = false;
		
		// First ingest each error report, then flush metadata
		for (String excelFileName : excelFileNames) {
			logger.info("Processing " + excelFileName);
			
			// Try to unzip the individual excel file
			// If unzipping fails, continue to the next excel file
			long startedUnzipping = System.currentTimeMillis();
			logger.info("Unzipping the excel file");
			try {
				unzipFile(zipFilePath, excelFileName, xlsTempDir);
			} catch (ZipException e) {
				e.printStackTrace();
				logger.error("Failed to unzip " + excelFileName + " from " + zipFileName);
				continue;
			}
			long finishedUnzipping = System.currentTimeMillis();
			logger.info("Elapsed time unzipping the excel file: " + Long.toString(TimeUnit.MILLISECONDS.toSeconds(finishedUnzipping - startedUnzipping)) + " sec");
			
			// Get the full paths for both the excel and csv files
			int extensionIndex = excelFileName.lastIndexOf(".");
			String excelFilePath = xlsTempDir + "\\" + excelFileName;
			String csvFilePath = csvTempDir + "\\" + excelFileName.substring(0, extensionIndex) + ".csv";
			
			// Try to convert the excel to csv
			// If converting fails, continue to the next excel file
			long startedConverting = System.currentTimeMillis();
			logger.info("Converting excel to csv");
			try {
				excelToCsv(excelFilePath, csvFilePath);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Failed to create " + csvFilePath);
				continue;
			}
			long finishedConverting = System.currentTimeMillis();
			logger.info("Elapsed time converting excel to csv: " + Long.toString(TimeUnit.MILLISECONDS.toSeconds(finishedConverting - startedConverting)) + " sec");
			
			// Index based on case
			boolean createIndexes;
			switch (whatToIndex) {
				case INDEX_ON_ALL:
					createIndexes = true;
					break;
				case INDEX_ON_NONE:
					createIndexes = false;
					break;
				case INDEX_ON_LAST:
					if (excelFileName.equals(lastExcelFile)) {
						createIndexes = true;
					} else {
						createIndexes = false;
					}
					break;
				default:
					createIndexes = true;
			}
			
			// Try ingesting the csv into the database
			// If ingesting fails, continue to the next excel file
			logger.info("Ingesting csv into database");
			long startedIngesting = System.currentTimeMillis();		
			try {
				ingest(csvFilePath, zipFileName, excelFileName, createIndexes);
			} catch (Exception e) {
				
				// Stack trace printed in ingest method
				logger.error("Failed to ingest " + csvFilePath);
				continue;
			}
			long finishedIngesting = System.currentTimeMillis();
			logger.info("Elapsed time ingesting csv into database: " + Long.toString(TimeUnit.MILLISECONDS.toSeconds(finishedIngesting - startedIngesting)) + " sec");							
				
			// Mark the zip file as processed, if not already marked
			if (!zipAddedToProcessedDB) {
				insertZipIntoProcessedDB(zipFileName);
				zipAddedToProcessedDB = true;
			}
			
			// Add some information about the error report into the processed db
			insertReportIntoProcessedDB(zipFileName, excelFileName);	
		}
		logger.info("Elapsed time processing " + zipFileName + ": " + Long.toString(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start)) + " sec");
	}
	
	// Check to see whether the zip file has already been processed
	private boolean needToProcess(String zipFileName) {
		
		// If the zip file is not accepted, no need to process
		if (!acceptZips.contains(zipFileName.substring(0, 5))) {
			logger.info("The zip file " + zipFileName + " is not accepted");
			return false;
		}
		
		boolean needToProcess;
		
		// If the database exists, then check whether zip file needs to be processed
		// Else, definitely process the zip file to create the database
		if (dbExists) {
			
			// In the case that the connection fails, don't process to avoid duplicates
			needToProcess = false;
			
			// Establish a connection to the database
			try (Connection dbConnection = DriverManager.getConnection(processedDbUrl, PROCESSED_DB_USER, PROCESSED_DB_PASS)) {
				
				// Query to see if the zip file name already exists in the database
				// The query will terminate after finding the first record to improve performance
				String processedQuery = "SELECT TOP 1 " + ZIP_COLUMN + " FROM " + ZIP_TABLE + " WHERE " + ZIP_COLUMN + " = '" + zipFileName + "'";
				
				// Only return true if there are explicitly no results for the given zip file
				try (Statement statement = dbConnection.createStatement()) {
					try(ResultSet result = statement.executeQuery(processedQuery)) {
						
						// Could set needToProces = !result.first(), but this improves readability
						boolean alreadyProcessed = result.first();
						if (!alreadyProcessed) {
							needToProcess = true;
						} else {
							logger.info("The zip file " + zipFileName + " has already been processed");
						}
					}
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("Failed to execute or close the query " + processedQuery + " at " + processedDbUrl);
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("Failed to connect to or close " + processedDbUrl);
			}
		} else {
			
			// There is no database; definitely process the zip file to create one
			needToProcess = true;
		}
		
		return needToProcess;
	}

	private void ingest(String csvFilePath, String zipFileName, String excelFileName, boolean createIndexes) throws Exception {
			
		// Specify the options necessary to load data from the csv file into SEMOSS
		ImportOptions options = new ImportOptions();
		options.setDbName(dbName);
		options.setImportType(ImportOptions.IMPORT_TYPE.CSV);
		options.setRDBMSDriverType(dbType);
		options.setDbType(ImportOptions.DB_TYPE.RDBMS);
		options.setBaseFolder(baseFolder);
		options.setAutoLoad(false);
		
		// Improves performance
		options.setAllowDuplicates(true);
		options.setFileLocation(csvFilePath);
		options.setPropertyFiles(propFile);
		
		// Populate the maps that store metadata from the file names
		Map<String, String> valueMap = populateValueMap(zipFileName, excelFileName);
		Map<String, String> typeMap = populateTypeMap(zipFileName, excelFileName);
		options.setObjectValueMap(valueMap);
		options.setObjectTypeMap(typeMap);
		options.setRowKey(ROW_KEY);
		options.setCreateIndexes(createIndexes);
				
		// Create new if the database does not yet exist, otherwise add to existing
		ImportDataProcessor importer = new ImportDataProcessor();
		if (!dbExists) {
			options.setImportMethod(ImportOptions.IMPORT_METHOD.CREATE_NEW);
			try {
				importer.runProcessor(options);
				dbExists = true;
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Failed to create the database " + dbName + " with " + csvFilePath);
				
				// Throw a new exception so that process() knows not to count the report as processed
				throw new Exception();
			}
		} else {
			options.setImportMethod(ImportOptions.IMPORT_METHOD.ADD_TO_EXISTING);
			try {
				importer.runProcessor(options);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Failed to add " + csvFilePath + " to " + dbName);

				// Throw a new exception so that process() knows not to count the report as processed
				throw new Exception();
			}
		}
		
		// Allow subclasses to ingest file metadata into a separate database if needed
		ingestMetadataIntoSeparateDB(valueMap, typeMap, createIndexes);
	}
	
	// For information that can be parsed from the zip or excel file names
	protected abstract Map<String, String> populateValueMap(String zipFileName, String excelFileName);
	protected abstract Map<String, String> populateTypeMap(String zipFileName, String excelFileName);
	protected abstract void ingestMetadataIntoSeparateDB(Map<String, String> valueMap, Map<String, String> typeMap, boolean createIndexes);
	
	// Really does return a list of strings
	@SuppressWarnings("unchecked")
	private List<String> retrieveFileNames(String zipFilePath) throws ZipException {
		ZipFile zipFile = new ZipFile(zipFilePath);
		return (List<String>) zipFile.getFileHeaders().stream().map(s -> ((FileHeader) s).getFileName()).collect(Collectors.toList());
	}
	
	private long retrieveFileSize(String zipFilePath, String excelFileName) throws ZipException {
		ZipFile zipFile = new ZipFile(zipFilePath);
		return zipFile.getFileHeader(excelFileName).getUncompressedSize();
	}
	
	// Unzip on a per-file basis, to avoid unzipping unnecessary files
	private void unzipFile(String zipFilePath, String excelFileName, String destinationPath) throws ZipException {
		ZipFile zipFile = new ZipFile(zipFilePath);
		zipFile.extractFile(excelFileName, destinationPath);
	}
		
	private void excelToCsv(String excelFilePath, String csvFilePath) throws Exception {
		List<String> commands = Arrays.asList("cmd", "/c", baseFolder + "\\portables\\R-Portable\\App\\R-Portable\\bin\\x64\\Rscript.exe", xlsToCsvScript, excelFilePath, csvFilePath);
		ProcessBuilder processBuilder = new ProcessBuilder(commands);
		int exitValue = processBuilder.start().waitFor();
		if (exitValue != 0) {
			throw new Exception("Failed to convert " + excelFilePath + " to csv");
		}
	}
	
	// This database does not need to be exposed to the user, so this method creates it manually
	private void createProcessedDB() {
		try (Connection dbConnection = DriverManager.getConnection(processedDbUrl, PROCESSED_DB_USER, PROCESSED_DB_PASS)) {
			
			// Drop what is already there and create new tables
			// This method is only called if the kickout database does not exist,
			// so forget about any progress that may have been logged in an existing processed database
			String dropZipSql = "DROP TABLE IF EXISTS " + ZIP_TABLE;
			String dropReportSql = "DROP TABLE IF EXISTS " + REPORT_TABLE;
			String createZipTableSql = "CREATE TABLE " + ZIP_TABLE + "(" + ZIP_COLUMN + " VARCHAR(255) PRIMARY KEY, " + ZIP_TIME_COLUMN + " VARCHAR(255), " + ZIP_SIZE_COLUMN + " INT)";
			String createReportTableSql = "CREATE TABLE " + REPORT_TABLE + "(" + REPORT_COLUMN + " VARCHAR(255) PRIMARY KEY, " + REPORT_TIME_COLUMN + " VARCHAR(255), " + REPORT_SIZE_COLUMN + " INT, " + REPORT_SYS_COLUMN + " VARCHAR(255), " + ZIP_COLUMN + " VARCHAR(255), FOREIGN KEY (" + ZIP_COLUMN + ") REFERENCES " + ZIP_TABLE + "(" + ZIP_COLUMN + "))";		
			try (Statement statement = dbConnection.createStatement()) {
				statement.execute(dropZipSql);
				statement.execute(dropReportSql);
				statement.execute(createZipTableSql);
				statement.execute(createReportTableSql);
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("Failed to create new tables in " + processedDbUrl);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Failed to connect to or close " + processedDbUrl);
		}
	}
	
	private void insertZipIntoProcessedDB(String zipFileName) {
		try (Connection dbConnection = DriverManager.getConnection(processedDbUrl, PROCESSED_DB_USER, PROCESSED_DB_PASS)) {
			
			// Get metadata
			File zipFile = new File(folderToWatch + "\\" + zipFileName);
			String zipTimeStamp = new Date(zipFile.lastModified()).toString();
			String zipFileSize = Long.toString(zipFile.length());
			
			// Insert values
			String zipInsertSql = "INSERT INTO " + ZIP_TABLE + " VALUES('" + zipFileName + "', '" + zipTimeStamp + "', " + zipFileSize + ")";
			try (Statement statement = dbConnection.createStatement()) {
				statement.execute(zipInsertSql);
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("Failed insert values into " + ZIP_TABLE + " at " + processedDbUrl);
			}			
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Failed to connect to or close " + processedDbUrl);
		}					
	}
	
	private void insertReportIntoProcessedDB(String zipFileName, String excelFileName) {
		try (Connection dbConnection = DriverManager.getConnection(processedDbUrl, PROCESSED_DB_USER, PROCESSED_DB_PASS)) {
			
			// Get metadata
			File excelFile = new File(xlsTempDir + "\\" + excelFileName);
			String reportTimeStamp = new Date(excelFile.lastModified()).toString();
			String reportFileSize = Long.toString(excelFile.length());
			String systemName = excelFileName.substring(11, 13);
			
			// Insert values
			String reportInsertSql = "INSERT INTO " + REPORT_TABLE + " VALUES('" + zipFileName + excelFileName + "', '" + reportTimeStamp + "', " + reportFileSize + ", '" + systemName + "', '" + zipFileName + "')";
			try (Statement statement = dbConnection.createStatement()) {
				statement.execute(reportInsertSql);
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("Failed insert values into " + REPORT_TABLE + " at " + processedDbUrl);
			}			
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Failed to connect to or close " + processedDbUrl);
		}					
	}
	
	@Override
	public void run() {
		loadFirst();
		
		// If the user wants to hold on indexing,
		// then create indexes after the last excel file has been ingested
		// from the new zip file
		if (holdOnIndexing) {
			whatToIndex = IndexEnum.INDEX_ON_LAST;
		}
		
		// Code taken from AbstractFileWatcher
		// TODO clean up this code take from AbstractFileWatcher
		try
		{
			WatchService watcher = FileSystems.getDefault().newWatchService();
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

			//Path dir2Watch = Paths.get(baseFolder + "/" + folderToWatch);

			Path dir2Watch = Paths.get(folderToWatch);

			WatchKey key = dir2Watch.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
			while(true)
			{
				//WatchKey key2 = watcher.poll(1, TimeUnit.MINUTES);
				WatchKey key2 = watcher.take();
				
				for(WatchEvent<?> event: key2.pollEvents())
				{
					WatchEvent.Kind kind = event.kind();
					if(kind == StandardWatchEventKinds.ENTRY_CREATE)
					{
						String newFile = event.context() + "";
						if(newFile.endsWith(extension))
						{
							// Allow a delay in case the file is still being held to save
							Thread.sleep(processDelay*1000);	
							try
							{
								if(needToProcess(newFile)) {
									process(newFile);
								}
								
							}catch(RuntimeException ex)
							{
								ex.printStackTrace();
							}
						}else
							logger.info("Ignoring File " + newFile);
					}
				}
				key2.reset();
			}
		}catch(RuntimeException ex)
		{
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		} catch (InterruptedException ex) {
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		} catch (IOException ex) {
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		}
		// End code taken from AbstractFileWatcher
	}
	
}
